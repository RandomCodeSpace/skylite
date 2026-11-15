package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// Key cache policy, fixed by decision record 0006.
const (
	// fetchTimeout bounds one refresh: discovery and JWKS together.
	fetchTimeout = 10 * time.Second
	// maxDocumentSize bounds a discovery document or a JWKS, fetched or read from disk.
	maxDocumentSize = 1 << 20
	// keyCacheLifetime is how long fetched keys are used before a refresh is attempted.
	keyCacheLifetime = time.Hour
	// keyMaxStale is how long fetched keys stay usable while refreshes fail.
	keyMaxStale = 24 * time.Hour
	// minRefreshInterval is the least time between two fetch attempts for one issuer.
	minRefreshInterval = time.Minute
	// minRSABits is the smallest RSA key accepted.
	minRSABits = 2048
)

// keySource holds one issuer's signing keys and refreshes them on demand.
// It starts no goroutines: refreshes run in the calling request.
type keySource struct {
	issuer Issuer
	client *http.Client
	now    func() time.Time
	log    *slog.Logger

	fetchMu sync.Mutex // held for the duration of a fetch

	mu        sync.Mutex
	keys      map[string]jose.JSONWebKey
	fetched   time.Time // last successful fetch; zero if none
	attempted time.Time // last fetch attempt; zero if none
}

// key returns the issuer's public key with id kid, refreshing the key set when
// the id is unknown or the cache has expired, at most once per minRefreshInterval.
func (s *keySource) key(ctx context.Context, kid string) (jose.JSONWebKey, Code) {
	k, found, fresh, usable := s.lookup(kid)
	switch {
	case found && fresh:
		return k, ""
	case found && usable:
		// The cached key is still good enough: refresh only if no other request is already doing so.
		if s.fetchMu.TryLock() {
			s.refresh(ctx)
			s.fetchMu.Unlock()
		}
	default:
		s.fetchMu.Lock()
		s.refresh(ctx)
		s.fetchMu.Unlock()
	}
	k, found, _, usable = s.lookup(kid)
	switch {
	case !usable:
		return jose.JSONWebKey{}, CodeKeysUnavailable
	case !found:
		return jose.JSONWebKey{}, CodeUnknownKey
	}
	return k, ""
}

func (s *keySource) lookup(kid string) (k jose.JSONWebKey, found, fresh, usable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	age := s.now().Sub(s.fetched)
	usable = !s.fetched.IsZero() && age <= keyMaxStale
	k, found = s.keys[kid]
	return k, found, usable && age < keyCacheLifetime, usable
}

// refresh fetches the key set unless an attempt was made within
// minRefreshInterval. A failed fetch keeps the previous keys. The caller holds fetchMu.
func (s *keySource) refresh(ctx context.Context) {
	now := s.now()
	s.mu.Lock()
	recent := !s.attempted.IsZero() && now.Sub(s.attempted) < minRefreshInterval
	if !recent {
		s.attempted = now
	}
	s.mu.Unlock()
	if recent {
		return
	}
	keys, err := s.fetch(ctx)
	if err != nil {
		s.log.Warn("auth: refreshing signing keys failed", "issuer", s.issuer.Issuer, "error", err)
		return
	}
	s.mu.Lock()
	s.keys, s.fetched = keys, now
	s.mu.Unlock()
}

func (s *keySource) fetch(ctx context.Context) (map[string]jose.JSONWebKey, error) {
	if s.issuer.JWKSFile != "" {
		raw, err := readFile(s.issuer.JWKSFile)
		if err != nil {
			return nil, err
		}
		return parseKeySet(raw)
	}
	// A request that gives up must not cancel a refresh other requests wait for.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
	defer cancel()
	raw, err := s.get(ctx, strings.TrimSuffix(s.issuer.Issuer, "/")+"/.well-known/openid-configuration")
	if err != nil {
		return nil, err
	}
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("discovery document is not valid JSON: %w", err)
	}
	if doc.Issuer != s.issuer.Issuer {
		return nil, fmt.Errorf("discovery document names issuer %q", doc.Issuer)
	}
	if u, err := url.Parse(doc.JWKSURI); err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("discovery document jwks_uri %q is not an https URL", doc.JWKSURI)
	}
	raw, err = s.get(ctx, doc.JWKSURI)
	if err != nil {
		return nil, err
	}
	return parseKeySet(raw)
}

func (s *keySource) get(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Request.URL.Scheme != "https" {
		return nil, fmt.Errorf("GET %s: redirected to a URL that is not https", u)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	return readLimited(resp.Body, u)
}

func readFile(name string) ([]byte, error) {
	f, err := os.Open(name) //nolint:gosec // the path comes from server configuration
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return readLimited(f, name)
}

func readLimited(r io.Reader, name string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxDocumentSize+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	if len(b) > maxDocumentSize {
		return nil, fmt.Errorf("%s is larger than %d bytes", name, maxDocumentSize)
	}
	return b, nil
}

// parseKeySet returns the usable signing keys of a JWKS by key id. Keys
// without an id, not for signing, of an unsupported type, or that do not
// parse are skipped; a set with no usable key is an error.
func parseKeySet(raw []byte) (map[string]jose.JSONWebKey, error) {
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, fmt.Errorf("JWKS is not valid JSON: %w", err)
	}
	keys := make(map[string]jose.JSONWebKey, len(set.Keys))
	for _, r := range set.Keys {
		var k jose.JSONWebKey
		if k.UnmarshalJSON(r) != nil || k.KeyID == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		k = k.Public() // drops private material, if a careless issuer published any
		if _, dup := keys[k.KeyID]; dup || !k.Valid() || !acceptedKey(k.Key) {
			continue
		}
		keys[k.KeyID] = k
	}
	if len(keys) == 0 {
		return nil, errors.New("JWKS has no usable signing key")
	}
	return keys, nil
}

// acceptedKey reports whether key is a public key of a type the accepted
// algorithms use: RSA of at least minRSABits, ECDSA on P-256, or Ed25519.
func acceptedKey(key any) bool {
	switch k := key.(type) {
	case *rsa.PublicKey:
		return k.N.BitLen() >= minRSABits
	case *ecdsa.PublicKey:
		return k.Curve == elliptic.P256()
	case ed25519.PublicKey:
		return true
	}
	return false
}
