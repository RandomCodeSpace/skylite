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
	"slices"
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
	// maxLoggedValue bounds a value from a remote document or a token copied into an error or a log line.
	maxLoggedValue = 200
)

// errRedirect refuses every redirect: no key or discovery request goes to a URL chosen by a redirect.
var errRedirect = errors.New("redirects are not followed")

// noRedirects returns a shallow copy of client that follows no redirect.
func noRedirects(client *http.Client) *http.Client {
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return errRedirect }
	return &c
}

// clip bounds s to maxLoggedValue bytes.
func clip(s string) string {
	if len(s) <= maxLoggedValue {
		return s
	}
	// A cut inside a multi-byte character would leave invalid UTF-8.
	return strings.ToValidUTF8(s[:maxLoggedValue], "") + "..."
}

// keySource holds one issuer's signing keys and refreshes them on demand.
// It starts no goroutines: refreshes run in the calling request.
type keySource struct {
	issuer Issuer
	client *http.Client
	now    func() time.Time
	log    *slog.Logger

	fetching chan struct{} // holds a token for the duration of a fetch

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
		s.refreshExclusive(ctx, true)
	default:
		s.refreshExclusive(ctx, false)
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

// refreshExclusive runs refresh while holding fetching's token, and
// releases it on every path, a panic included. With try set, it does
// nothing if another request holds it. A request that must wait for it
// calls ctx's wait hook (WithRefreshWait) first, and gives up when ctx ends.
func (s *keySource) refreshExclusive(ctx context.Context, try bool) {
	select {
	case s.fetching <- struct{}{}:
	default:
		if try {
			return
		}
		if wait, ok := ctx.Value(waitKey{}).(func() (func(), bool)); ok {
			resume, ok := wait()
			if !ok {
				return
			}
			defer resume()
		}
		select {
		case s.fetching <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
	defer func() { <-s.fetching }()
	s.refresh(ctx)
}

type waitKey struct{}

// WithRefreshWait returns ctx with wait, which Verify calls before it waits
// for another request's refresh of an issuer's signing keys, a wait as long
// as that fetch. If wait reports false Verify does not wait, and refuses the
// token as it would without the key; otherwise it calls resume once the wait,
// and any refresh it then runs, have ended.
func WithRefreshWait(ctx context.Context, wait func() (resume func(), ok bool)) context.Context {
	return context.WithValue(ctx, waitKey{}, wait)
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
// minRefreshInterval. A failed fetch keeps the previous keys. The caller holds fetching's token.
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

// LoadKeyFiles reads the key set of every issuer configured with a JWKS
// file, as a first refresh, and fails if one cannot be read or holds no
// usable key. Later refreshes follow the cache policy.
func (g *Guard) LoadKeyFiles() error {
	for _, s := range g.sources {
		if s.issuer.JWKSFile == "" {
			continue
		}
		keys, err := s.fetch(context.Background())
		if err != nil {
			return fmt.Errorf("auth: issuer %s: %w", clip(s.issuer.Issuer), err)
		}
		now := s.now()
		s.mu.Lock()
		s.keys, s.fetched, s.attempted = keys, now, now
		s.mu.Unlock()
	}
	return nil
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
		return nil, fmt.Errorf("discovery document is not valid JSON: %s", clip(err.Error()))
	}
	if doc.Issuer != s.issuer.Issuer {
		return nil, fmt.Errorf("discovery document names issuer %q", clip(doc.Issuer))
	}
	if u, err := url.Parse(doc.JWKSURI); err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("discovery document jwks_uri %q is not an https URL", clip(doc.JWKSURI))
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
		return nil, fmt.Errorf("GET %s: %s", clip(u), clip(err.Error()))
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		// A *url.Error repeats the URL, or a redirect's Location; report only its cause.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return nil, fmt.Errorf("GET %s: %s", clip(u), clip(err.Error()))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", clip(u), resp.StatusCode)
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
		return nil, fmt.Errorf("reading %s: %s", clip(name), clip(err.Error()))
	}
	if len(b) > maxDocumentSize {
		return nil, fmt.Errorf("%s is larger than %d bytes", clip(name), maxDocumentSize)
	}
	return b, nil
}

// parseKeySet returns the usable signing keys of a JWKS by key id. Keys
// without an id, not for signing (a use other than sig, or a key_ops without
// verify), of an unsupported type, or that do not parse are skipped; of two
// keys with one id, the first usable one is kept. A set with no usable key
// is an error.
func parseKeySet(raw []byte) (map[string]jose.JSONWebKey, error) {
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, fmt.Errorf("JWKS is not valid JSON: %s", clip(err.Error()))
	}
	keys := make(map[string]jose.JSONWebKey, len(set.Keys))
	for _, r := range set.Keys {
		var k jose.JSONWebKey
		var ops struct {
			KeyOps []string `json:"key_ops"` // go-jose does not decode key_ops
		}
		if k.UnmarshalJSON(r) != nil || k.KeyID == "" || (k.Use != "" && k.Use != "sig") ||
			json.Unmarshal(r, &ops) != nil || (ops.KeyOps != nil && !slices.Contains(ops.KeyOps, "verify")) {
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
