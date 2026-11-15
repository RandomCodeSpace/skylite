package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Verification policy, fixed by decision record 0006.
const (
	// MaxTokenSize is the largest token accepted, in bytes.
	MaxTokenSize = 16 << 10
	// ClockSkew is the allowance applied to exp, nbf and iat.
	ClockSkew = time.Minute
	// maxLoggedKeyID bounds the length of a key id copied into an Error.
	maxLoggedKeyID = 64
)

// acceptedAlgorithms are the only signature algorithms accepted: asymmetric,
// never HMAC, never "none".
var acceptedAlgorithms = []jose.SignatureAlgorithm{jose.RS256, jose.ES256, jose.EdDSA}

// Code is a stable identifier for why a request was refused.
type Code string

// The codes. Clients only ever see whether a token was missing, invalid or
// insufficient; the specific code is for server logs.
const (
	CodeMissingToken         Code = "missing_token"
	CodeTokenTooLarge        Code = "token_too_large"
	CodeMalformed            Code = "malformed_token"
	CodeUnsupportedAlgorithm Code = "unsupported_algorithm"
	CodeUntrustedIssuer      Code = "untrusted_issuer"
	CodeUnknownKey           Code = "unknown_key"
	CodeKeysUnavailable      Code = "keys_unavailable"
	CodeInvalidSignature     Code = "invalid_signature"
	CodeMissingClaim         Code = "missing_claim"
	CodeInvalidAudience      Code = "invalid_audience"
	CodeExpired              Code = "token_expired"
	CodeNotYetValid          Code = "token_not_yet_valid"
	CodeIssuedInFuture       Code = "issued_in_future"
	CodeForbidden            Code = "forbidden"
)

// Error is a refused token or request. Its fields are safe to log: Issuer is
// set only when it equals a configured issuer, Subject only when the token's
// signature verified, and KeyID only when it is short.
type Error struct {
	Code    Code
	Issuer  string
	Subject string
	KeyID   string
}

func (e *Error) Error() string { return "auth: " + string(e.Code) }

// Principal is an authenticated caller: the pair (Issuer, Subject), with the
// claims of the token that authenticated it.
type Principal struct {
	Issuer  string
	Subject string
	Claims  map[string]any
}

// Guard verifies bearer tokens and authorises principals.
type Guard struct {
	sources map[string]*keySource
	rules   []Rule
	now     func() time.Time
	log     *slog.Logger
}

// New validates cfg and returns a Guard. client fetches discovery documents
// and key sets; each fetch is bounded by its own timeout, so client needs
// none. now is the clock and log receives refusals and key refresh failures.
// A nil client, now or log means http.DefaultClient, time.Now or no logging.
// New makes no network request.
func New(cfg Config, client *http.Client, now func() time.Time, log *slog.Logger) (*Guard, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	g := &Guard{sources: make(map[string]*keySource, len(cfg.Issuers)), rules: cfg.Rules, now: now, log: log}
	for _, iss := range cfg.Issuers {
		g.sources[iss.Issuer] = &keySource{issuer: iss, client: client, now: now, log: log}
	}
	return g, nil
}

// Verify checks token and returns its principal, or an *Error.
func (g *Guard) Verify(ctx context.Context, token string) (*Principal, error) {
	if token == "" {
		return nil, &Error{Code: CodeMissingToken}
	}
	if len(token) > MaxTokenSize {
		return nil, &Error{Code: CodeTokenTooLarge}
	}
	tok, err := jwt.ParseSigned(token, acceptedAlgorithms)
	if err != nil {
		if _, ok := errors.AsType[*jose.ErrUnexpectedSignatureAlgorithm](err); ok {
			return nil, &Error{Code: CodeUnsupportedAlgorithm}
		}
		return nil, &Error{Code: CodeMalformed}
	}
	e := &Error{}
	if kid := tok.Headers[0].KeyID; len(kid) <= maxLoggedKeyID {
		e.KeyID = kid
	}
	var unverified struct {
		Issuer string `json:"iss"`
	}
	if tok.UnsafeClaimsWithoutVerification(&unverified) != nil {
		return nil, e.with(CodeMalformed)
	}
	src, ok := g.sources[unverified.Issuer]
	if !ok {
		return nil, e.with(CodeUntrustedIssuer)
	}
	e.Issuer = src.issuer.Issuer
	if tok.Headers[0].KeyID == "" {
		return nil, e.with(CodeUnknownKey)
	}
	key, code := src.key(ctx, tok.Headers[0].KeyID)
	if code != "" {
		return nil, e.with(code)
	}
	if key.Algorithm != "" && key.Algorithm != tok.Headers[0].Algorithm {
		return nil, e.with(CodeInvalidSignature)
	}
	var std jwt.Claims
	var all map[string]any
	if err := tok.Claims(key.Key, &std, &all); err != nil {
		if errors.Is(err, jose.ErrCryptoFailure) || errors.Is(err, jose.ErrUnsupportedKeyType) {
			return nil, e.with(CodeInvalidSignature)
		}
		// The signature verified, so the payload is signed but its claims have the wrong types.
		return nil, e.with(CodeMalformed)
	}
	e.Subject = std.Subject
	switch {
	case std.Subject == "", std.Expiry == nil, len(std.Audience) == 0:
		return nil, e.with(CodeMissingClaim)
	}
	err = std.ValidateWithLeeway(jwt.Expected{Issuer: src.issuer.Issuer, AnyAudience: jwt.Audience{src.issuer.Audience}, Time: g.now()}, ClockSkew)
	switch {
	case err == nil:
		return &Principal{Issuer: std.Issuer, Subject: std.Subject, Claims: all}, nil
	case errors.Is(err, jwt.ErrInvalidAudience):
		return nil, e.with(CodeInvalidAudience)
	case errors.Is(err, jwt.ErrExpired):
		return nil, e.with(CodeExpired)
	case errors.Is(err, jwt.ErrNotValidYet):
		return nil, e.with(CodeNotYetValid)
	case errors.Is(err, jwt.ErrIssuedInTheFuture):
		return nil, e.with(CodeIssuedInFuture)
	}
	return nil, e.with(CodeUntrustedIssuer) // jwt.ErrInvalidIssuer, the only other error it returns
}

func (e *Error) with(c Code) *Error {
	e.Code = c
	return e
}
