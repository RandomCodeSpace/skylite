package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

type principalKey struct{}

// PrincipalFromContext returns the principal Require stored in ctx.
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(*Principal)
	return p, ok
}

// Require returns a handler that serves next only if the request carries an
// "Authorization: Bearer" token that verifies, and its principal holds at
// least role need on the database that database returns for the request.
// next finds the principal with PrincipalFromContext.
//
// A request without a bearer token gets 401 with "WWW-Authenticate: Bearer";
// one whose token does not verify gets 401 with error="invalid_token"; an
// authenticated principal without the role gets 403. The response names no
// more than that; the specific Code is logged.
func (g *Guard) Require(need Role, database func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := BearerToken(r.Header)
		var p *Principal
		if err == nil {
			p, err = g.Verify(r.Context(), token)
		}
		if err != nil {
			var e *Error
			if !errors.As(err, &e) {
				e = &Error{Code: CodeMalformed}
			}
			g.log.LogAttrs(r.Context(), slog.LevelInfo, "auth: request refused", refusalAttrs(e, token)...)
			if e.Code == CodeMissingToken {
				w.Header().Set("WWW-Authenticate", "Bearer")
				http.Error(w, string(CodeMissingToken), http.StatusUnauthorized)
				return
			}
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			http.Error(w, "invalid_token", http.StatusUnauthorized)
			return
		}
		db := database(r)
		if !g.Authorize(p, db, need) {
			attrs := refusalAttrs(&Error{Code: CodeForbidden, Issuer: p.Issuer, Subject: p.Subject}, token)
			attrs = append(attrs, slog.String("database", clip(db)), slog.String("role", string(need)))
			g.log.LogAttrs(r.Context(), slog.LevelInfo, "auth: request refused", attrs...)
			w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
			http.Error(w, string(CodeForbidden), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

// BearerToken returns the token of the request's single Authorization header
// with the Bearer scheme, or an *Error with CodeMissingToken or CodeMalformed.
func BearerToken(h http.Header) (string, error) {
	values := h.Values("Authorization")
	if len(values) > 1 {
		return "", &Error{Code: CodeMalformed}
	}
	if len(values) == 0 {
		return "", &Error{Code: CodeMissingToken}
	}
	// RFC 7235 allows one or more spaces after the scheme.
	scheme, token, _ := strings.Cut(values[0], " ")
	token = strings.TrimLeft(token, " ")
	if !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", &Error{Code: CodeMissingToken}
	}
	return token, nil
}

// refusalAttrs are the log attributes of a refusal, each value cut to 200
// bytes. The token itself is represented only by a prefix of its SHA-256 hash.
func refusalAttrs(e *Error, token string) []slog.Attr {
	attrs := []slog.Attr{slog.String("code", string(e.Code))}
	for _, a := range []struct{ k, v string }{{"issuer", e.Issuer}, {"subject", e.Subject}, {"kid", e.KeyID}} {
		if a.v != "" {
			attrs = append(attrs, slog.String(a.k, clip(a.v)))
		}
	}
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		attrs = append(attrs, slog.String("token_sha256_prefix", hex.EncodeToString(sum[:6])))
	}
	return attrs
}
