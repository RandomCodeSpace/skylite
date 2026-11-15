// Package auth authenticates requests to the query endpoint with OIDC JWT
// bearer tokens and authorises them by role, as decided in
// docs/content/docs/decisions/0006-authentication.md.
//
// A [Guard] verifies a token against the configured issuers, which turns it
// into a [Principal], checks the principal's role on a database against the
// configured rules, and wraps an [http.Handler] to do both for each request.
// Verification failures are [*Error] values with a stable [Code]; neither
// errors nor log lines contain a token or any part of one.
package auth

import (
	"errors"
	"fmt"
	"net/url"
	"path"
)

// Config is the authentication and authorisation configuration of a server.
type Config struct {
	// Issuers are the token issuers the server trusts. At least one is required.
	Issuers []Issuer
	// Rules grant roles. A principal no rule matches is denied everything.
	Rules []Rule
}

// Issuer is one trusted token issuer.
type Issuer struct {
	// Issuer is the exact value tokens carry in their iss claim. Unless
	// JWKSFile is set, it must be an https URL, and the issuer's discovery
	// document is fetched from Issuer + "/.well-known/openid-configuration".
	Issuer string
	// Audience is the value a token's aud claim must contain.
	Audience string
	// JWKSFile, if set, is the path of a JSON Web Key Set file holding the
	// issuer's public keys. No network request is then made for this issuer.
	JWKSFile string
}

// Role is a level of access to a database. Each role includes the ones
// before it: read, then write, then admin.
type Role string

// The roles, in increasing order of access.
const (
	RoleRead  Role = "read"
	RoleWrite Role = "write"
	RoleAdmin Role = "admin"
)

// rank orders the roles; an unknown role ranks 0 and grants nothing.
func (r Role) rank() int {
	switch r {
	case RoleRead:
		return 1
	case RoleWrite:
		return 2
	case RoleAdmin:
		return 3
	}
	return 0
}

// Rule grants Role on the databases matching Database to the principals of
// Issuer that have the given Subject, or, if Claim is set instead, whose
// token has the claim Claim equal to Value or, for an array claim, containing it.
type Rule struct {
	Issuer string
	// Subject matches the sub claim exactly. Exactly one of Subject and
	// Claim must be set.
	Subject string
	// Claim names a top-level claim, for example "groups" or "roles".
	Claim string
	// Value is the string the claim must equal or contain.
	Value string
	// Database is a database name or a pattern in the syntax of [path.Match].
	Database string
	Role     Role
}

// Validate reports the first problem with the configuration, if any.
func (c Config) Validate() error {
	if len(c.Issuers) == 0 {
		return errors.New("auth: no issuers configured")
	}
	seen := make(map[string]bool, len(c.Issuers))
	for i, iss := range c.Issuers {
		if err := iss.validate(); err != nil {
			return fmt.Errorf("auth: issuers[%d]: %w", i, err)
		}
		if seen[iss.Issuer] {
			return fmt.Errorf("auth: issuers[%d]: issuer %q configured twice", i, iss.Issuer)
		}
		seen[iss.Issuer] = true
	}
	for i, r := range c.Rules {
		if err := r.validate(seen); err != nil {
			return fmt.Errorf("auth: rules[%d]: %w", i, err)
		}
	}
	return nil
}

func (iss Issuer) validate() error {
	if iss.Issuer == "" {
		return errors.New("issuer is empty")
	}
	if iss.Audience == "" {
		return errors.New("audience is empty")
	}
	if iss.JWKSFile != "" {
		return nil
	}
	u, err := url.Parse(iss.Issuer)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return fmt.Errorf("issuer %q is not an https URL without query, fragment or user info", iss.Issuer)
	}
	return nil
}

func (r Rule) validate(issuers map[string]bool) error {
	if !issuers[r.Issuer] {
		return fmt.Errorf("issuer %q is not a configured issuer", r.Issuer)
	}
	if (r.Subject == "") == (r.Claim == "") {
		return errors.New("exactly one of subject and claim must be set")
	}
	if r.Claim != "" && r.Value == "" {
		return errors.New("claim rule has an empty value")
	}
	if r.Database == "" {
		return errors.New("database is empty")
	}
	if _, err := path.Match(r.Database, ""); err != nil {
		return fmt.Errorf("database pattern %q: %w", r.Database, err)
	}
	if r.Role.rank() == 0 {
		return fmt.Errorf("unknown role %q", r.Role)
	}
	return nil
}
