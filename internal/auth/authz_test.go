package auth

import (
	"strings"
	"testing"
)

const (
	issA = "https://a.example"
	issB = "https://b.example"
)

func TestConfigValidate(t *testing.T) {
	iss := []Issuer{{Issuer: issA, Audience: "aud"}}
	rule := Rule{Issuer: issA, Subject: "alice", Database: "orders", Role: RoleRead}
	withRule := func(change func(*Rule)) Config {
		r := rule
		change(&r)
		return Config{Issuers: iss, Rules: []Rule{r}}
	}
	good := []Config{
		{Issuers: iss, Rules: []Rule{rule}},
		{Issuers: []Issuer{{Issuer: "kubernetes", Audience: "aud", JWKSFile: "/jwks.json"}}},
		withRule(func(r *Rule) { r.Subject, r.Claim, r.Value = "", "groups", "analysts" }),
		withRule(func(r *Rule) { r.Database = "a" }),
		withRule(func(r *Rule) { r.Database = "z" }),
		withRule(func(r *Rule) { r.Database = "9" }),
		withRule(func(r *Rule) { r.Database = "0-db-1" }),
		withRule(func(r *Rule) { r.Database = strings.Repeat("a", 63) }),
		withRule(func(r *Rule) { r.Database = "*" }),
		withRule(func(r *Rule) { r.Database = "?x" }),
		withRule(func(r *Rule) { r.Database = "*-reports" }),
		withRule(func(r *Rule) { r.Database = "report-*" }),
		{Issuers: []Issuer{{Issuer: issA + "/tenant/v2.0", Audience: "aud"}}},
		{Issuers: []Issuer{{Issuer: "any value, with ? and #", Audience: "aud", JWKSFile: "/jwks.json"}}},
	}
	for i, cfg := range good {
		if err := cfg.Validate(); err != nil {
			t.Errorf("good config %d: %v", i, err)
		}
	}
	bad := map[string]Config{
		"no issuers":                   {},
		"empty issuer":                 {Issuers: []Issuer{{Audience: "aud"}}},
		"empty audience":               {Issuers: []Issuer{{Issuer: issA}}},
		"http issuer":                  {Issuers: []Issuer{{Issuer: "http://a.example", Audience: "aud"}}},
		"issuer without host":          {Issuers: []Issuer{{Issuer: "https:///path", Audience: "aud"}}},
		"issuer with query":            {Issuers: []Issuer{{Issuer: issA + "?x=1", Audience: "aud"}}},
		"issuer with empty query":      {Issuers: []Issuer{{Issuer: issA + "?", Audience: "aud"}}},
		"issuer with fragment":         {Issuers: []Issuer{{Issuer: issA + "#x", Audience: "aud"}}},
		"issuer with empty fragment":   {Issuers: []Issuer{{Issuer: issA + "#", Audience: "aud"}}},
		"issuer with a space":          {Issuers: []Issuer{{Issuer: issA + "/a b", Audience: "aud"}}},
		"issuer with a no-break space": {Issuers: []Issuer{{Issuer: issA + "/a\u00a0b", Audience: "aud"}}},
		"issuer with user info":        {Issuers: []Issuer{{Issuer: "https://user@a.example", Audience: "aud"}}},
		"issuer twice":                 {Issuers: append(iss, iss...)},
		"rule for unknown issuer":      withRule(func(r *Rule) { r.Issuer = issB }),
		"subject and claim":            withRule(func(r *Rule) { r.Claim, r.Value = "groups", "x" }),
		"neither subject nor claim":    withRule(func(r *Rule) { r.Subject = "" }),
		"claim without value":          withRule(func(r *Rule) { r.Subject, r.Claim = "", "groups" }),
		"empty database":               withRule(func(r *Rule) { r.Database = "" }),
		"bad pattern":                  withRule(func(r *Rule) { r.Database = "[" }),
		"character class":              withRule(func(r *Rule) { r.Database = "orders[1]" }),
		"escape":                       withRule(func(r *Rule) { r.Database = `a\*` }),
		"upper case":                   withRule(func(r *Rule) { r.Database = "Orders" }),
		"upper case inside":            withRule(func(r *Rule) { r.Database = "orDers" }),
		"dot inside":                   withRule(func(r *Rule) { r.Database = "a.b" }),
		"slash":                        withRule(func(r *Rule) { r.Database = "a/b" }),
		"dot":                          withRule(func(r *Rule) { r.Database = "." }),
		"dot dot":                      withRule(func(r *Rule) { r.Database = ".." }),
		"leading hyphen":               withRule(func(r *Rule) { r.Database = "-x" }),
		"leading underscore":           withRule(func(r *Rule) { r.Database = "_*" }),
		"underscore inside":            withRule(func(r *Rule) { r.Database = "a_b" }),
		"underscore in a pattern":      withRule(func(r *Rule) { r.Database = "report_*" }),
		"64 bytes":                     withRule(func(r *Rule) { r.Database = strings.Repeat("a", 64) }),
		"non-ASCII":                    withRule(func(r *Rule) { r.Database = "caf\u00e9" }),
		"space":                        withRule(func(r *Rule) { r.Database = "a b" }),
		"unknown role":                 withRule(func(r *Rule) { r.Role = "owner" }),
		"empty role":                   withRule(func(r *Rule) { r.Role = "" }),
	}
	for name, cfg := range bad {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: config accepted", name)
		}
		if _, err := New(cfg, nil, nil, nil); err == nil {
			t.Errorf("%s: New accepted the config", name)
		}
	}
}

func TestAuthorize(t *testing.T) {
	cfg := Config{
		Issuers: []Issuer{{Issuer: issA, Audience: "aud"}, {Issuer: issB, Audience: "aud"}},
		Rules: []Rule{
			{Issuer: issA, Subject: "alice", Database: "orders", Role: RoleRead},
			{Issuer: issA, Subject: "bob", Database: "orders", Role: RoleWrite},
			{Issuer: issA, Subject: "carol", Database: "*", Role: RoleAdmin},
			{Issuer: issA, Claim: "groups", Value: "analysts", Database: "report-*", Role: RoleRead},
			{Issuer: issA, Claim: "roles", Value: "skylite.writer", Database: "inventory", Role: RoleWrite},
			{Issuer: issB, Subject: "alice", Database: "b-only", Role: RoleRead},
			{Issuer: issA, Subject: "frank", Database: "a?c", Role: RoleRead},
		},
	}
	g, err := New(cfg, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := New(Config{Issuers: cfg.Issuers}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := func(iss, sub string, claims map[string]any) *Principal {
		return &Principal{Issuer: iss, Subject: sub, Claims: claims}
	}
	alice, bob, carol := p(issA, "alice", nil), p(issA, "bob", nil), p(issA, "carol", nil)
	group := func(v any) *Principal { return p(issA, "erin", map[string]any{"groups": v}) }
	analyst := p(issA, "dave", map[string]any{"groups": []any{"staff", "analysts"}})
	cases := []struct {
		name string
		g    *Guard
		p    *Principal
		db   string
		need Role
		want bool
	}{
		{"no rules denies", empty, carol, "orders", RoleRead, false},
		{"nil principal", g, nil, "orders", RoleRead, false},
		{"read grants read", g, alice, "orders", RoleRead, true},
		{"read does not grant write", g, alice, "orders", RoleWrite, false},
		{"read does not grant admin", g, alice, "orders", RoleAdmin, false},
		{"rule is for one database", g, alice, "payments", RoleRead, false},
		{"write includes read", g, bob, "orders", RoleRead, true},
		{"write grants write", g, bob, "orders", RoleWrite, true},
		{"write does not grant admin", g, bob, "orders", RoleAdmin, false},
		{"admin includes write", g, carol, "anything", RoleWrite, true},
		{"admin grants admin", g, carol, "anything", RoleAdmin, true},
		{"empty database name", g, carol, "", RoleRead, false},
		{"one-character name", g, carol, "a", RoleRead, true},
		{"name starting with z", g, carol, "z", RoleRead, true},
		{"name starting with 9", g, carol, "9", RoleRead, true},
		{"upper case inside a name", g, carol, "orDers", RoleRead, false},
		{"dot inside a name", g, carol, "a.b", RoleRead, false},
		{"63-byte name", g, carol, strings.Repeat("a", 63), RoleRead, true},
		{"name of every allowed character", g, carol, "0az9-", RoleRead, true},
		{"64-byte name", g, carol, strings.Repeat("a", 64), RoleRead, false},
		{"upper-case name", g, carol, "Orders", RoleRead, false},
		{"name with a slash", g, carol, "a/b", RoleRead, false},
		{"name dot", g, carol, ".", RoleRead, false},
		{"name dot dot", g, carol, "..", RoleRead, false},
		{"name with a leading hyphen", g, carol, "-x", RoleRead, false},
		{"name with a leading underscore", g, carol, "_x", RoleRead, false},
		{"name with an underscore", g, carol, "a_b", RoleRead, false},
		{"name with NUL", g, carol, "a\x00b", RoleRead, false},
		{"non-ASCII name", g, carol, "caf\u00e9", RoleRead, false},
		{"subject with a suffix", g, p(issA, "alice2", nil), "orders", RoleRead, false},
		{"subject prefix", g, p(issA, "alic", nil), "orders", RoleRead, false},
		{"subject inside another", g, p(issA, "xalicex", nil), "orders", RoleRead, false},
		{"subject in another case", g, p(issA, "Alice", nil), "orders", RoleRead, false},
		{"claim string with a suffix", g, group("analysts2"), "report-q1", RoleRead, false},
		{"claim string containing the value", g, group("xanalystsx"), "report-q1", RoleRead, false},
		{"claim string prefix", g, group("analyst"), "report-q1", RoleRead, false},
		{"claim string in another case", g, group("Analysts"), "report-q1", RoleRead, false},
		{"claim entries near the value", g, group([]any{"analysts2", "Analysts", "analyst"}), "report-q1", RoleRead, false},
		{"pattern ? matches one character", g, p(issA, "frank", nil), "abc", RoleRead, true},
		{"pattern ? needs a character", g, p(issA, "frank", nil), "ac", RoleRead, false},
		{"pattern ? matches only one", g, p(issA, "frank", nil), "abbc", RoleRead, false},
		{"unknown role required", g, carol, "orders", "owner", false},
		{"pattern matches", g, analyst, "report-q1", RoleRead, true},
		{"pattern does not match", g, analyst, "reports", RoleRead, false},
		{"claim rule grants only its role", g, analyst, "report-q1", RoleWrite, false},
		{"claim as a string", g, p(issA, "erin", map[string]any{"groups": "analysts"}), "report-q1", RoleRead, true},
		{"claim value absent", g, p(issA, "erin", map[string]any{"groups": []any{"staff"}}), "report-q1", RoleRead, false},
		{"claim of another type", g, p(issA, "erin", map[string]any{"groups": []any{1.0, true}}), "report-q1", RoleRead, false},
		{"subject equal to the claim value", g, p(issA, "analysts", nil), "report-q1", RoleRead, false},
		{"roles claim", g, p(issA, "app", map[string]any{"roles": []any{"skylite.writer"}}), "inventory", RoleWrite, true},
		{"same subject, other issuer", g, p(issB, "alice", nil), "orders", RoleRead, false},
		{"other issuer's own rule", g, p(issB, "alice", nil), "b-only", RoleRead, true},
		{"issuer A subject on issuer B rule", g, alice, "b-only", RoleRead, false},
		{"claim rule needs its issuer", g, p(issB, "dave", map[string]any{"groups": []any{"analysts"}}), "report-q1", RoleRead, false},
	}
	for _, tc := range cases {
		if got := tc.g.Authorize(tc.p, tc.db, tc.need); got != tc.want {
			t.Errorf("%s: Authorize = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestBadPatternNeverGrants(t *testing.T) {
	// Config.Validate rejects these; a Guard built around it must still deny.
	for _, pattern := range []string{"[", `\`, "a["} {
		g := &Guard{rules: []Rule{{Issuer: issA, Subject: "alice", Database: pattern, Role: RoleAdmin}}}
		for _, db := range []string{"a", "x"} {
			if g.Authorize(&Principal{Issuer: issA, Subject: "alice"}, db, RoleRead) {
				t.Errorf("pattern %q granted %q", pattern, db)
			}
		}
	}
}
