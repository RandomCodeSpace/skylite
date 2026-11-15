package auth

import "testing"

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
	}
	for i, cfg := range good {
		if err := cfg.Validate(); err != nil {
			t.Errorf("good config %d: %v", i, err)
		}
	}
	bad := map[string]Config{
		"no issuers":                {},
		"empty issuer":              {Issuers: []Issuer{{Audience: "aud"}}},
		"empty audience":            {Issuers: []Issuer{{Issuer: issA}}},
		"http issuer":               {Issuers: []Issuer{{Issuer: "http://a.example", Audience: "aud"}}},
		"issuer without host":       {Issuers: []Issuer{{Issuer: "https:///path", Audience: "aud"}}},
		"issuer with query":         {Issuers: []Issuer{{Issuer: issA + "?x=1", Audience: "aud"}}},
		"issuer with user info":     {Issuers: []Issuer{{Issuer: "https://user@a.example", Audience: "aud"}}},
		"issuer twice":              {Issuers: append(iss, iss...)},
		"rule for unknown issuer":   withRule(func(r *Rule) { r.Issuer = issB }),
		"subject and claim":         withRule(func(r *Rule) { r.Claim, r.Value = "groups", "x" }),
		"neither subject nor claim": withRule(func(r *Rule) { r.Subject = "" }),
		"claim without value":       withRule(func(r *Rule) { r.Subject, r.Claim = "", "groups" }),
		"empty database":            withRule(func(r *Rule) { r.Database = "" }),
		"bad pattern":               withRule(func(r *Rule) { r.Database = "[" }),
		"unknown role":              withRule(func(r *Rule) { r.Role = "owner" }),
		"empty role":                withRule(func(r *Rule) { r.Role = "" }),
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
			{Issuer: issA, Claim: "groups", Value: "analysts", Database: "report_*", Role: RoleRead},
			{Issuer: issA, Claim: "roles", Value: "skylite.writer", Database: "inventory", Role: RoleWrite},
			{Issuer: issB, Subject: "alice", Database: "b_only", Role: RoleRead},
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
		{"unknown role required", g, carol, "orders", "owner", false},
		{"pattern matches", g, analyst, "report_q1", RoleRead, true},
		{"pattern does not match", g, analyst, "reports", RoleRead, false},
		{"claim rule grants only its role", g, analyst, "report_q1", RoleWrite, false},
		{"claim as a string", g, p(issA, "erin", map[string]any{"groups": "analysts"}), "report_q1", RoleRead, true},
		{"claim value absent", g, p(issA, "erin", map[string]any{"groups": []any{"staff"}}), "report_q1", RoleRead, false},
		{"claim of another type", g, p(issA, "erin", map[string]any{"groups": []any{1.0, true}}), "report_q1", RoleRead, false},
		{"subject equal to the claim value", g, p(issA, "analysts", nil), "report_q1", RoleRead, false},
		{"roles claim", g, p(issA, "app", map[string]any{"roles": []any{"skylite.writer"}}), "inventory", RoleWrite, true},
		{"same subject, other issuer", g, p(issB, "alice", nil), "orders", RoleRead, false},
		{"other issuer's own rule", g, p(issB, "alice", nil), "b_only", RoleRead, true},
		{"issuer A subject on issuer B rule", g, alice, "b_only", RoleRead, false},
		{"claim rule needs its issuer", g, p(issB, "dave", map[string]any{"groups": []any{"analysts"}}), "report_q1", RoleRead, false},
	}
	for _, tc := range cases {
		if got := tc.g.Authorize(tc.p, tc.db, tc.need); got != tc.want {
			t.Errorf("%s: Authorize = %v, want %v", tc.name, got, tc.want)
		}
	}
}
