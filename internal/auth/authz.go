package auth

import "path"

// Authorize reports whether p holds at least role need on database. It is
// deny by default: without a matching rule the answer is false, and a name
// that is not a valid database name is denied before any rule is read.
// Subjects, claim values and names are compared exactly, byte for byte.
func (g *Guard) Authorize(p *Principal, database string, need Role) bool {
	if p == nil || !validDatabaseName(database) || need.rank() == 0 {
		return false
	}
	for _, r := range g.rules {
		if r.Issuer == p.Issuer && r.Role.rank() >= need.rank() && r.matches(p) && matchDatabase(r.Database, database) {
			return true
		}
	}
	return false
}

func (r Rule) matches(p *Principal) bool {
	if r.Subject != "" {
		return r.Subject == p.Subject
	}
	switch v := p.Claims[r.Claim].(type) {
	case string:
		return v == r.Value
	case []any:
		for _, e := range v {
			if s, ok := e.(string); ok && s == r.Value {
				return true
			}
		}
	}
	return false
}

func matchDatabase(pattern, name string) bool {
	ok, err := path.Match(pattern, name)
	return err == nil && ok // Config.Validate allows only * and ? in a pattern, so this cannot fail
}
