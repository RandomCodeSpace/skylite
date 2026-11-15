package auth

import "path"

// Authorize reports whether p holds at least role need on database. It is
// deny by default: without a matching rule the answer is false.
func (g *Guard) Authorize(p *Principal, database string, need Role) bool {
	if p == nil || database == "" || need.rank() == 0 {
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
	return err == nil && ok // Config.Validate rejects bad patterns
}
