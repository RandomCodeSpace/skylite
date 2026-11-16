package skylite

import (
	"database/sql/driver"
	"errors"
	"strings"
	"time"
)

// CheckNamedValue converts an argument to a value the protocol carries
// (ADR 0005 decision 8): after database/sql's default conversion, a bool
// becomes INTEGER 0 or 1 and a time.Time the canonical instant text, in
// UTC. A time outside the years 0001 to 9999 is refused. The request
// encoder refuses a NaN; either way nothing is sent.
func (cn *conn) CheckNamedValue(nv *driver.NamedValue) error {
	v, err := driver.DefaultParameterConverter.ConvertValue(nv.Value)
	if err != nil {
		return err
	}
	switch x := v.(type) {
	case bool:
		v = int64(0)
		if x {
			v = int64(1)
		}
	case time.Time:
		x = x.UTC()
		if !validYear(x.Year()) {
			return errors.New("skylite: a time must be in the years 0001 to 9999")
		}
		v = x.Format(instantLayout)
	}
	nv.Value = v
	return nil
}

// instantLayout is the canonical form of an instant, in UTC.
const instantLayout = "2006-01-02 15:04:05.000000000"

// validYear reports whether y is in ADR 0005's range of years, 0001 to 9999.
func validYear(y int) bool { return y >= 1 && y <= 9999 }

// typeName returns the name of a declared type as SQLite reads it: without
// the arguments in parentheses that SQLite ignores, trimmed, in upper case.
// "datetime(6)" is DATETIME.
func typeName(decl string) string {
	t, _, _ := strings.Cut(decl, "(")
	return strings.ToUpper(strings.TrimSpace(t))
}

// columnValue returns a value read from a column declared decl: a time.Time,
// in UTC, for TEXT in a date or timestamp form in a column whose type name
// is DATE, DATETIME or TIMESTAMP, and the value unchanged otherwise.
func columnValue(decl string, v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	switch typeName(decl) {
	case "DATE", "DATETIME", "TIMESTAMP":
		if t, ok := parseTime(s); ok {
			return t
		}
	}
	return v
}

// parseTime reads the date and timestamp forms of ADR 0005 decision 8:
// YYYY-MM-DD, then optionally a space or T and HH:MM, HH:MM:SS or HH:MM:SS
// with 1 to 9 fraction digits, then, after a time, optionally Z or an offset
// +HH:MM or -HH:MM. Text without an offset is UTC. A time whose year in UTC
// is outside 0001 to 9999 is refused.
func parseTime(s string) (time.Time, bool) {
	i := 0
	num := func(n int) int { // n digits at i, or -1
		if i+n > len(s) {
			return -1
		}
		v := 0
		for _, c := range []byte(s[i : i+n]) {
			if c < '0' || c > '9' {
				return -1
			}
			v = v*10 + int(c-'0')
		}
		i += n
		return v
	}
	sep := func(c byte) bool {
		if i < len(s) && s[i] == c {
			i++
			return true
		}
		return false
	}
	y := num(4)
	if y < 1 || !sep('-') {
		return time.Time{}, false
	}
	mo := num(2)
	if !sep('-') {
		return time.Time{}, false
	}
	d := num(2)
	h, mi, sec, ns, off := 0, 0, 0, 0, 0
	if i < len(s) {
		if !sep(' ') && !sep('T') {
			return time.Time{}, false
		}
		h = num(2)
		if !sep(':') {
			return time.Time{}, false
		}
		mi = num(2)
		if sep(':') {
			if sec = num(2); sec < 0 {
				return time.Time{}, false
			}
			if sep('.') {
				start := i
				for i < len(s) && i-start < 10 && s[i] >= '0' && s[i] <= '9' {
					ns = ns*10 + int(s[i]-'0')
					i++
				}
				n := i - start
				if n < 1 || n > 9 {
					return time.Time{}, false
				}
				for ; n < 9; n++ {
					ns *= 10
				}
			}
		}
		switch {
		case sep('Z'):
		case i < len(s) && (s[i] == '+' || s[i] == '-'):
			sign := 1
			if s[i] == '-' {
				sign = -1
			}
			i++
			oh := num(2)
			if !sep(':') {
				return time.Time{}, false
			}
			om := num(2)
			if oh < 0 || oh > 23 || om < 0 || om > 59 {
				return time.Time{}, false
			}
			off = sign * (oh*3600 + om*60)
		}
	}
	if i != len(s) || mo < 1 || mo > 12 || d < 1 || h < 0 || h > 23 || mi < 0 || mi > 59 || sec > 59 {
		return time.Time{}, false
	}
	t := time.Date(y, time.Month(mo), d, h, mi, sec, ns, time.FixedZone("", off))
	if t.Day() != d { // 31 April and the like
		return time.Time{}, false
	}
	t = t.UTC()
	return t, validYear(t.Year())
}
