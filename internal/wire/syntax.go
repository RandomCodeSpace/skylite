package wire

import (
	"bytes"
	"errors"
	"hash/maphash"
	"slices"

	"github.com/go-json-experiment/json/jsontext"
)

// newDecoder reads data in place. Duplicate names are left to checkSyntax:
// jsontext finds them with a map per object, about 135 bytes per name,
// which breaks ADR 0005's bound of 7 bytes per body byte for a decoded
// body; checkSyntax keeps 8 bytes per name. jsontext still refuses invalid
// UTF-8, bad escapes and lone surrogates.
func newDecoder(data []byte) *jsontext.Decoder {
	return jsontext.NewDecoder(bytes.NewBuffer(data), jsontext.AllowDuplicateNames(true))
}

var (
	errDuplicate = errors.New("an object has two members of the same name")
	errTrailing  = errors.New("bytes follow the value")
	errLeading   = errors.New("the value must start at the first byte")
	nameSeed     = maphash.MakeSeed()
)

// checkSyntax checks that data is exactly one JSON value, from its first
// byte to its last, with no duplicate member name in any object.
func checkSyntax(data []byte) error {
	if len(data) == 0 || isSpace(data[0]) {
		return errLeading
	}
	type level struct {
		start  int64
		object bool
		hashes []uint64
	}
	var stack []level
	var scratch []byte
	d := newDecoder(data)
	for {
		switch k := d.PeekKind(); k {
		case '{', '[':
			if _, err := d.ReadToken(); err != nil {
				return err
			}
			n := len(stack)
			if n < cap(stack) {
				stack = stack[:n+1]
				stack[n].hashes = stack[n].hashes[:0]
			} else {
				stack = append(stack, level{})
			}
			stack[n].start, stack[n].object = d.InputOffset()-1, k == '{'
		case '}', ']':
			if _, err := d.ReadToken(); err != nil {
				return err
			}
			top := &stack[len(stack)-1]
			if top.object && hasDuplicate(data[top.start:], top.hashes) {
				return errDuplicate
			}
			stack = stack[:len(stack)-1]
		case 0:
			_, err := d.ReadToken()
			return errOr(err, "invalid JSON")
		default:
			name := false
			if len(stack) > 0 && stack[len(stack)-1].object {
				_, n := d.StackIndex(d.StackDepth())
				name = n%2 == 0
			}
			raw, err := d.ReadValue()
			if err != nil {
				return err
			}
			if name {
				s, err := unquote(raw, &scratch)
				if err != nil {
					return err
				}
				top := &stack[len(stack)-1]
				if len(top.hashes) == cap(top.hashes) {
					top.hashes = slices.Grow(top.hashes, max(len(top.hashes), 8))
				}
				top.hashes = append(top.hashes, maphash.Bytes(nameSeed, s))
			}
		}
		if d.StackDepth() == 0 {
			break
		}
	}
	if d.InputOffset() != int64(len(data)) {
		return errTrailing
	}
	return nil
}

// hasDuplicate reports whether the object at the start of data has two
// members of the same name, given the hashes of its names. Only equal
// hashes lead to comparing the names themselves.
func hasDuplicate(data []byte, hashes []uint64) bool {
	slices.Sort(hashes)
	if len(slices.Compact(hashes)) == len(hashes) {
		return false
	}
	d := newDecoder(data)
	if _, err := d.ReadToken(); err != nil {
		return true
	}
	seen := make(map[string]bool)
	var scratch []byte
	for d.PeekKind() == '"' {
		raw, err := d.ReadValue()
		if err != nil {
			return true
		}
		s, err := unquote(raw, &scratch)
		if err != nil || seen[string(s)] {
			return true
		}
		seen[string(s)] = true
		if err := d.SkipValue(); err != nil {
			return true
		}
	}
	return false
}
