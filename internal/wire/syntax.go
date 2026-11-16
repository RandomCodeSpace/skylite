package wire

import (
	"bytes"
	"errors"
	"hash/maphash"
	"slices"

	"github.com/go-json-experiment/json/jsontext"
)

// MaxDepth is the deepest a body may nest arrays and objects, its root
// object counting as one level. A deeper body is refused before acceptance.
const MaxDepth = 16

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
	errDepth     = errors.New("arrays and objects nest more than 16 deep")
	nameSeed     = maphash.MakeSeed()
)

// nameHash hashes an object member's name. Tests replace it to force
// collisions.
var nameHash = func(name []byte) uint64 { return maphash.Bytes(nameSeed, name) }

// checkSyntax checks that data is exactly one JSON value, from its first
// byte to its last, nested at most MaxDepth deep, with no duplicate member
// name in any object.
func checkSyntax(data []byte) error {
	if len(data) == 0 || isSpace(data[0]) {
		return errLeading
	}
	type level struct {
		start  int64
		object bool
		hashes []uint64
	}
	var levels [MaxDepth]level
	stack := levels[:0]
	var scratch []byte
	d := newDecoder(data)
	for {
		switch k := d.PeekKind(); k {
		case '{', '[':
			if len(stack) == MaxDepth {
				return errDepth
			}
			if _, err := d.ReadToken(); err != nil {
				return err
			}
			n := len(stack)
			stack = stack[:n+1]
			stack[n].start, stack[n].object = d.InputOffset()-1, k == '{'
			stack[n].hashes = stack[n].hashes[:0]
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
				top.hashes = append(top.hashes, nameHash(s))
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
// members of the same name, given the hashes of its names, which it sorts.
// Only names whose hash occurs more than once are compared, one candidate
// name at a time against the later names of its hash, so that it keeps no
// more than one name, whatever the object holds.
func hasDuplicate(data []byte, hashes []uint64) bool {
	slices.Sort(hashes)
	var name []byte
	for i := 0; i < len(hashes); {
		j := i + 1
		for j < len(hashes) && hashes[j] == hashes[i] {
			j++
		}
		// Each pass takes the nth name of the hash as the candidate; a
		// duplicate is found on the pass of its first occurrence.
		for nth := 0; nth < j-i-1; nth++ {
			found, ok := sameName(data, hashes[i], nth, &name)
			if found || !ok {
				return true
			}
		}
		i = j
	}
	return false
}

// sameName rescans the object at the start of data and reports whether a
// name after the nth name of hash h equals it. It reports !ok for an object
// that does not decode, which checkSyntax has already ruled out.
func sameName(data []byte, h uint64, nth int, name *[]byte) (found, ok bool) {
	d := newDecoder(data)
	if _, err := d.ReadToken(); err != nil {
		return false, false
	}
	var scratch []byte
	seen := 0
	for d.PeekKind() == '"' {
		raw, err := d.ReadValue()
		if err != nil {
			return false, false
		}
		s, err := unquote(raw, &scratch)
		if err != nil {
			return false, false
		}
		if nameHash(s) == h {
			switch {
			case seen == nth:
				*name = append((*name)[:0], s...)
			case seen > nth && bytes.Equal(s, *name):
				return true, true
			}
			seen++
		}
		if err := d.SkipValue(); err != nil {
			return false, false
		}
	}
	return false, true
}
