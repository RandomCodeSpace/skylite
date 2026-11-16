package wire

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"regexp"
	"strings"
	"testing"
)

type vector struct {
	Name       string `json:"name"`
	Wire       string `json:"wire"`
	WireHex    string `json:"wire_hex"`
	Class      string `json:"class"`
	Value      string `json:"value"`
	DecodeOnly bool   `json:"decode_only"`
	Error      string `json:"error"`
}

// describe returns a value's storage class and its value as the vectors
// write it.
func describe(v any) (string, string) {
	switch v := v.(type) {
	case nil:
		return "null", ""
	case int64:
		return "integer", fmt.Sprint(v)
	case float64:
		return "real", fmt.Sprintf("%016x", math.Float64bits(v))
	case string:
		return "text", hex.EncodeToString([]byte(v))
	case []byte:
		return "blob", hex.EncodeToString(v)
	}
	return fmt.Sprintf("%T", v), ""
}

func TestConformanceVectors(t *testing.T) {
	f, err := os.Open("../../clients/conformance/values.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 8<<20)
	n := 0
	for sc.Scan() {
		var v vector
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatalf("line %d: %v", n+1, err)
		}
		n++
		wire := []byte(v.Wire)
		if v.WireHex != "" {
			if wire, err = hex.DecodeString(v.WireHex); err != nil {
				t.Fatalf("%s: %v", v.Name, err)
			}
		}
		t.Run(v.Name, func(t *testing.T) { checkVector(t, v, wire) })
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 100 {
		t.Fatalf("read %d vectors", n)
	}
}

func checkVector(t *testing.T, v vector, wire []byte) {
	got, err := DecodeValue(wire)
	// The server's path: a query's parameter, checked by Parse, then decoded by Bind.
	body := append(append([]byte(`{"sql":"?","params":[`), wire...), "]}"...)
	r, perr := Parse(KindQuery, body, 10)
	if v.Error != "" {
		var we *Error
		if !errors.As(err, &we) || string(we.Code) != v.Error {
			t.Fatalf("DecodeValue = %v, %v; want error %s", got, err, v.Error)
		}
		if len(wire) > 0 && !isSpace(wire[0]) && !isSpace(wire[len(wire)-1]) {
			if perr == nil || string(perr.Code) != v.Error {
				t.Fatalf("Parse = %v; want error %s", perr, v.Error)
			}
		}
		return
	}
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if class, value := describe(got); class != v.Class || value != v.Value {
		t.Fatalf("DecodeValue = %s %s; want %s %s", class, value, v.Class, v.Value)
	}
	if perr != nil {
		t.Fatalf("Parse: %v", perr)
	}
	var bound any
	if err := r.Bind(r.Args, []string{""}, func(_ int, v any) error { bound = v; return nil }); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if class, value := describe(bound); class != v.Class || value != v.Value {
		t.Fatalf("Bind = %s %s; want %s %s", class, value, v.Class, v.Value)
	}
	if v.DecodeOnly {
		return
	}
	enc, err := AppendValue(nil, got)
	if err != nil || !bytes.Equal(enc, wire) {
		t.Fatalf("AppendValue = %q, %v; want %q", enc, err, wire)
	}
	if v.Name == "text at the frame size" {
		var buf bytes.Buffer
		w := NewWriter(&buf, DefaultFrameBytes, DefaultLimits())
		if err := w.Columns([]Column{{Name: "t"}}); err != nil {
			t.Fatal(err)
		}
		before := buf.Len()
		if err := w.Row([]any{got}); err != nil || buf.Len()-before != DefaultFrameBytes {
			t.Fatalf("row frame is %d bytes, %v; want %d", buf.Len()-before, err, DefaultFrameBytes)
		}
	}
}

func TestAppendValueRefuses(t *testing.T) {
	for _, v := range []any{math.NaN(), true, 1, float32(1), []any{}} {
		if b, err := AppendValue(nil, v); err == nil {
			t.Errorf("AppendValue(%#v) = %q; want an error", v, b)
		}
	}
}

var realForm = regexp.MustCompile(`^-?[0-9](\.[0-9]*[1-9])?e(0|-?[1-9][0-9]*)$`)

// TestFormatRealShortestNearest proves, for a million random doubles, the
// subnormals nearest zero and every power of two, that FormatReal writes
// the canonical form: the fewest significant digits that round back to the
// double, and of those the digits nearest its exact value, the even one on a
// tie. The check uses exact integer arithmetic, not strconv.
func TestFormatRealShortestNearest(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed seed, so a failure can be reproduced
	var xs []float64
	for len(xs) < 1_000_000 {
		if x := math.Float64frombits(rng.Uint64()); !math.IsNaN(x) && !math.IsInf(x, 0) {
			xs = append(xs, x)
		}
	}
	for b := range uint64(100_000) {
		xs = append(xs, math.Float64frombits(b), math.Float64frombits(0x000fffffffffffff-b))
	}
	for e := range uint64(2047) {
		xs = append(xs, math.Float64frombits(e<<52))
	}
	for _, x := range xs {
		if err := checkReal(x); err != nil {
			t.Fatalf("%016x: %v", math.Float64bits(x), err)
		}
	}
}

func checkReal(x float64) error {
	s := FormatReal(x)
	if !realForm.MatchString(s) {
		return fmt.Errorf("%q is not in the canonical form", s)
	}
	if math.Signbit(x) != strings.HasPrefix(s, "-") {
		return fmt.Errorf("%q has the wrong sign", s)
	}
	if x == 0 {
		if strings.TrimPrefix(s, "-") != "0e0" {
			return fmt.Errorf("zero is %q", s)
		}
		return nil
	}
	// The digits as an integer k and its scale p: the value is k * 10^p.
	mant, exp, _ := strings.Cut(strings.TrimPrefix(s, "-"), "e")
	digits := strings.Replace(mant, ".", "", 1)
	var e10 big.Int
	e10.SetString(exp, 10)
	k, _ := new(big.Int).SetString(digits, 10)
	p := int(e10.Int64()) - len(digits) + 1
	if digits[0] == '0' {
		return fmt.Errorf("%q has a leading zero", s)
	}
	b := newBinary(math.Abs(x))
	want := b.nearest(len(digits))
	if want == nil || want.k.Cmp(k) != 0 || want.p != p {
		return fmt.Errorf("%q is not the nearest %d-digit decimal that rounds back", s, len(digits))
	}
	if len(digits) > 1 && b.nearest(len(digits)-1) != nil {
		return fmt.Errorf("%q is not the shortest", s)
	}
	return nil
}

// binary is a positive finite double x = m * 2^e with its rounding interval:
// the reals that round to x lie strictly between lo and hi, in units of
// 2^(e-2), and also at lo and hi when m is even.
type binary struct {
	m, e   int
	lo, hi *big.Int
	even   bool
}

func newBinary(x float64) *binary {
	bits := math.Float64bits(x)
	frac, be := int(bits&(1<<52-1)), int(bits>>52)
	b := &binary{m: frac, e: -1074}
	if be > 0 {
		b.m, b.e = frac|1<<52, be-1075
	}
	m4 := big.NewInt(int64(4 * b.m))
	b.hi = new(big.Int).Add(m4, big.NewInt(2))
	if frac == 0 && be > 1 { // the double below is twice as close
		b.lo = new(big.Int).Sub(m4, big.NewInt(1))
	} else {
		b.lo = new(big.Int).Sub(m4, big.NewInt(2))
	}
	b.even = b.m%2 == 0
	return b
}

var ten = big.NewInt(10)

// pow10[i] is 10^i, for the exponents a double's decimals need.
var pow10 = func() []*big.Int {
	t := []*big.Int{big.NewInt(1)}
	for len(t) < 400 {
		t = append(t, new(big.Int).Mul(t[len(t)-1], ten))
	}
	return t
}()

// scaled returns k * 10^p in units of 2^(e-2) as num/den.
func (b *binary) scaled(k *big.Int, p int) (num, den *big.Int) {
	num, den = new(big.Int).Set(k), big.NewInt(1)
	if p >= 0 {
		num.Mul(num, pow10[p])
	} else {
		den.Set(pow10[-p])
	}
	if s := 2 - b.e; s >= 0 {
		num.Lsh(num, uint(s))
	} else {
		den.Lsh(den, uint(-s))
	}
	return num, den
}

// cmp compares k * 10^p with a * 2^(e-2).
func (b *binary) cmp(k *big.Int, p int, a *big.Int) int {
	num, den := b.scaled(k, p)
	return num.Cmp(new(big.Int).Mul(a, den))
}

func (b *binary) roundsBack(k *big.Int, p int) bool {
	lo, hi := b.cmp(k, p, b.lo), b.cmp(k, p, b.hi)
	if b.even {
		return lo >= 0 && hi <= 0
	}
	return lo > 0 && hi < 0
}

type decimal struct {
	k *big.Int
	p int
}

// nearest returns the decimal of n significant digits nearest x that rounds
// back to x, the even one on a tie, with trailing zeros removed, or nil.
func (b *binary) nearest(n int) *decimal {
	x := big.NewInt(int64(4 * b.m)) // x in units of 2^(e-2)
	// d = floor(log10 x): the largest d with 10^d <= x.
	d := int(math.Floor(math.Log10(math.Ldexp(float64(b.m), b.e))))
	for b.cmp(big.NewInt(1), d, x) > 0 {
		d--
	}
	for b.cmp(big.NewInt(1), d+1, x) <= 0 {
		d++
	}
	p := d - n + 1
	// floor(x / 10^p) = floor(x * 2^(e-2) / 10^p): invert the scaling.
	num, den := b.scaled(big.NewInt(1), p) // 10^p in units: num/den
	q, r := new(big.Int).QuoRem(new(big.Int).Mul(x, den), num, new(big.Int))
	lo, hi := q, new(big.Int).Add(q, big.NewInt(1))
	var cands []*big.Int
	switch c := new(big.Int).Lsh(r, 1).Cmp(num); {
	case r.Sign() == 0:
		cands = []*big.Int{lo}
	case c < 0:
		cands = []*big.Int{lo, hi}
	case c > 0:
		cands = []*big.Int{hi, lo}
	case lo.Bit(0) == 0:
		cands = []*big.Int{lo, hi}
	default:
		cands = []*big.Int{hi, lo}
	}
	for _, k := range cands {
		if b.roundsBack(k, p) {
			for {
				q, r := new(big.Int).QuoRem(k, ten, new(big.Int))
				if r.Sign() != 0 {
					return &decimal{k, p}
				}
				k, p = q, p+1
			}
		}
	}
	return nil
}
