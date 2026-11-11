package tungo

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// The API expresses money in tiyn, 1/100 of a tenge, as 64-bit integers.

// ToTiyn converts a decimal amount in tenge, written as a string, to tiyn. The grammar is
// -?digits[.digits] in ASCII, nothing else (no spaces, exponent, plus sign or separators). The
// conversion is exact; more than two decimals are rounded half away from zero ("1.005" is 101).
// A malformed amount or one outside the int64 range is a *ValidationError.
func ToTiyn(amount string) (int64, error) {
	fail := func(msg string) (int64, error) {
		return 0, &ValidationError{Param: "amount", Message: msg}
	}
	s := amount
	negative := strings.HasPrefix(s, "-")
	if negative {
		s = s[1:]
	}
	whole, frac, hasPoint := strings.Cut(s, ".")
	if whole == "" || (hasPoint && frac == "") || !asciiDigits(whole) || !asciiDigits(frac) {
		return fail("expected a decimal number such as \"1234.56\", got " + strconv.Quote(amount))
	}
	frac += "000"
	n, ok := new(big.Int).SetString(whole+frac[:2], 10)
	if !ok {
		return fail("expected a decimal number, got " + strconv.Quote(amount))
	}
	if frac[2] >= '5' {
		n.Add(n, big.NewInt(1))
	}
	if negative {
		n.Neg(n)
	}
	if !n.IsInt64() {
		return fail("the amount " + strconv.Quote(amount) + " does not fit in 64-bit tiyn")
	}
	return n.Int64(), nil
}

func asciiDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ToTiynFloat converts a binary floating-point amount in tenge to tiyn. The float is first
// written in its shortest decimal form that round-trips (so 1.005 is "1.005", not
// 1.00499999999999989...), then converted exactly like ToTiyn. Prefer ToTiyn with a string, or
// ToTiynRat, when the amount does not start its life as a float. NaN, an infinity and an amount
// outside the int64 range are a *ValidationError.
func ToTiynFloat(amount float64) (int64, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, &ValidationError{Param: "amount", Message: "the amount must be a finite number"}
	}
	return ToTiyn(strconv.FormatFloat(amount, 'f', -1, 64))
}

// ToTiynRat converts an exact rational amount in tenge (math/big is the standard library's
// exact decimal arithmetic) to tiyn, rounding half away from zero. A nil amount or one outside
// the int64 range is a *ValidationError.
func ToTiynRat(amount *big.Rat) (int64, error) {
	if amount == nil {
		return 0, &ValidationError{Param: "amount", Message: "the amount must not be nil"}
	}
	// round(|x| * 100) = floor((2 * |x| * 100 * denom + denom) / (2 * denom))
	scaled := new(big.Rat).Mul(new(big.Rat).Abs(amount), big.NewRat(100, 1))
	num := new(big.Int).Mul(scaled.Num(), big.NewInt(2))
	num.Add(num, scaled.Denom())
	n := num.Quo(num, new(big.Int).Mul(scaled.Denom(), big.NewInt(2)))
	if amount.Sign() < 0 {
		n.Neg(n)
	}
	if !n.IsInt64() {
		return 0, &ValidationError{Param: "amount", Message: "the amount " + amount.FloatString(2) + " does not fit in 64-bit tiyn"}
	}
	return n.Int64(), nil
}

// TiynToDecimal writes tiyn as a decimal amount in tenge with two decimals ("-0.50"), the form
// ToTiyn accepts. It is exact for every int64.
func TiynToDecimal(tiyn int64) string {
	whole, frac := splitTiyn(tiyn)
	sign := ""
	if tiyn < 0 {
		sign = "-"
	}
	return sign + whole + "." + frac
}

// splitTiyn returns the absolute whole part and the two-digit fraction without overflowing on
// math.MinInt64.
func splitTiyn(tiyn int64) (whole, frac string) {
	abs := uint64(tiyn)
	if tiyn < 0 {
		abs = -abs // two's complement: correct for MinInt64 as well
	}
	frac = strconv.FormatUint(abs%100, 10)
	if len(frac) < 2 {
		frac = "0" + frac
	}
	return strconv.FormatUint(abs/100, 10), frac
}

// FormatTiyn formats tiyn for display: "1 234 567,89 ₸". Thousands are separated by a space, the
// decimal mark is a comma, and the amount is followed by a space and the currency symbol
// (KZT ₸, USD $, EUR €, RUB ₽; the code is case-insensitive and "" means KZT) or, for any other
// currency, its upper-cased code ("1 234,56 GBP").
func FormatTiyn(tiyn int64, currency string) string {
	symbol := strings.ToUpper(currency)
	switch symbol {
	case "", "KZT":
		symbol = "₸"
	case "USD":
		symbol = "$"
	case "EUR":
		symbol = "€"
	case "RUB":
		symbol = "₽"
	}
	whole, frac := splitTiyn(tiyn)
	var b strings.Builder
	if tiyn < 0 {
		b.WriteByte('-')
	}
	lead := len(whole) % 3
	if lead == 0 {
		lead = 3
	}
	b.WriteString(whole[:lead])
	for i := lead; i < len(whole); i += 3 {
		b.WriteByte(' ')
		b.WriteString(whole[i : i+3])
	}
	b.WriteString("," + frac + " " + symbol)
	return b.String()
}

// IsValidBINIIN checks a Kazakhstan business or individual identification number: exactly 12
// ASCII digits with a correct check digit. The input is not trimmed.
func IsValidBINIIN(value string) bool {
	if len(value) != 12 || !asciiDigits(value) {
		return false
	}
	var digits [12]int
	for i := range digits {
		digits[i] = int(value[i] - '0')
	}
	sum := 0
	for i := 0; i < 11; i++ {
		sum += digits[i] * (i + 1)
	}
	control := sum % 11
	if control == 10 {
		sum = 0
		for i := 0; i < 11; i++ {
			sum += digits[i] * ((i+2)%11 + 1)
		}
		control = sum % 11
	}
	return control < 10 && control == digits[11]
}
