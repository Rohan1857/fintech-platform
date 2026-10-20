package money

import (
	"database/sql/driver"
	"errors"
	"fmt"
)

// Money represents monetary amounts in the currency's smallest indivisible unit
// (e.g. Paise for INR, Cents for USD).
// Floats are explicitly prohibited to eliminate floating-point rounding errors.
type Money int64

// Common currency codes
const (
	CurrencyINR = "INR"
	CurrencyUSD = "USD"
	CurrencyEUR = "EUR"
)

// Standard minor unit multipliers (1 INR = 100 paise)
const (
	MinorUnitsINR = 100
	MinorUnitsUSD = 100
)

// FromMajor converts a major currency unit to minor units (e.g., 50.25 INR -> 5025 Paise).
func FromMajor(major int64, minorUnits int64) Money {
	return Money(major * minorUnits)
}

// ToMajorString returns formatted string representation with decimals.
func (m Money) ToMajorString() string {
	sign := ""
	val := int64(m)
	if val < 0 {
		sign = "-"
		val = -val
	}
	major := val / 100
	minor := val % 100
	return fmt.Sprintf("%s%d.%02d", sign, major, minor)
}

// Value implements driver.Valuer for database storage.
func (m Money) Value() (driver.Value, error) {
	return int64(m), nil
}

// Scan implements sql.Scanner for database retrieval.
func (m *Money) Scan(value any) error {
	if value == nil {
		*m = 0
		return nil
	}
	switch v := value.(type) {
	case int64:
		*m = Money(v)
		return nil
	case int32:
		*m = Money(v)
		return nil
	case int:
		*m = Money(v)
		return nil
	default:
		return errors.New("cannot scan non-integer into Money")
	}
}
