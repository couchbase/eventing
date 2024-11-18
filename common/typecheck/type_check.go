package typecheck

import (
	"fmt"
)

type Optional[T any] struct {
	val     T
	present bool
}

func NewOptional[T any]() Optional[T] {
	return Optional[T]{}
}

func (o Optional[T]) Set(val T) Optional[T] {
	o.val = val
	o.present = true
	return o
}

func (o Optional[T]) Get() (T, bool) {
	return o.val, o.present
}

// Validate the generic values
func ValidateInteger[T float64 | int](value interface{}, low, high Optional[T], possibleValues map[T]struct{}) error {
	intValue, ok := value.(T)
	if !ok {
		return fmt.Errorf("expected interger value")
	}

	if possibleValues != nil {
		if _, ok := possibleValues[intValue]; !ok {
			return fmt.Errorf("value should be %v", possibleValues)
		}
	}

	if lValue, ok := low.Get(); ok && intValue < lValue {
		return fmt.Errorf("value should be greater than or equal to %v", lValue)
	}

	if hValue, ok := high.Get(); ok && intValue > hValue {
		return fmt.Errorf("value should be less than or equal to %v", hValue)
	}

	return nil
}

func ValidateBoolean(value interface{}) error {
	if _, ok := value.(bool); !ok {
		return fmt.Errorf("expected boolean value")
	}
	return nil
}

func ValidateString(value interface{}, possibleValues map[string]struct{}) error {
	stringValue, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value")
	}

	if possibleValues == nil {
		return nil
	}

	if _, ok := possibleValues[stringValue]; !ok {
		return fmt.Errorf("value should be %v", possibleValues)
	}
	return nil
}

type checkType int8

const (
	TypeString checkType = iota
	TypeInt
	TypeBoolean
)

func (c checkType) String() string {
	switch c {
	case TypeString:
		return "string"

	case TypeInt:
		return "int"

	case TypeBoolean:
		return "boolean"
	}

	return "unknown"
}

func ValidateArray(val interface{}, valType checkType) error {
	values, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf("expected slice of %s", valType)
	}

	switch valType {
	case TypeString:
		for _, val := range values {
			err := ValidateString(val, nil)
			if err != nil {
				return err
			}
		}

	case TypeInt:
		for _, val := range values {
			err := ValidateInteger[float64](val, NewOptional[float64](), NewOptional[float64](), nil)
			if err != nil {
				return err
			}
		}

	case TypeBoolean:
		for _, val := range values {
			err := ValidateBoolean(val)
			if err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("invalid type supplied")
	}

	return nil
}
