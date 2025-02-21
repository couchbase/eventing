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

func ValidateString[T map[string]struct{} | []fmt.Stringer](value interface{}, possibleValues T) error {
	stringValue, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value")
	}

	if possibleValues == nil {
		return nil
	}

	switch v := any(possibleValues).(type) {
	case map[string]struct{}:
		if _, ok := v[stringValue]; ok {
			return nil
		}

	case []fmt.Stringer:
		for _, val := range v {
			if val.String() == stringValue {
				return nil
			}
		}
	}

	return fmt.Errorf("value should be one of %v", possibleValues)
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
		return fmt.Errorf("expected array of %s", valType)
	}

	for _, val := range values {
		switch valType {
		case TypeString:
			err := ValidateString[[]fmt.Stringer](val, nil)
			if err != nil {
				return err
			}

		case TypeInt:
			err := ValidateInteger[float64](val, NewOptional[float64](), NewOptional[float64](), nil)
			if err != nil {
				return err
			}

		case TypeBoolean:
			err := ValidateBoolean(val)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("invalid type supplied")
		}
	}
	return nil
}
