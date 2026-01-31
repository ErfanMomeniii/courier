package courier

import (
	"context"
	"errors"
	"fmt"
)

// ErrValidation is returned when a record fails validation.
var ErrValidation = errors.New("courier: validation failed")

// ValidationError contains details about a validation failure.
type ValidationError struct {
	Record  interface{}
	Message string
	Err     error
}

func (e *ValidationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("courier: validation failed: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("courier: validation failed: %s", e.Message)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// Validator is a function that validates a record.
// Return nil if valid, or an error describing the validation failure.
type Validator[T any] func(T) error

// ValidatingApplier wraps an applier to validate records before applying.
type ValidatingApplier[T any] struct {
	applier    Applier[T]
	validators []Validator[T]
	skipFailed bool // if true, failed validations count as synced (skipped)
}

// NewValidatingApplier creates an applier that validates records.
// If skipFailed is true, records that fail validation are skipped (counted as synced).
// If skipFailed is false, records that fail validation are counted as failed.
func NewValidatingApplier[T any](applier Applier[T], skipFailed bool, validators ...Validator[T]) *ValidatingApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if len(validators) == 0 {
		panic("courier: at least one validator is required")
	}
	return &ValidatingApplier[T]{
		applier:    applier,
		validators: validators,
		skipFailed: skipFailed,
	}
}

// Apply implements Applier with validation.
func (v *ValidatingApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	var valid []T
	var invalid []T
	var skipped []T

	for _, r := range records {
		if err := v.validate(r); err != nil {
			if v.skipFailed {
				skipped = append(skipped, r)
			} else {
				invalid = append(invalid, r)
			}
		} else {
			valid = append(valid, r)
		}
	}

	if len(valid) == 0 {
		if v.skipFailed {
			return skipped, nil, nil
		}
		return nil, invalid, nil
	}

	synced, failed, err := v.applier.Apply(ctx, valid)

	// Add skipped to synced
	synced = append(synced, skipped...)
	// Add invalid to failed
	failed = append(failed, invalid...)

	return synced, failed, err
}

func (v *ValidatingApplier[T]) validate(record T) error {
	for _, validator := range v.validators {
		if err := validator(record); err != nil {
			return err
		}
	}
	return nil
}

// ValidateMiddleware creates a middleware that validates records.
func ValidateMiddleware[T any](skipFailed bool, validators ...Validator[T]) Middleware[T] {
	if len(validators) == 0 {
		panic("courier: at least one validator is required")
	}
	return func(applier Applier[T]) Applier[T] {
		return NewValidatingApplier(applier, skipFailed, validators...)
	}
}

// Common validators

// NotEmpty creates a validator that checks if a string field is not empty.
func NotEmpty[T any](getField func(T) string, fieldName string) Validator[T] {
	return func(record T) error {
		if getField(record) == "" {
			return &ValidationError{
				Record:  record,
				Message: fieldName + " is required",
			}
		}
		return nil
	}
}

// NotNil creates a validator that checks if a pointer field is not nil.
func NotNil[T any, P any](getField func(T) *P, fieldName string) Validator[T] {
	return func(record T) error {
		if getField(record) == nil {
			return &ValidationError{
				Record:  record,
				Message: fieldName + " is required",
			}
		}
		return nil
	}
}

// Positive creates a validator that checks if an int field is positive.
func Positive[T any](getField func(T) int, fieldName string) Validator[T] {
	return func(record T) error {
		if getField(record) <= 0 {
			return &ValidationError{
				Record:  record,
				Message: fieldName + " must be positive",
			}
		}
		return nil
	}
}

// InRange creates a validator that checks if an int field is within range.
func InRange[T any](getField func(T) int, min, max int, fieldName string) Validator[T] {
	return func(record T) error {
		v := getField(record)
		if v < min || v > max {
			return &ValidationError{
				Record:  record,
				Message: fmt.Sprintf("%s must be between %d and %d", fieldName, min, max),
			}
		}
		return nil
	}
}

// MatchesPattern creates a validator using a custom predicate.
func MatchesPattern[T any](predicate func(T) bool, message string) Validator[T] {
	return func(record T) error {
		if !predicate(record) {
			return &ValidationError{
				Record:  record,
				Message: message,
			}
		}
		return nil
	}
}
