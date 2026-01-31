package courier_test

import (
	"context"
	"errors"
	"testing"

	"github.com/erfanmomeniii/courier"
)

type testEvent struct {
	ID      string
	Type    string
	Payload []byte
	Count   int
}

func TestValidatingApplier_AllValid(t *testing.T) {
	applier := courier.ApplierFunc[testEvent](func(ctx context.Context, records []testEvent) ([]testEvent, []testEvent, error) {
		return records, nil, nil
	})

	validator := courier.NotEmpty(func(e testEvent) string { return e.ID }, "ID")
	validating := courier.NewValidatingApplier(applier, false, validator)

	records := []testEvent{{ID: "1"}, {ID: "2"}}
	synced, failed, err := validating.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestValidatingApplier_SomeInvalid(t *testing.T) {
	applier := courier.ApplierFunc[testEvent](func(ctx context.Context, records []testEvent) ([]testEvent, []testEvent, error) {
		return records, nil, nil
	})

	validator := courier.NotEmpty(func(e testEvent) string { return e.ID }, "ID")
	validating := courier.NewValidatingApplier(applier, false, validator)

	records := []testEvent{{ID: "1"}, {ID: ""}, {ID: "3"}}
	synced, failed, err := validating.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed, got %d", len(failed))
	}
}

func TestValidatingApplier_SkipFailed(t *testing.T) {
	var processedCount int

	applier := courier.ApplierFunc[testEvent](func(ctx context.Context, records []testEvent) ([]testEvent, []testEvent, error) {
		processedCount = len(records)
		return records, nil, nil
	})

	validator := courier.NotEmpty(func(e testEvent) string { return e.ID }, "ID")
	validating := courier.NewValidatingApplier(applier, true, validator) // skipFailed = true

	records := []testEvent{{ID: "1"}, {ID: ""}, {ID: "3"}}
	synced, failed, err := validating.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 3 { // All synced (invalid ones are skipped = synced)
		t.Errorf("expected 3 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed (skipped), got %d", len(failed))
	}
	if processedCount != 2 { // Only valid ones processed
		t.Errorf("expected 2 processed, got %d", processedCount)
	}
}

func TestValidatingApplier_MultipleValidators(t *testing.T) {
	applier := courier.ApplierFunc[testEvent](func(ctx context.Context, records []testEvent) ([]testEvent, []testEvent, error) {
		return records, nil, nil
	})

	validators := []courier.Validator[testEvent]{
		courier.NotEmpty(func(e testEvent) string { return e.ID }, "ID"),
		courier.NotEmpty(func(e testEvent) string { return e.Type }, "Type"),
	}
	validating := courier.NewValidatingApplier(applier, false, validators...)

	records := []testEvent{
		{ID: "1", Type: "a"},         // Valid
		{ID: "", Type: "b"},          // Invalid (no ID)
		{ID: "3", Type: ""},          // Invalid (no Type)
		{ID: "4", Type: "d"},         // Valid
	}
	synced, failed, err := validating.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 2 {
		t.Errorf("expected 2 failed, got %d", len(failed))
	}
}

func TestPositiveValidator(t *testing.T) {
	validator := courier.Positive(func(e testEvent) int { return e.Count }, "Count")

	if err := validator(testEvent{Count: 5}); err != nil {
		t.Errorf("expected valid, got %v", err)
	}
	if err := validator(testEvent{Count: 0}); err == nil {
		t.Error("expected invalid for zero")
	}
	if err := validator(testEvent{Count: -1}); err == nil {
		t.Error("expected invalid for negative")
	}
}

func TestInRangeValidator(t *testing.T) {
	validator := courier.InRange(func(e testEvent) int { return e.Count }, 1, 10, "Count")

	if err := validator(testEvent{Count: 5}); err != nil {
		t.Errorf("expected valid, got %v", err)
	}
	if err := validator(testEvent{Count: 1}); err != nil {
		t.Errorf("expected valid at min, got %v", err)
	}
	if err := validator(testEvent{Count: 10}); err != nil {
		t.Errorf("expected valid at max, got %v", err)
	}
	if err := validator(testEvent{Count: 0}); err == nil {
		t.Error("expected invalid below range")
	}
	if err := validator(testEvent{Count: 11}); err == nil {
		t.Error("expected invalid above range")
	}
}

func TestMatchesPatternValidator(t *testing.T) {
	validator := courier.MatchesPattern(
		func(e testEvent) bool { return len(e.ID) >= 3 },
		"ID must be at least 3 characters",
	)

	if err := validator(testEvent{ID: "abc"}); err != nil {
		t.Errorf("expected valid, got %v", err)
	}
	if err := validator(testEvent{ID: "ab"}); err == nil {
		t.Error("expected invalid for short ID")
	}
}

func TestValidationError(t *testing.T) {
	err := &courier.ValidationError{
		Record:  "test",
		Message: "field is required",
	}

	expected := "courier: validation failed: field is required"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}

	wrappedErr := errors.New("underlying error")
	err2 := &courier.ValidationError{
		Record:  "test",
		Message: "field is invalid",
		Err:     wrappedErr,
	}

	if !errors.Is(err2, wrappedErr) {
		t.Error("expected unwrap to return underlying error")
	}
}

func TestValidateMiddleware(t *testing.T) {
	middleware := courier.ValidateMiddleware[testEvent](false,
		courier.NotEmpty(func(e testEvent) string { return e.ID }, "ID"),
	)

	applier := courier.ApplierFunc[testEvent](func(ctx context.Context, records []testEvent) ([]testEvent, []testEvent, error) {
		return records, nil, nil
	})

	wrapped := middleware(applier)
	synced, failed, err := wrapped.Apply(context.Background(), []testEvent{{ID: "1"}, {ID: ""}})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 1 {
		t.Errorf("expected 1 synced, got %d", len(synced))
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed, got %d", len(failed))
	}
}
