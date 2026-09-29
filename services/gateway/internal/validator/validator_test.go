package validator

import (
	"testing"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

func TestValidate_ZeroValueNormalization(t *testing.T) {
	v := New()

	zero := 0
	zeroF := 0.0
	req := &model.Request{
		Model:     "gpt-4o",
		MaxTokens: &zero,
		TopP:      &zeroF,
	}
	if err := v.Validate(req); err != nil {
		t.Fatal(err)
	}
	if req.MaxTokens != nil {
		t.Error("MaxTokens=0 should be normalized to nil")
	}
	if req.TopP != nil {
		t.Error("TopP=0 should be normalized to nil")
	}
}

func TestValidate_MissingModel(t *testing.T) {
	v := New()
	if err := v.Validate(&model.Request{}); err == nil {
		t.Error("expected error for missing model")
	}
}

func TestValidate_TemperatureClamp(t *testing.T) {
	v := New()
	high := 5.0
	req := &model.Request{Model: "m", Temperature: &high}
	_ = v.Validate(req)
	if req.Temperature == nil || *req.Temperature != 2.0 {
		t.Errorf("expected temperature clamped to 2, got %v", req.Temperature)
	}
}
