package router

import (
	"testing"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

func TestMatchCondition(t *testing.T) {
	tests := []struct {
		name string
		cond model.Condition
		meta map[string]any
		want bool
	}{
		{
			name: "gt matched",
			cond: model.Condition{"prompt_length": map[string]any{"$gt": 1000}},
			meta: map[string]any{"prompt_length": 2000},
			want: true,
		},
		{
			name: "gt not matched",
			cond: model.Condition{"prompt_length": map[string]any{"$gt": 1000}},
			meta: map[string]any{"prompt_length": 500},
			want: false,
		},
		{
			name: "implicit eq",
			cond: model.Condition{"user": "vip"},
			meta: map[string]any{"user": "vip"},
			want: true,
		},
		{
			name: "in list",
			cond: model.Condition{"model": map[string]any{"$in": []any{"a", "b"}}},
			meta: map[string]any{"model": "b"},
			want: true,
		},
		{
			name: "multi field AND",
			cond: model.Condition{
				"prompt_length": map[string]any{"$gte": 100},
				"stream":        true,
			},
			meta: map[string]any{"prompt_length": 100, "stream": true},
			want: true,
		},
		{
			name: "missing field",
			cond: model.Condition{"x": map[string]any{"$gt": 1}},
			meta: map[string]any{},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchCondition(tt.cond, tt.meta); got != tt.want {
				t.Errorf("matchCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConditionStrategy_Priority(t *testing.T) {
	s := NewConditionStrategy()
	state := &RoutingState{
		Metadata: map[string]any{"prompt_length": 5000},
		CurrentModels: []model.ModelConfig{
			{Name: "m", ProviderModel: "small", Priority: 1},
			{Name: "m", ProviderModel: "big", Priority: 10,
				Condition: model.Condition{"prompt_length": map[string]any{"$gt": 4000}}},
		},
	}
	cont, err := s.Apply(nil, state)
	if err != nil || !cont {
		t.Fatalf("Apply err=%v cont=%v", err, cont)
	}
	if len(state.CurrentModels) != 1 || state.CurrentModels[0].ProviderModel != "big" {
		t.Errorf("expected big model selected, got %+v", state.CurrentModels)
	}
}
