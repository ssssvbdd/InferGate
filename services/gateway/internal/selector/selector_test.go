package selector

import (
	"context"
	"testing"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

func TestWeightedSelector_Distribution(t *testing.T) {
	s := newWeightedSelector()
	eps := []model.Endpoint{
		{ID: "a", Weight: 3},
		{ID: "b", Weight: 1},
	}
	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		ep, err := s.Select(context.Background(), "m", eps)
		if err != nil {
			t.Fatal(err)
		}
		counts[ep.ID]++
	}
	// a 权重是 b 的 3 倍，应大致 3:1。
	if counts["a"] <= counts["b"] {
		t.Errorf("expected a > b, got a=%d b=%d", counts["a"], counts["b"])
	}
	ratio := float64(counts["a"]) / float64(counts["b"])
	if ratio < 2.5 || ratio > 3.5 {
		t.Errorf("expected ratio ~3, got %.2f", ratio)
	}
}

func TestRoundRobinSelector(t *testing.T) {
	s := newRoundRobinSelector()
	eps := []model.Endpoint{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	seen := []string{}
	for i := 0; i < 3; i++ {
		ep, _ := s.Select(context.Background(), "m", eps)
		seen = append(seen, ep.ID)
	}
	if seen[0] == seen[1] || seen[1] == seen[2] {
		t.Errorf("round robin should rotate, got %v", seen)
	}
}
