package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

func TestDNSUsesConfiguredPortAndInternalPolicy(t *testing.T) {
	d := NewManager(time.Second)
	eps, err := d.Discover(context.Background(), &model.ModelConfig{
		Discovery: model.DiscoveryDNS, DNSName: "localhost", DNSPort: 18001,
		DNSScheme: "http", DNSAllowInternal: true,
	})
	if err != nil || len(eps) == 0 {
		t.Fatalf("localhost DNS discovery failed: %v", err)
	}
	for _, ep := range eps {
		if ep.Port != 18001 || ep.Scheme != "http" || ep.Metadata["allow_internal"] != "true" {
			t.Fatalf("DNS endpoint template lost: %+v", ep)
		}
	}
}
