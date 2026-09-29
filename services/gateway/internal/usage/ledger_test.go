package usage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLedgerStoresMetadataWithoutPrompt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.jsonl")
	ledger, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Record(Event{Tenant: "team-a", RequestID: "r1", PromptTokens: 10, Metered: true}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "secret text") {
		t.Fatal("prompt content leaked")
	}
	var event Event
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatal(err)
	}
	if event.Tenant != "team-a" || event.PromptTokens != 10 {
		t.Fatalf("bad event: %#v", event)
	}
}
