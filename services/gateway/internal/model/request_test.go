package model

import (
	"encoding/json"
	"testing"
)

func TestRequestPreservesUnknownFieldsAndProtectsCacheSalt(t *testing.T) {
	var req Request
	if err := json.Unmarshal([]byte(`{"model":"local-model","stream_options":{"include_usage":true},"guided_json":{"type":"object"},"cache_salt":"caller"}`), &req); err != nil {
		t.Fatal(err)
	}
	req.CacheSalt = "tenant-derived"
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if string(got["guided_json"]) != `{"type":"object"}` {
		t.Fatalf("unknown field lost: %s", body)
	}
	if string(got["cache_salt"]) != `"tenant-derived"` {
		t.Fatalf("caller cache salt was not replaced: %s", body)
	}
}
