package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestAuthenticationLimitsAndTenantSalt(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	secret := filepath.Join(dir, "salt.key")
	digest := sha256.Sum256([]byte("test-key"))
	body, _ := json.Marshal(keyFile{Keys: []Key{{Tenant: "team-a", SHA256: hex.EncodeToString(digest[:]), RPM: 2, Concurrency: 1}}})
	if err := os.WriteFile(keys, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte("01234567890123456789012345678901"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := Load(keys, secret, 1)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := m.Authenticate("test-key")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Tenant != "team-a" || identity.CacheSalt == "" {
		t.Fatalf("bad identity: %#v", identity)
	}
	if _, err := m.Acquire(identity); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Acquire(identity); err != ErrBusy {
		t.Fatalf("expected busy, got %v", err)
	}
	m.Release(identity)
	if _, err := m.Authenticate("wrong"); err != ErrUnauthorized {
		t.Fatalf("expected unauthorized, got %v", err)
	}
}
