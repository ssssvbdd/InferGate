// Package auth authenticates callers and enforces trusted tenant limits.
package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"
)

var tenantPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,48}$`)

var (
	ErrUnauthorized = errors.New("invalid API key")
	ErrRateLimited  = errors.New("tenant request limit reached")
	ErrBusy         = errors.New("tenant concurrency limit reached")
	ErrGlobalBusy   = errors.New("gateway concurrency limit reached")
)

// Key is a hashed caller credential. Plaintext keys never enter configuration.
type Key struct {
	Tenant      string `json:"tenant"`
	SHA256      string `json:"sha256"`
	RPM         int    `json:"rpm"`
	Concurrency int    `json:"concurrency"`
	digest      [sha256.Size]byte
}

type keyFile struct {
	Keys []Key `json:"keys"`
}

// Identity contains only trusted values derived from an authenticated key.
type Identity struct {
	Tenant    string
	CacheSalt string
	key       *Key
}

// Manager stores bounded, in-process tenant limit state. Redis based global
// limits can still be applied by the routing layer when the gateway is scaled.
type Manager struct {
	keys        []Key
	saltSecret  []byte
	maxInFlight int

	mu          sync.Mutex
	arrivals    map[string][]time.Time
	active      map[string]int
	totalActive int
	now         func() time.Time
}

func Load(keysPath, saltPath string, maxInFlight int) (*Manager, error) {
	if keysPath == "" || saltPath == "" {
		return nil, errors.New("auth keys_file and cache_salt_secret_file are required")
	}
	raw, err := os.ReadFile(keysPath)
	if err != nil {
		return nil, fmt.Errorf("read auth keys: %w", err)
	}
	var file keyFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse auth keys: %w", err)
	}
	secret, err := os.ReadFile(saltPath)
	if err != nil {
		return nil, fmt.Errorf("read cache salt secret: %w", err)
	}
	secret = bytes.TrimSpace(secret)
	if len(secret) < 32 {
		return nil, errors.New("cache salt secret must contain at least 32 bytes")
	}
	if maxInFlight < 1 {
		return nil, errors.New("auth max_in_flight must be positive")
	}
	seen := make(map[string]struct{}, len(file.Keys))
	for i := range file.Keys {
		key := &file.Keys[i]
		if !tenantPattern.MatchString(key.Tenant) || key.RPM < 1 || key.Concurrency < 1 {
			return nil, fmt.Errorf("invalid auth key entry %d", i)
		}
		decoded, err := hex.DecodeString(key.SHA256)
		if err != nil || len(decoded) != sha256.Size || key.SHA256 != hex.EncodeToString(decoded) {
			return nil, fmt.Errorf("invalid sha256 in auth key entry %d", i)
		}
		if _, duplicate := seen[key.SHA256]; duplicate {
			return nil, errors.New("duplicate auth key hash")
		}
		seen[key.SHA256] = struct{}{}
		copy(key.digest[:], decoded)
	}
	if len(file.Keys) == 0 {
		return nil, errors.New("at least one auth key is required")
	}
	return &Manager{
		keys: file.Keys, saltSecret: append([]byte(nil), secret...), maxInFlight: maxInFlight,
		arrivals: make(map[string][]time.Time), active: make(map[string]int), now: time.Now,
	}, nil
}

func (m *Manager) Authenticate(token string) (*Identity, error) {
	if len(token) == 0 || len(token) > 256 {
		return nil, ErrUnauthorized
	}
	digest := sha256.Sum256([]byte(token))
	var selected *Key
	for i := range m.keys {
		if subtle.ConstantTimeCompare(digest[:], m.keys[i].digest[:]) == 1 {
			selected = &m.keys[i]
		}
	}
	if selected == nil {
		return nil, ErrUnauthorized
	}
	mac := hmac.New(sha256.New, m.saltSecret)
	_, _ = mac.Write([]byte(selected.Tenant))
	salt := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return &Identity{Tenant: selected.Tenant, CacheSalt: salt, key: selected}, nil
}

// Acquire enforces tenant RPM, tenant concurrency and global concurrency.
func (m *Manager) Acquire(identity *Identity) (time.Duration, error) {
	if identity == nil || identity.key == nil {
		return 0, ErrUnauthorized
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	cutoff := now.Add(-time.Minute)
	arrivals := m.arrivals[identity.Tenant]
	first := 0
	for first < len(arrivals) && !arrivals[first].After(cutoff) {
		first++
	}
	arrivals = arrivals[first:]
	if len(arrivals) >= identity.key.RPM {
		retry := time.Minute - now.Sub(arrivals[0])
		if retry < time.Second {
			retry = time.Second
		}
		m.arrivals[identity.Tenant] = arrivals
		return retry, ErrRateLimited
	}
	if m.active[identity.Tenant] >= identity.key.Concurrency {
		return time.Second, ErrBusy
	}
	if m.totalActive >= m.maxInFlight {
		return time.Second, ErrGlobalBusy
	}
	m.arrivals[identity.Tenant] = append(arrivals, now)
	m.active[identity.Tenant]++
	m.totalActive++
	return 0, nil
}

func (m *Manager) Release(identity *Identity) {
	if identity == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active[identity.Tenant] > 0 {
		m.active[identity.Tenant]--
		m.totalActive--
	}
}

func (m *Manager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalActive
}
