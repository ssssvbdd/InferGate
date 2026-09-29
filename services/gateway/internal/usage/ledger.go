// Package usage writes an append-only audit ledger without storing prompts.
package usage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	Timestamp        time.Time `json:"timestamp"`
	RequestID        string    `json:"request_id"`
	Tenant           string    `json:"tenant"`
	Route            string    `json:"route"`
	Model            string    `json:"model"`
	Status           int       `json:"status"`
	Stream           bool      `json:"stream"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	Metered          bool      `json:"metered"`
}

// Ledger is a single-writer append-only JSONL file. It is suitable for the
// current single-host deployment and can later be replaced by a remote sink.
type Ledger struct {
	mu   sync.Mutex
	file *os.File
}

func Open(path string) (*Ledger, error) {
	if path == "" {
		return nil, errors.New("usage ledger path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &Ledger{file: file}, nil
}

func (l *Ledger) Record(event Event) error {
	if l == nil {
		return nil
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.file.Write(body)
	return err
}

func (l *Ledger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Sync(); err != nil {
		_ = l.file.Close()
		return err
	}
	return l.file.Close()
}
