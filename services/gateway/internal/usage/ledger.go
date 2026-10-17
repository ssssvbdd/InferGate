// Package usage writes an append-only audit ledger without storing prompts.
package usage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Event struct {
	EventID          string    `json:"event_id"`
	Timestamp        time.Time `json:"timestamp"`
	RequestID        string    `json:"request_id"`
	Tenant           string    `json:"tenant"`
	Route            string    `json:"route"`
	Model            string    `json:"model"`
	Status           int       `json:"status"`
	Outcome          string    `json:"outcome"`
	Stream           bool      `json:"stream"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	Metered          bool      `json:"metered"`
	DurationMS       int64     `json:"duration_ms"`
}

// Ledger is a single-writer append-only JSONL file. It is suitable for the
// current single-host deployment and can later be replaced by a remote sink.
type Ledger struct {
	mu         sync.Mutex
	deliveryMu sync.Mutex
	file       *os.File
	path       string
	redis      *redis.Client
	stream     string
	offset     int64
	wake       chan struct{}
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
	return &Ledger{file: file, path: path, wake: make(chan struct{}, 1)}, nil
}

func (l *Ledger) Record(event Event) error {
	if l == nil {
		return nil
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.EventID == "" {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return fmt.Errorf("generate usage event id: %w", err)
		}
		event.EventID = hex.EncodeToString(id[:])
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err = l.file.Write(body); err != nil {
		return err
	}
	if err = l.file.Sync(); err != nil {
		return err
	}
	if l.redis != nil {
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// EnableRedis configures an at-least-once outbox. Redis stream entries carry a
// stable event_id so downstream accounting can deduplicate independently.
func (l *Ledger) EnableRedis(client *redis.Client, stream string) error {
	if client == nil || stream == "" {
		return errors.New("central usage requires a Redis client and stream key")
	}
	if _, err := os.Stat(l.path + ".offset"); err == nil {
		raw, err := os.ReadFile(l.path + ".offset")
		if err != nil {
			return err
		}
		if l.offset, err = strconv.ParseInt(string(bytes.TrimSpace(raw)), 10, 64); err != nil || l.offset < 0 {
			return errors.New("invalid usage delivery offset")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	l.redis, l.stream = client, stream
	return nil
}

var appendEvent = redis.NewScript(`
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then return 0 end
redis.call('XADD', KEYS[1], '*', 'event_id', ARGV[1], 'event', ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], '1')
redis.call('EXPIRE', KEYS[2], 7776000)
return 1
`)

// DeliverPending advances a durable local checkpoint only after Redis accepts
// each event. It may be called on startup and by a background worker.
func (l *Ledger) DeliverPending(ctx context.Context) (int, error) {
	l.deliveryMu.Lock()
	defer l.deliveryMu.Unlock()
	if l.redis == nil {
		return 0, nil
	}
	l.mu.Lock()
	start := l.offset
	stat, statErr := l.file.Stat()
	l.mu.Unlock()
	if statErr != nil {
		return 0, statErr
	}
	end := stat.Size()
	reader, err := os.Open(l.path)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	if _, err := reader.Seek(start, io.SeekStart); err != nil {
		return 0, err
	}
	scan := bufio.NewReader(reader)
	delivered := 0
	position := start
	for position < end {
		line, err := scan.ReadBytes('\n')
		if err != nil {
			return delivered, err
		}
		if position+int64(len(line)) > end {
			return delivered, errors.New("usage journal changed during delivery")
		}
		var event Event
		if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
			return delivered, err
		}
		id := event.EventID
		if id == "" {
			legacy := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", l.path, position, line)))
			id = hex.EncodeToString(legacy[:])
		}
		seenKey := l.stream + ":seen:" + event.Timestamp.UTC().Format("20060102")
		if err := appendEvent.Run(ctx, l.redis, []string{l.stream, seenKey}, id, string(bytes.TrimSpace(line))).Err(); err != nil {
			return delivered, err
		}
		position += int64(len(line))
		checkpoint, err := os.OpenFile(l.path+".offset.tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			return delivered, err
		}
		_, err = fmt.Fprintf(checkpoint, "%d\n", position)
		if err == nil {
			err = checkpoint.Sync()
		}
		closeErr := checkpoint.Close()
		if err != nil {
			return delivered, err
		}
		if closeErr != nil {
			return delivered, closeErr
		}
		if err := os.Rename(l.path+".offset.tmp", l.path+".offset"); err != nil {
			return delivered, err
		}
		l.mu.Lock()
		l.offset = position
		l.mu.Unlock()
		delivered++
	}
	return delivered, nil
}

func (l *Ledger) PendingBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.redis == nil {
		return 0
	}
	stat, err := l.file.Stat()
	if err != nil {
		return 0
	}
	return max(0, stat.Size()-l.offset)
}

// RunDelivery retries the local outbox without blocking online inference.
func (l *Ledger) RunDelivery(ctx context.Context, onFailure func(error)) {
	if l.redis == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		deliveryCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		_, err := l.DeliverPending(deliveryCtx)
		cancel()
		if err != nil && ctx.Err() == nil && onFailure != nil {
			onFailure(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-l.wake:
		}
	}
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
