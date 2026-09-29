// Package health checks dependencies used by the serving path.
package health

import (
	"context"
	"net/http"
	"time"
)

type Checker struct {
	url    string
	client *http.Client
}

func New(url string, timeout time.Duration) *Checker {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Checker{url: url, client: &http.Client{Timeout: timeout}}
}

func (c *Checker) Ready(ctx context.Context) bool {
	if c == nil || c.url == "" {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
