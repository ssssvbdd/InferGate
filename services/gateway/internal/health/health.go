// Package health checks dependencies used by the serving path.
package health

import (
	"context"
	"net/http"
	"time"
)

type Checker struct {
	urls       []string
	client     *http.Client
	dependency func(context.Context) bool
}

func New(url string, timeout time.Duration) *Checker {
	return NewAny([]string{url}, timeout)
}

// NewAny reports ready when at least one serving upstream is healthy.
func NewAny(urls []string, timeout time.Duration) *Checker {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Checker{urls: append([]string(nil), urls...), client: &http.Client{Timeout: timeout}}
}

func (c *Checker) WithDependency(check func(context.Context) bool) *Checker {
	c.dependency = check
	return c
}

func (c *Checker) Ready(ctx context.Context) bool {
	if c == nil || len(c.urls) == 0 {
		return false
	}
	if c.dependency != nil && !c.dependency(ctx) {
		return false
	}
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan bool, len(c.urls))
	for _, url := range c.urls {
		go func(url string) {
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
			if err != nil {
				results <- false
				return
			}
			resp, err := c.client.Do(req)
			if err != nil {
				results <- false
				return
			}
			defer resp.Body.Close()
			results <- resp.StatusCode >= 200 && resp.StatusCode < 300
		}(url)
	}
	for range c.urls {
		if <-results {
			return true
		}
	}
	return false
}
