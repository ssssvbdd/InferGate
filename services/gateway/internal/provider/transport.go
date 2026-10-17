package provider

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const maxUpstreamResponseBytes = 32 << 20

var sharedTransports sync.Map // map[bool]*http.Transport; separate internal and public policies

// safeTransport 是一个防 SSRF 的 http.Transport 包装。
// 默认拒绝解析到内网/回环地址的连接，除非 allowInternal 为 true。
func safeTransport(allowInternal bool) *http.Transport {
	if existing, ok := sharedTransports.Load(allowInternal); ok {
		return existing.(*http.Transport)
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	created := &http.Transport{
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		MaxConnsPerHost:       64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if !allowInternal {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				if isBlockedHost(host) {
					return nil, fmt.Errorf("connection to internal address %q is blocked", host)
				}
				if ip := net.ParseIP(host); ip != nil {
					return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				}
				ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, err
				}
				if len(ips) == 0 {
					return nil, fmt.Errorf("host %q has no DNS addresses", host)
				}
				for _, ip := range ips {
					if isBlockedIP(ip.IP) {
						return nil, fmt.Errorf("resolved internal ip %s is blocked", ip.IP)
					}
				}
				// Dial the validated address directly so DNS cannot change between
				// the security check and the actual connection.
				return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}
	actual, loaded := sharedTransports.LoadOrStore(allowInternal, created)
	if loaded {
		created.CloseIdleConnections()
	}
	return actual.(*http.Transport)
}

func isBlockedHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "localhost" || strings.HasSuffix(h, ".local") || strings.HasSuffix(h, ".internal") {
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		return isBlockedIP(ip)
	}
	return false
}

func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	// 额外拦截安全规则要求的网段前缀。
	if v4 := ip.To4(); v4 != nil {
		switch v4[0] {
		case 9, 10, 11, 21, 30:
			return true
		}
	}
	return false
}

// httpClient 构造带超时与安全传输的 HTTP 客户端。
func httpClient(timeout time.Duration, allowInternal bool) *http.Client {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &http.Client{Timeout: timeout, Transport: safeTransport(allowInternal)}
}

// A stream can legitimately outlive the non-stream request timeout. Dial and
// response-header deadlines remain on the shared transport; doStream enforces
// an idle deadline between incoming SSE lines.
func streamHTTPClient(allowInternal bool) *http.Client {
	return &http.Client{Transport: safeTransport(allowInternal)}
}

// doJSON 发送 JSON 请求并读取完整响应。
func doJSON(ctx context.Context, cli *http.Client, method, url string, headers map[string]string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, gwerr.ErrInternal.WithCause(err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return 0, nil, classifyTransportError(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBytes+1))
	if err != nil {
		return resp.StatusCode, nil, gwerr.ErrUpstream.WithCause(err)
	}
	if len(data) > maxUpstreamResponseBytes {
		return resp.StatusCode, nil, gwerr.ErrUpstream.WithCause(errors.New("upstream response exceeds 32 MiB"))
	}
	return resp.StatusCode, data, nil
}

// doStream 发送流式请求，按 SSE 逐行解析，通过 out 推送。
func doStream(ctx context.Context, cli *http.Client, url string, headers map[string]string, body []byte, out chan<- rawChunk, requireDone bool, idleTimeout time.Duration) error {
	if idleTimeout <= 0 {
		idleTimeout = 90 * time.Second
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var idleExpired atomic.Bool
	timer := time.AfterFunc(idleTimeout, func() { idleExpired.Store(true); cancel() })
	defer timer.Stop()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return gwerr.ErrInternal.WithCause(err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cli.Do(req)
	if err != nil {
		if idleExpired.Load() {
			return gwerr.ErrTimeout.WithCause(errors.New("upstream stream idle timeout"))
		}
		return classifyTransportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return gwerr.NewRequestError(resp.StatusCode, string(data), nil)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	for scanner.Scan() {
		timer.Reset(idleTimeout)
		trimmed := bytes.TrimSpace(scanner.Bytes())
		if bytes.HasPrefix(trimmed, []byte("data:")) {
			payload := bytes.TrimSpace(trimmed[len("data:"):])
			if bytes.Equal(payload, []byte("[DONE]")) {
				if err := sendRaw(ctx, out, rawChunk{done: true}); err != nil {
					return err
				}
				return nil
			}
			if err := sendRaw(ctx, out, rawChunk{data: append([]byte(nil), payload...)}); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return gwerr.ErrTimeout.WithCause(ctx.Err())
		default:
		}
	}
	if err := scanner.Err(); err != nil {
		if idleExpired.Load() {
			return gwerr.ErrTimeout.WithCause(errors.New("upstream stream idle timeout"))
		}
		return gwerr.ErrUpstream.WithCause(err)
	}
	if requireDone {
		return gwerr.ErrUpstream.WithCause(errors.New("SSE stream ended without [DONE]"))
	}
	return sendRaw(ctx, out, rawChunk{done: true})
}

func sendRaw(ctx context.Context, out chan<- rawChunk, chunk rawChunk) error {
	select {
	case out <- chunk:
		return nil
	case <-ctx.Done():
		return gwerr.ErrTimeout.WithCause(ctx.Err())
	}
}

// classifyTransportError 将传输层错误归类为网关错误。
func classifyTransportError(err error) error {
	if err == nil {
		return nil
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return gwerr.ErrTimeout.WithCause(err)
	}
	return gwerr.ErrUpstream.WithCause(err)
}

// apiKeyFromEnv 从环境变量读取 API Key（密钥只从 env 读取）。
func apiKeyFromEnv(envName string) string {
	if envName == "" {
		return ""
	}
	return os.Getenv(envName)
}
