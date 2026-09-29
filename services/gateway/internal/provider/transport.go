package provider

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

// safeTransport 是一个防 SSRF 的 http.Transport 包装。
// 默认拒绝解析到内网/回环地址的连接，除非 allowInternal 为 true。
func safeTransport(allowInternal bool) *http.Transport {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if !allowInternal {
				host, _, err := net.SplitHostPort(addr)
				if err != nil {
					host = addr
				}
				if isBlockedHost(host) {
					return nil, fmt.Errorf("connection to internal address %q is blocked", host)
				}
				ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, err
				}
				for _, ip := range ips {
					if isBlockedIP(ip.IP) {
						return nil, fmt.Errorf("resolved internal ip %s is blocked", ip.IP)
					}
				}
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}
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

// doJSON 发送 JSON 请求并读取完整响应。
func doJSON(ctx context.Context, cli *http.Client, method, url string, headers map[string]string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, gwerr.ErrInternal.WithCause(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return 0, nil, classifyTransportError(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, gwerr.ErrUpstream.WithCause(err)
	}
	return resp.StatusCode, data, nil
}

// doStream 发送流式请求，按 SSE 逐行解析，通过 out 推送。
func doStream(ctx context.Context, cli *http.Client, url string, headers map[string]string, body []byte, out chan<- rawChunk) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return gwerr.ErrInternal.WithCause(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return classifyTransportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return gwerr.NewRequestError(resp.StatusCode, string(data), nil)
	}

	reader := bufio.NewReaderSize(resp.Body, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if bytes.HasPrefix(trimmed, []byte("data:")) {
				payload := bytes.TrimSpace(trimmed[len("data:"):])
				if bytes.Equal(payload, []byte("[DONE]")) {
					out <- rawChunk{done: true}
					return nil
				}
				out <- rawChunk{data: append([]byte(nil), payload...)}
			}
		}
		if err != nil {
			if err == io.EOF {
				out <- rawChunk{done: true}
				return nil
			}
			return gwerr.ErrUpstream.WithCause(err)
		}
		select {
		case <-ctx.Done():
			return gwerr.ErrTimeout.WithCause(ctx.Err())
		default:
		}
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
