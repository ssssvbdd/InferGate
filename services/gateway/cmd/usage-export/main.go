// Command usage-export writes a bounded Redis Stream slice as privacy-safe JSONL.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	stream := flag.String("stream", "infergate:usage:{platform}:events", "usage stream key")
	output := flag.String("output", "", "new JSONL output file")
	start := flag.String("start", "-", "exclusive Redis Stream ID cursor, or - for the beginning")
	limit := flag.Int("limit", 100000, "maximum records to export")
	passwordFile := flag.String("password-file", "", "file containing Redis password")
	useTLS := flag.Bool("tls", false, "connect to Redis with TLS")
	caFile := flag.String("tls-ca-file", "", "additional trusted Redis CA PEM")
	serverName := flag.String("tls-server-name", "", "Redis TLS certificate hostname")
	flag.Parse()
	if *output == "" || *limit < 1 {
		fmt.Fprintln(os.Stderr, "output and positive limit are required")
		os.Exit(2)
	}
	password := ""
	if *passwordFile != "" {
		body, err := os.ReadFile(*passwordFile)
		if err != nil {
			fail(err)
		}
		password = strings.TrimSpace(string(body))
	}
	var tlsConfig *tls.Config
	if *useTLS {
		host, _, err := net.SplitHostPort(*addr)
		if err != nil {
			fail(err)
		}
		if *serverName != "" {
			host = *serverName
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if *caFile != "" {
			body, err := os.ReadFile(*caFile)
			if err != nil {
				fail(err)
			}
			if !roots.AppendCertsFromPEM(body) {
				fail(fmt.Errorf("TLS CA file has no certificates"))
			}
		}
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host, RootCAs: roots}
	} else if *caFile != "" || *serverName != "" {
		fail(fmt.Errorf("-tls is required with TLS options"))
	}
	client := redis.NewClient(&redis.Options{Addr: *addr, Password: password, TLSConfig: tlsConfig})
	defer client.Close()
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fail(err)
	}
	defer file.Close()
	cursor := *start
	written := 0
	for written < *limit {
		batch := int64(min(1000, *limit-written))
		queryStart := cursor
		if queryStart != "-" {
			queryStart = "(" + queryStart
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		messages, err := client.XRangeN(ctx, *stream, queryStart, "+", batch).Result()
		cancel()
		if err != nil {
			fail(err)
		}
		if len(messages) == 0 {
			break
		}
		for _, message := range messages {
			body, ok := message.Values["event"].(string)
			if !ok || !json.Valid([]byte(body)) {
				fail(fmt.Errorf("invalid event at %s", message.ID))
			}
			if _, err := file.WriteString(body + "\n"); err != nil {
				fail(err)
			}
			cursor = message.ID
			written++
		}
	}
	if err := file.Sync(); err != nil {
		fail(err)
	}
	fmt.Printf("exported=%d next_start=%s\n", written, cursor)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
