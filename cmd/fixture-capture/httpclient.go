package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/proxy"
)

// httpClient wraps http.Client with TRON-specific defaults: optional SOCKS5,
// 30s timeout, connection reuse via a single Transport (cheap socks5 calls
// share TCP). Methods are POST-only — every wallet/* path is POST in
// java-tron's HTTP API.
type httpClient struct {
	base   string
	cli    *http.Client
}

func newHTTPClient(baseURL, socks5Addr string) (*httpClient, error) {
	tr := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	if socks5Addr != "" {
		dialer, err := proxy.SOCKS5("tcp", socks5Addr, nil, proxy.Direct)
		if err != nil {
			return nil, fmt.Errorf("socks5 dialer: %w", err)
		}
		ctxDialer, ok := dialer.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("socks5 dialer is not ContextDialer")
		}
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return ctxDialer.DialContext(ctx, network, addr)
		}
	}
	return &httpClient{
		base: baseURL,
		cli: &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		},
	}, nil
}

// post executes a POST against base+path with body, returning the response
// bytes. Non-2xx and any logical "Error" field surface as errors.
func (c *httpClient) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.cli.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(data, 200))
	}
	if bytes.Contains(data, []byte(`"Error"`)) {
		return nil, fmt.Errorf("logical error: %s", truncate(data, 200))
	}
	return data, nil
}

// postRetry calls post with up to N attempts, doubling the backoff.
func (c *httpClient) postRetry(ctx context.Context, path string, body []byte, attempts int) ([]byte, error) {
	var lastErr error
	backoff := 200 * time.Millisecond
	for i := 0; i < attempts; i++ {
		data, err := c.post(ctx, path, body)
		if err == nil {
			return data, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
	return nil, fmt.Errorf("after %d attempts: %w", attempts, lastErr)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
