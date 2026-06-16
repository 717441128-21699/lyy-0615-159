package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"time"

	"loadbalancer/balancer"
	"loadbalancer/backend"
)

type ReverseProxy struct {
	balancer      balancer.LoadBalancer
	maxRetries    int
	retryOnStatus []int
	backoff       time.Duration
}

func NewReverseProxy(lb balancer.LoadBalancer, maxRetries int, retryOnStatus []int, backoff time.Duration) *ReverseProxy {
	return &ReverseProxy{
		balancer:      lb,
		maxRetries:    maxRetries,
		retryOnStatus: retryOnStatus,
		backoff:       backoff,
	}
}

func (rp *ReverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var lastErr error
	var lastResp *http.Response
	attempts := rp.maxRetries + 1

	if r.Body != nil {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(rp.backoff * time.Duration(i))
			if r.Body != nil {
				bodyBytes, _ := io.ReadAll(r.Body)
				r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			}
		}

		backend, err := rp.balancer.Next(r)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := rp.forwardRequest(r, backend)
		if err != nil {
			lastErr = err
			backend.RecordFailure(err)
			continue
		}

		if rp.shouldRetry(resp.StatusCode) && i < attempts-1 {
			resp.Body.Close()
			lastResp = resp
			continue
		}

		rp.copyResponse(w, resp)
		resp.Body.Close()
		return
	}

	if lastResp != nil {
		rp.copyResponse(w, lastResp)
		lastResp.Body.Close()
		return
	}

	if lastErr != nil {
		http.Error(w, fmt.Sprintf("Service Unavailable: %v", lastErr), http.StatusServiceUnavailable)
	} else {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
	}
}

func (rp *ReverseProxy) forwardRequest(r *http.Request, b *backend.Backend) (*http.Response, error) {
	b.IncConn()
	defer b.DecConn()

	targetURL := *b.URL
	targetURL.Path = r.URL.Path
	targetURL.RawPath = r.URL.RawPath
	targetURL.RawQuery = r.URL.RawQuery

	outReq := r.Clone(r.Context())
	outReq.URL = &targetURL
	outReq.Host = b.URL.Host
	outReq.Header = make(http.Header)
	for k, v := range r.Header {
		outReq.Header[k] = v
	}

	if clientIP := getRealIP(r); clientIP != "" {
		if existing := outReq.Header.Get("X-Forwarded-For"); existing != "" {
			outReq.Header.Set("X-Forwarded-For", existing+", "+clientIP)
		} else {
			outReq.Header.Set("X-Forwarded-For", clientIP)
		}
	}

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	return client.Do(outReq)
}

func (rp *ReverseProxy) shouldRetry(statusCode int) bool {
	for _, code := range rp.retryOnStatus {
		if statusCode == code {
			return true
		}
	}
	return false
}

func (rp *ReverseProxy) copyResponse(w http.ResponseWriter, resp *http.Response) {
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func getRealIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := splitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func splitHostPort(addr string) (string, string, error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid address: %s", addr)
}

type ProxyManager struct {
	rp *ReverseProxy
}

func NewProxyManager() *ProxyManager {
	return &ProxyManager{}
}

func (pm *ProxyManager) SetProxy(rp *ReverseProxy) {
	pm.rp = rp
}

func (pm *ProxyManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if pm.rp == nil {
		http.Error(w, "proxy not initialized", http.StatusInternalServerError)
		return
	}
	pm.rp.ServeHTTP(w, r)
}

var _ = httputil.ReverseProxy{}
