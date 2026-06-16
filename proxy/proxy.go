package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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

type attemptError struct {
	Backend string `json:"backend"`
	Error   string `json:"error"`
}

func (rp *ReverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var bodyBytes []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		bodyBytes = b
	}

	var (
		lastValidResp *http.Response
		attemptErrs   []attemptError
		attempts      = rp.maxRetries + 1
		failedBackends = make(map[string]bool)
	)

	defer func() {
		if lastValidResp != nil {
			lastValidResp.Body.Close()
		}
	}()

	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(rp.backoff * time.Duration(i))
		}

		if bodyBytes != nil {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			r.ContentLength = int64(len(bodyBytes))
		} else {
			r.Body = http.NoBody
			r.ContentLength = 0
		}

		exclude := failedBackends
		if len(exclude) > 0 && i == attempts-1 {
			exclude = nil
		}

		be, err := rp.balancer.Next(r, exclude)
		if err != nil {
			attemptErrs = append(attemptErrs, attemptError{
				Backend: "balancer",
				Error:   err.Error(),
			})
			continue
		}

		start := time.Now()
		resp, fwdErr := rp.forwardRequest(r, be)
		latency := time.Since(start)

		if fwdErr != nil {
			be.RecordRequestFailure(fwdErr, latency)
			failedBackends[be.Name] = true
			attemptErrs = append(attemptErrs, attemptError{
				Backend: be.Name,
				Error:   fwdErr.Error(),
			})
			continue
		}

		if rp.shouldRetry(resp.StatusCode) && i < attempts-1 {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			be.RecordRequestFailure(fmt.Errorf("status %d", resp.StatusCode), latency)
			failedBackends[be.Name] = true
			attemptErrs = append(attemptErrs, attemptError{
				Backend: be.Name,
				Error:   fmt.Sprintf("status %d (retried)", resp.StatusCode),
			})
			continue
		}

		be.RecordRequestSuccess(latency)

		if lastValidResp != nil {
			lastValidResp.Body.Close()
		}
		lastValidResp = resp
		break
	}

	if lastValidResp != nil {
		rp.copyResponse(w, lastValidResp)
		return
	}

	rp.writeServiceUnavailable(w, attemptErrs)
}

func (rp *ReverseProxy) writeServiceUnavailable(w http.ResponseWriter, errs []attemptError) {
	msgParts := []string{"Service Unavailable"}
	if len(errs) > 0 {
		for _, e := range errs {
			msgParts = append(msgParts, fmt.Sprintf("[%s] %s", e.Backend, e.Error))
		}
	}

	accept := w.Header().Get("Accept")
	wantsJSON := strings.Contains(accept, "application/json")

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)

	payload := map[string]interface{}{
		"error":    "Service Unavailable",
		"code":     http.StatusServiceUnavailable,
		"attempts": errs,
		"message":  strings.Join(msgParts, "; "),
	}
	if !wantsJSON {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(strings.Join(msgParts, "\n")))
		w.Write([]byte("\n"))
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func (rp *ReverseProxy) forwardRequest(r *http.Request, b *backend.Backend) (*http.Response, error) {
	b.IncConn()
	defer b.DecConn()

	targetURL := *b.URL
	targetURL.Path = r.URL.Path
	targetURL.RawPath = r.URL.RawPath
	targetURL.RawQuery = r.URL.RawQuery
	targetURL.Opaque = ""
	targetURL.ForceQuery = false

	outReq := &http.Request{
		Method:        r.Method,
		URL:           &targetURL,
		Header:        make(http.Header),
		Body:          r.Body,
		ContentLength: r.ContentLength,
		Host:          b.URL.Host,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Close:         false,
	}
	if r.Trailer != nil {
		outReq.Trailer = make(http.Header)
		for k, v := range r.Trailer {
			outReq.Trailer[k] = v
		}
	}
	if len(r.TransferEncoding) > 0 {
		outReq.TransferEncoding = make([]string, len(r.TransferEncoding))
		copy(outReq.TransferEncoding, r.TransferEncoding)
	}
	if r.Form != nil {
		outReq.Form = r.Form
	}
	if r.PostForm != nil {
		outReq.PostForm = r.PostForm
	}
	if r.MultipartForm != nil {
		outReq.MultipartForm = r.MultipartForm
	}

	for k, v := range r.Header {
		outReq.Header[k] = v
	}
	outReq.Header.Del("Te")
	outReq.Header.Del("Trailer")
	outReq.Header.Del("Upgrade")
	outReq.Header.Del("Connection")

	if clientIP := getRealIP(r); clientIP != "" {
		if existing := outReq.Header.Get("X-Forwarded-For"); existing != "" {
			outReq.Header.Set("X-Forwarded-For", existing+", "+clientIP)
		} else {
			outReq.Header.Set("X-Forwarded-For", clientIP)
		}
	}
	outReq.Header.Set("X-Forwarded-Proto", schemeOf(r))
	if r.Host != "" {
		outReq.Header.Set("X-Forwarded-Host", r.Host)
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

func schemeOf(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if s := r.Header.Get("X-Forwarded-Proto"); s != "" {
		return s
	}
	return "http"
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
	dst := w.Header()
	for k, v := range resp.Header {
		dst[k] = v
	}
	dst.Del("Content-Length")
	dst.Del("Connection")
	w.WriteHeader(resp.StatusCode)
	buf := make([]byte, 32*1024)
	for {
		nr, er := resp.Body.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[0:nr])
			if ew != nil {
				return
			}
			if nr != nw {
				return
			}
		}
		if er != nil {
			return
		}
	}
}

func getRealIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
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
