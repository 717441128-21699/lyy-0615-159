package backend

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type BackendStatus int

const (
	StatusHealthy BackendStatus = iota
	StatusUnhealthy
	StatusUnknown
)

type Backend struct {
	Name    string
	URL     *url.URL
	Weight  int

	mu              sync.RWMutex
	status          BackendStatus
	activeConns     int64
	failCount       int
	successCount    int
	failureThreshold int
	successThreshold int

	lastCheck time.Time
	lastError string
}

func NewBackend(name, rawURL string, weight, failureThreshold, successThreshold int) (*Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse backend URL: %w", err)
	}
	return &Backend{
		Name:              name,
		URL:               u,
		Weight:            weight,
		status:            StatusUnknown,
		failureThreshold:  failureThreshold,
		successThreshold:  successThreshold,
	}, nil
}

func (b *Backend) Status() BackendStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func (b *Backend) IsHealthy() bool {
	return b.Status() == StatusHealthy
}

func (b *Backend) ActiveConns() int64 {
	return atomic.LoadInt64(&b.activeConns)
}

func (b *Backend) IncConn() {
	atomic.AddInt64(&b.activeConns, 1)
}

func (b *Backend) DecConn() {
	atomic.AddInt64(&b.activeConns, -1)
}

func (b *Backend) FailCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.failCount
}

func (b *Backend) SuccessCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.successCount
}

func (b *Backend) LastError() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastError
}

func (b *Backend) LastCheck() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastCheck
}

func (b *Backend) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastCheck = time.Now()
	b.failCount = 0
	b.successCount++

	if b.status != StatusHealthy && b.successCount >= b.successThreshold {
		b.status = StatusHealthy
		b.lastError = ""
		fmt.Printf("Backend %s is now healthy (consecutive successes: %d)\n", b.Name, b.successCount)
	}
}

func (b *Backend) RecordFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastCheck = time.Now()
	b.lastError = err.Error()
	b.successCount = 0
	b.failCount++

	if b.status != StatusUnhealthy && b.failCount >= b.failureThreshold {
		b.status = StatusUnhealthy
		fmt.Printf("Backend %s is now unhealthy (consecutive failures: %d): %v\n", b.Name, b.failCount, err)
	}
}

func (b *Backend) SetUnhealthy(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = StatusUnhealthy
	b.lastError = err.Error()
	b.failCount = b.failureThreshold
	b.successCount = 0
}

func (b *Backend) UpdateConfig(rawURL string, weight, failureThreshold, successThreshold int) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse backend URL: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	oldURL := b.URL.String()
	oldWeight := b.Weight
	oldFT := b.failureThreshold
	oldST := b.successThreshold

	b.URL = u
	b.Weight = weight
	b.failureThreshold = failureThreshold
	b.successThreshold = successThreshold

	changed := false
	if oldURL != rawURL {
		fmt.Printf("Backend %s URL updated: %s -> %s\n", b.Name, oldURL, rawURL)
		changed = true
	}
	if oldWeight != weight {
		fmt.Printf("Backend %s weight updated: %d -> %d\n", b.Name, oldWeight, weight)
		changed = true
	}
	if oldFT != failureThreshold {
		fmt.Printf("Backend %s failure_threshold updated: %d -> %d\n", b.Name, oldFT, failureThreshold)
		changed = true
	}
	if oldST != successThreshold {
		fmt.Printf("Backend %s success_threshold updated: %d -> %d\n", b.Name, oldST, successThreshold)
		changed = true
	}
	if !changed {
		fmt.Printf("Backend %s config unchanged\n", b.Name)
	}
	return nil
}

type HealthChecker struct {
	backends  []*Backend
	path      string
	interval  time.Duration
	timeout   time.Duration
	client    *http.Client
	stopCh    chan struct{}
	running   bool
	mu        sync.Mutex
}

func NewHealthChecker(path string, interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{
		path:     path,
		interval: interval,
		timeout:  timeout,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 1,
				DisableKeepAlives:   true,
			},
		},
		stopCh: make(chan struct{}),
	}
}

func (hc *HealthChecker) SetBackends(backends []*Backend) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.backends = backends
}

func (hc *HealthChecker) Start() {
	hc.mu.Lock()
	if hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = true
	hc.stopCh = make(chan struct{})
	hc.mu.Unlock()

	go hc.run()
}

func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if !hc.running {
		return
	}
	hc.running = false
	close(hc.stopCh)
}

func (hc *HealthChecker) run() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	hc.checkAll()

	for {
		select {
		case <-ticker.C:
			hc.checkAll()
		case <-hc.stopCh:
			return
		}
	}
}

func (hc *HealthChecker) checkAll() {
	hc.mu.Lock()
	backends := make([]*Backend, len(hc.backends))
	copy(backends, hc.backends)
	hc.mu.Unlock()

	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func(backend *Backend) {
			defer wg.Done()
			hc.check(backend)
		}(b)
	}
	wg.Wait()
}

func (hc *HealthChecker) check(b *Backend) {
	checkURL := fmt.Sprintf("%s://%s%s", b.URL.Scheme, b.URL.Host, hc.path)

	resp, err := hc.client.Get(checkURL)
	if err != nil {
		b.RecordFailure(fmt.Errorf("health check failed: %w", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		b.RecordSuccess()
	} else {
		b.RecordFailure(fmt.Errorf("health check returned status %d", resp.StatusCode))
	}
}

type BackendPool struct {
	mu        sync.RWMutex
	backends  []*Backend
	checker   *HealthChecker
}

func NewBackendPool() *BackendPool {
	return &BackendPool{
		backends: make([]*Backend, 0),
	}
}

func (bp *BackendPool) SetHealthChecker(hc *HealthChecker) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.checker = hc
}

func (bp *BackendPool) Backends() []*Backend {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	result := make([]*Backend, len(bp.backends))
	copy(result, bp.backends)
	return result
}

func (bp *BackendPool) HealthyBackends() []*Backend {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	healthy := make([]*Backend, 0)
	for _, b := range bp.backends {
		if b.IsHealthy() {
			healthy = append(healthy, b)
		}
	}
	return healthy
}

func (bp *BackendPool) AddBackend(b *Backend) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.backends = append(bp.backends, b)
}

func (bp *BackendPool) RemoveBackend(name string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for i, b := range bp.backends {
		if b.Name == name {
			bp.backends = append(bp.backends[:i], bp.backends[i+1:]...)
			return
		}
	}
}

func (bp *BackendPool) GetBackend(name string) *Backend {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	for _, b := range bp.backends {
		if b.Name == name {
			return b
		}
	}
	return nil
}

func (bp *BackendPool) UpdateBackends(newBackends []*Backend) {
	bp.mu.Lock()
	oldBackends := bp.backends
	bp.backends = newBackends
	bp.mu.Unlock()

	if bp.checker != nil {
		bp.checker.SetBackends(newBackends)
	}

	oldNames := make(map[string]bool)
	for _, b := range oldBackends {
		oldNames[b.Name] = true
	}
	newNames := make(map[string]bool)
	for _, b := range newBackends {
		newNames[b.Name] = true
	}

	added := make([]string, 0)
	removed := make([]string, 0)
	for name := range newNames {
		if !oldNames[name] {
			added = append(added, name)
		}
	}
	for name := range oldNames {
		if !newNames[name] {
			removed = append(removed, name)
		}
	}

	if len(added) > 0 || len(removed) > 0 {
		fmt.Printf("Backend pool updated - added: %v, removed: %v\n", added, removed)
	}
}
