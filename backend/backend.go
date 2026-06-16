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

type BackendStats struct {
	TotalRequests  int64
	TotalSuccesses int64
	TotalFailures  int64
	TotalLatencyNs int64

	RecentRequests  int64
	RecentSuccesses int64
	RecentFailures  int64
}

type Backend struct {
	Name    string
	URL     *url.URL
	Weight  int

	mu               sync.RWMutex
	status           BackendStatus
	maintenance      bool
	maintenanceSince time.Time
	activeConns      int64

	failCount        int
	successCount     int
	failureThreshold int
	successThreshold int

	lastCheck       time.Time
	lastHCError     string

	lastRequestTime  time.Time
	lastRequestError string

	statsMu            sync.Mutex
	totalRequests      int64
	totalSuccesses     int64
	totalFailures      int64
	totalLatencyNs     int64
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

func (b *Backend) IsMaintenance() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maintenance
}

func (b *Backend) IsEligible() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return !b.maintenance && b.status == StatusHealthy
}

func (b *Backend) MaintenanceSince() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maintenanceSince
}

func (b *Backend) SetMaintenance(on bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if on && !b.maintenance {
		b.maintenance = true
		b.maintenanceSince = time.Now()
		fmt.Printf("Backend %s placed into MAINTENANCE mode (drained)\n", b.Name)
	} else if !on && b.maintenance {
		b.maintenance = false
		b.maintenanceSince = time.Time{}
		fmt.Printf("Backend %s restored from MAINTENANCE mode\n", b.Name)
	}
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

func (b *Backend) LastHCError() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastHCError
}

func (b *Backend) LastError() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.lastRequestError != "" {
		return b.lastRequestError
	}
	return b.lastHCError
}

func (b *Backend) LastRequestTime() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastRequestTime
}

func (b *Backend) LastRequestError() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastRequestError
}

func (b *Backend) LastCheck() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastCheck
}

func (b *Backend) Stats() BackendStats {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return BackendStats{
		TotalRequests:  b.totalRequests,
		TotalSuccesses: b.totalSuccesses,
		TotalFailures:  b.totalFailures,
		TotalLatencyNs: b.totalLatencyNs,
	}
}

func (b *Backend) AvgLatencyMs() float64 {
	s := b.Stats()
	if s.TotalRequests == 0 {
		return 0
	}
	return float64(s.TotalLatencyNs) / float64(s.TotalRequests) / 1e6
}

func (b *Backend) ErrorRate() float64 {
	s := b.Stats()
	if s.TotalRequests == 0 {
		return 0
	}
	return float64(s.TotalFailures) / float64(s.TotalRequests)
}

func (b *Backend) ResetStats() {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	b.totalRequests = 0
	b.totalSuccesses = 0
	b.totalFailures = 0
	b.totalLatencyNs = 0
	fmt.Printf("Backend %s stats reset\n", b.Name)
}

func (b *Backend) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastCheck = time.Now()
	b.failCount = 0
	b.successCount++

	if b.status != StatusHealthy && b.successCount >= b.successThreshold {
		b.status = StatusHealthy
		b.lastHCError = ""
		fmt.Printf("Backend %s is now healthy (consecutive HC successes: %d)\n", b.Name, b.successCount)
	}
}

func (b *Backend) RecordFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastCheck = time.Now()
	b.lastHCError = err.Error()
	b.successCount = 0
	b.failCount++

	if b.status != StatusUnhealthy && b.failCount >= b.failureThreshold {
		b.status = StatusUnhealthy
		fmt.Printf("Backend %s is now unhealthy (consecutive HC failures: %d): %v\n", b.Name, b.failCount, err)
	}
}

func (b *Backend) RecordRequestSuccess(latency time.Duration) {
	b.mu.Lock()
	b.lastRequestTime = time.Now()
	b.lastRequestError = ""
	b.mu.Unlock()

	b.statsMu.Lock()
	b.totalRequests++
	b.totalSuccesses++
	b.totalLatencyNs += int64(latency)
	b.statsMu.Unlock()
}

func (b *Backend) RecordRequestFailure(err error, latency time.Duration) {
	b.mu.Lock()
	b.lastRequestTime = time.Now()
	if err != nil {
		b.lastRequestError = err.Error()
	} else {
		b.lastRequestError = "request failed"
	}
	b.mu.Unlock()

	b.statsMu.Lock()
	b.totalRequests++
	b.totalFailures++
	b.totalLatencyNs += int64(latency)
	b.statsMu.Unlock()
}

func (b *Backend) SetUnhealthy(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = StatusUnhealthy
	b.lastHCError = err.Error()
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
	backends []*Backend
	path     string
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	stopCh   chan struct{}
	running  bool
	mu       sync.Mutex
	id       int64
}

var healthCheckerCounter int64

func NewHealthChecker(path string, interval, timeout time.Duration) *HealthChecker {
	id := atomic.AddInt64(&healthCheckerCounter, 1)
	return &HealthChecker{
		path:     path,
		interval: interval,
		timeout:  timeout,
		id:       id,
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
	stopCh := hc.stopCh
	hc.mu.Unlock()

	fmt.Printf("Health checker #%d started (path=%s, interval=%s, timeout=%s, %d backends)\n",
		hc.id, hc.path, hc.interval, hc.timeout, len(hc.backends))

	go hc.run(stopCh)
}

func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	if !hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = false
	close(hc.stopCh)
	hc.backends = nil
	hc.mu.Unlock()
	fmt.Printf("Health checker #%d stopped\n", hc.id)
}

func (hc *HealthChecker) run(stopCh chan struct{}) {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	hc.checkAll()

	for {
		select {
		case <-ticker.C:
			hc.checkAll()
		case <-stopCh:
			return
		}
	}
}

func (hc *HealthChecker) checkAll() {
	hc.mu.Lock()
	if !hc.running {
		hc.mu.Unlock()
		return
	}
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
		b.RecordFailure(fmt.Errorf("HC #%d failed: %w", hc.id, err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		b.RecordSuccess()
	} else {
		b.RecordFailure(fmt.Errorf("HC #%d returned status %d", hc.id, resp.StatusCode))
	}
}

type BackendPool struct {
	mu       sync.RWMutex
	backends []*Backend
	checker  *HealthChecker
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

func (bp *BackendPool) EligibleBackends() []*Backend {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	eligible := make([]*Backend, 0)
	for _, b := range bp.backends {
		if b.IsEligible() {
			eligible = append(eligible, b)
		}
	}
	return eligible
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

type PoolStats struct {
	TotalRequests     int64
	TotalSuccesses    int64
	TotalFailures     int64
	TotalLatencyNs    int64
	TotalActiveConns  int64
	TotalBackends     int
	HealthyBackends   int
	EligibleBackends  int
	MaintenanceBackends int
	UnhealthyBackends int
}

func (bp *BackendPool) PoolStats() PoolStats {
	bp.mu.RLock()
	backends := make([]*Backend, len(bp.backends))
	copy(backends, bp.backends)
	bp.mu.RUnlock()

	var ps PoolStats
	ps.TotalBackends = len(backends)
	for _, b := range backends {
		s := b.Stats()
		ps.TotalRequests += s.TotalRequests
		ps.TotalSuccesses += s.TotalSuccesses
		ps.TotalFailures += s.TotalFailures
		ps.TotalLatencyNs += s.TotalLatencyNs
		ps.TotalActiveConns += b.ActiveConns()

		switch {
		case b.IsMaintenance():
			ps.MaintenanceBackends++
		case b.IsHealthy():
			ps.HealthyBackends++
			ps.EligibleBackends++
		default:
			ps.UnhealthyBackends++
		}
	}
	return ps
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
