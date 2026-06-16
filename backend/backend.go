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

type CircuitStatus int

const (
	CircuitClosed CircuitStatus = iota
	CircuitOpen
	CircuitHalfOpen
)

type RateLimiter struct {
	capacity       int64
	tokens         int64
	refillAmount   int64
	refillInterval time.Duration
	lastRefill     time.Time
	mu             sync.Mutex
	enabled        bool
}

func NewRateLimiter(capacity int64, refillAmount int64, refillInterval time.Duration) *RateLimiter {
	return &RateLimiter{
		capacity:       capacity,
		tokens:         capacity,
		refillAmount:   refillAmount,
		refillInterval: refillInterval,
		lastRefill:     time.Now(),
		enabled:        capacity > 0,
	}
}

func (rl *RateLimiter) SetLimit(capacity int64, refillAmount int64, refillInterval time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.capacity = capacity
	rl.refillAmount = refillAmount
	rl.refillInterval = refillInterval
	rl.tokens = capacity
	rl.lastRefill = time.Now()
	rl.enabled = capacity > 0
}

func (rl *RateLimiter) Disable() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.enabled = false
}

func (rl *RateLimiter) IsEnabled() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.enabled
}

func (rl *RateLimiter) TryAcquire() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if !rl.enabled {
		return true
	}

	now := time.Now()
	elapsed := now.Sub(rl.lastRefill)
	if elapsed >= rl.refillInterval {
		refillCount := int64(elapsed / rl.refillInterval)
		rl.tokens = min(rl.capacity, rl.tokens+refillCount*rl.refillAmount)
		rl.lastRefill = now
	}

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}
	return false
}

func (rl *RateLimiter) Config() (capacity int64, refillAmount int64, refillInterval time.Duration, enabled bool) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.capacity, rl.refillAmount, rl.refillInterval, rl.enabled
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type BackendStats struct {
	TotalRequests    int64
	TotalSuccesses   int64
	TotalFailures    int64
	TotalLatencyNs  int64
	TotalRateLimited int64

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
	totalRateLimited   int64

	rateLimiter *RateLimiter

	circuitStatus      CircuitStatus
	circuitOpenSince   time.Time
	circuitOpenReason  string
	circuitOpenSeconds int
	circuitConsecFails int
	circuitFailThreshold int
	circuitProbeSuccess int
	circuitManualOpen  bool
}

func NewBackend(name, rawURL string, weight, failureThreshold, successThreshold int) (*Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse backend URL: %w", err)
	}
	return &Backend{
		Name:                    name,
		URL:                     u,
		Weight:                  weight,
		status:                  StatusUnknown,
		failureThreshold:        failureThreshold,
		successThreshold:        successThreshold,
		rateLimiter:             NewRateLimiter(0, 0, 0),
		circuitFailThreshold:    5,
		circuitOpenSeconds:      30,
		circuitProbeSuccess:     2,
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
	return !b.maintenance && b.status == StatusHealthy && b.circuitStatus != CircuitOpen
}

func (b *Backend) CircuitStatus() CircuitStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.circuitStatus
}

func (b *Backend) CircuitInfo() (status CircuitStatus, reason string, remainingSec int, openSince time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	remaining := 0
	if b.circuitStatus == CircuitOpen && !b.circuitOpenSince.IsZero() {
		elapsed := int(time.Since(b.circuitOpenSince).Seconds())
		remaining = b.circuitOpenSeconds - elapsed
		if remaining < 0 {
			remaining = 0
		}
	}
	return b.circuitStatus, b.circuitOpenReason, remaining, b.circuitOpenSince
}

func (b *Backend) SetCircuitThresholds(failThreshold int, openSeconds int, probeSuccess int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if failThreshold > 0 {
		b.circuitFailThreshold = failThreshold
	}
	if openSeconds > 0 {
		b.circuitOpenSeconds = openSeconds
	}
	if probeSuccess > 0 {
		b.circuitProbeSuccess = probeSuccess
	}
}

func (b *Backend) RecordCircuitSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.circuitConsecFails = 0
	if b.circuitStatus == CircuitHalfOpen {
		b.circuitProbeSuccess--
		if b.circuitProbeSuccess <= 0 {
			b.circuitStatus = CircuitClosed
			b.circuitOpenReason = ""
			b.circuitOpenSince = time.Time{}
			fmt.Printf("Backend %s circuit CLOSED (probe successes reached threshold)\n", b.Name)
		}
	}
}

func (b *Backend) RecordCircuitFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.circuitStatus == CircuitHalfOpen {
		b.circuitStatus = CircuitOpen
		b.circuitOpenSince = time.Now()
		b.circuitOpenReason = fmt.Sprintf("probe failed: %v", err)
		b.circuitConsecFails = 0
		fmt.Printf("Backend %s circuit RE-OPENED: probe failed\n", b.Name)
		return
	}

	if b.circuitStatus == CircuitOpen {
		return
	}

	b.circuitConsecFails++
	if b.circuitConsecFails >= b.circuitFailThreshold && !b.circuitManualOpen {
		b.circuitStatus = CircuitOpen
		b.circuitOpenSince = time.Now()
		b.circuitOpenReason = fmt.Sprintf("consecutive failures: %d, last: %v", b.circuitConsecFails, err)
		b.circuitConsecFails = 0
		fmt.Printf("Backend %s circuit OPENED: %s\n", b.Name, b.circuitOpenReason)
	}
}

func (b *Backend) TryCircuitHalfOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.circuitStatus != CircuitOpen || b.circuitManualOpen {
		return false
	}

	elapsed := time.Since(b.circuitOpenSince)
	if elapsed >= time.Duration(b.circuitOpenSeconds)*time.Second {
		b.circuitStatus = CircuitHalfOpen
		b.circuitConsecFails = 0
		b.circuitProbeSuccess = b.circuitProbeSuccess
		fmt.Printf("Backend %s circuit HALF-OPEN: starting probe\n", b.Name)
		return true
	}
	return false
}

func (b *Backend) SetManualCircuitOpen(open bool, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if open {
		b.circuitManualOpen = true
		b.circuitStatus = CircuitOpen
		b.circuitOpenSince = time.Now()
		if reason == "" {
			reason = "manual operation"
		}
		b.circuitOpenReason = fmt.Sprintf("manual: %s", reason)
		fmt.Printf("Backend %s circuit manually OPENED: %s\n", b.Name, b.circuitOpenReason)
	} else {
		b.circuitManualOpen = false
		b.circuitStatus = CircuitClosed
		b.circuitOpenReason = ""
		b.circuitOpenSince = time.Time{}
		b.circuitConsecFails = 0
		fmt.Printf("Backend %s circuit manually CLOSED\n", b.Name)
	}
}

func (b *Backend) TryAcquireRateLimit() bool {
	ok := b.rateLimiter.TryAcquire()
	if !ok {
		b.statsMu.Lock()
		b.totalRateLimited++
		b.statsMu.Unlock()
	}
	return ok
}

func (b *Backend) SetRateLimit(capacity int64, refillAmount int64, refillInterval time.Duration) {
	b.rateLimiter.SetLimit(capacity, refillAmount, refillInterval)
	fmt.Printf("Backend %s rate limit set: capacity=%d, refill=%d/%s\n", b.Name, capacity, refillAmount, refillInterval)
}

func (b *Backend) DisableRateLimit() {
	b.rateLimiter.Disable()
	fmt.Printf("Backend %s rate limit disabled\n", b.Name)
}

func (b *Backend) RateLimitConfig() (capacity int64, refillAmount int64, refillInterval time.Duration, enabled bool) {
	return b.rateLimiter.Config()
}

func (b *Backend) TotalRateLimited() int64 {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()
	return b.totalRateLimited
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
		TotalRequests:    b.totalRequests,
		TotalSuccesses:   b.totalSuccesses,
		TotalFailures:    b.totalFailures,
		TotalLatencyNs:   b.totalLatencyNs,
		TotalRateLimited: b.totalRateLimited,
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

	b.RecordCircuitSuccess()

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

	b.RecordCircuitFailure(err)

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
	mu           sync.RWMutex
	backends     []*Backend
	checker      *HealthChecker
	rateLimiter  *RateLimiter
	statsMu      sync.Mutex
	totalRateLimited int64
}

func NewBackendPool() *BackendPool {
	return &BackendPool{
		backends:    make([]*Backend, 0),
		rateLimiter: NewRateLimiter(0, 0, 0),
	}
}

func (bp *BackendPool) TryAcquireGlobalRateLimit() bool {
	ok := bp.rateLimiter.TryAcquire()
	if !ok {
		bp.statsMu.Lock()
		bp.totalRateLimited++
		bp.statsMu.Unlock()
	}
	return ok
}

func (bp *BackendPool) SetGlobalRateLimit(capacity int64, refillAmount int64, refillInterval time.Duration) {
	bp.rateLimiter.SetLimit(capacity, refillAmount, refillInterval)
	fmt.Printf("Global rate limit set: capacity=%d, refill=%d/%s\n", capacity, refillAmount, refillInterval)
}

func (bp *BackendPool) DisableGlobalRateLimit() {
	bp.rateLimiter.Disable()
	fmt.Println("Global rate limit disabled")
}

func (bp *BackendPool) GlobalRateLimitConfig() (capacity int64, refillAmount int64, refillInterval time.Duration, enabled bool) {
	return bp.rateLimiter.Config()
}

func (bp *BackendPool) TotalGlobalRateLimited() int64 {
	bp.statsMu.Lock()
	defer bp.statsMu.Unlock()
	return bp.totalRateLimited
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
	TotalRateLimited  int64
	TotalActiveConns  int64
	TotalBackends     int
	HealthyBackends   int
	EligibleBackends  int
	MaintenanceBackends int
	UnhealthyBackends int
	CircuitOpenBackends int
}

func (bp *BackendPool) PoolStats() PoolStats {
	bp.mu.RLock()
	backends := make([]*Backend, len(bp.backends))
	copy(backends, bp.backends)
	bp.mu.RUnlock()

	bp.statsMu.Lock()
	globalRL := bp.totalRateLimited
	bp.statsMu.Unlock()

	var ps PoolStats
	ps.TotalBackends = len(backends)
	ps.TotalRateLimited = globalRL
	for _, b := range backends {
		s := b.Stats()
		ps.TotalRequests += s.TotalRequests
		ps.TotalSuccesses += s.TotalSuccesses
		ps.TotalFailures += s.TotalFailures
		ps.TotalLatencyNs += s.TotalLatencyNs
		ps.TotalRateLimited += s.TotalRateLimited
		ps.TotalActiveConns += b.ActiveConns()

		if b.CircuitStatus() == CircuitOpen {
			ps.CircuitOpenBackends++
		}

		switch {
		case b.IsMaintenance():
			ps.MaintenanceBackends++
		case b.IsHealthy():
			ps.HealthyBackends++
			if b.CircuitStatus() != CircuitOpen {
				ps.EligibleBackends++
			}
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
