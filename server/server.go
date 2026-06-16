package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"loadbalancer/backend"
	"loadbalancer/balancer"
	"loadbalancer/config"
	"loadbalancer/proxy"
)

type ServerSnapshot struct {
	Config   *config.Config
	Pool     *backend.BackendPool
	Balancer balancer.LoadBalancer
	Proxy    *proxy.ReverseProxy
	Checker  *backend.HealthChecker
}

type LoadBalancerServer struct {
	configMgr *config.ConfigManager

	snapshot atomic.Value

	proxyHandler *atomicProxyHandler

	mu       sync.Mutex
	stopCh   chan struct{}
	running  bool
}

type atomicProxyHandler struct {
	mu   sync.RWMutex
	snap *ServerSnapshot
}

func (h *atomicProxyHandler) set(snap *ServerSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snap = snap
}

func (h *atomicProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	snap := h.snap
	h.mu.RUnlock()

	if snap == nil || snap.Proxy == nil {
		http.Error(w, "proxy not ready", http.StatusServiceUnavailable)
		return
	}
	snap.Proxy.ServeHTTP(w, r)
}

func NewLoadBalancerServer(configPath string) (*LoadBalancerServer, error) {
	cm, err := config.NewConfigManager(configPath)
	if err != nil {
		return nil, fmt.Errorf("create config manager: %w", err)
	}

	srv := &LoadBalancerServer{
		configMgr:    cm,
		proxyHandler: &atomicProxyHandler{},
	}

	cm.OnChange(func(old, newCfg *config.Config) {
		srv.onConfigChange(old, newCfg)
	})

	if err := srv.buildFromConfig(cm.Get()); err != nil {
		return nil, fmt.Errorf("initial build: %w", err)
	}

	return srv, nil
}

func (s *LoadBalancerServer) buildFromConfig(cfg *config.Config) error {
	pool := backend.NewBackendPool()

	for _, bc := range cfg.Backends {
		b, err := backend.NewBackend(
			bc.Name,
			bc.URL,
			bc.Weight,
			cfg.HealthCheck.FailureThreshold,
			cfg.HealthCheck.SuccessThreshold,
		)
		if err != nil {
			return fmt.Errorf("create backend %s: %w", bc.Name, err)
		}
		pool.AddBackend(b)
	}

	checker := backend.NewHealthChecker(
		cfg.HealthCheck.Path,
		cfg.HealthCheck.IntervalDuration(),
		cfg.HealthCheck.TimeoutDuration(),
	)
	checker.SetBackends(pool.Backends())
	pool.SetHealthChecker(checker)

	var lb balancer.LoadBalancer
	switch cfg.LoadBalancing.Strategy {
	case "least_conn":
		lb = balancer.NewLeastConn(pool.Backends())
	case "consistent_hash":
		lb = balancer.NewConsistentHash(pool.Backends(), cfg.LoadBalancing.HashHeader)
	case "round_robin":
		fallthrough
	default:
		lb = balancer.NewRoundRobin(pool.Backends())
	}

	rp := proxy.NewReverseProxy(
		lb,
		cfg.Retry.MaxRetries,
		cfg.Retry.RetryOnStatus,
		cfg.Retry.BackoffDuration(),
	)

	snap := &ServerSnapshot{
		Config:   cfg,
		Pool:     pool,
		Balancer: lb,
		Proxy:    rp,
		Checker:  checker,
	}

	s.snapshot.Store(snap)
	s.proxyHandler.set(snap)
	checker.Start()

	fmt.Printf("Built new server snapshot with %d backends, strategy: %s\n",
		len(cfg.Backends), cfg.LoadBalancing.Strategy)
	return nil
}

func (s *LoadBalancerServer) onConfigChange(oldCfg, newCfg *config.Config) {
	oldSnap := s.currentSnapshot()

	pool := backend.NewBackendPool()

	oldBackendMap := make(map[string]*backend.Backend)
	if oldSnap != nil {
		for _, b := range oldSnap.Pool.Backends() {
			oldBackendMap[b.Name] = b
		}
	}

	for _, bc := range newCfg.Backends {
		if oldB, ok := oldBackendMap[bc.Name]; ok {
			pool.AddBackend(oldB)
		} else {
			b, err := backend.NewBackend(
				bc.Name,
				bc.URL,
				bc.Weight,
				newCfg.HealthCheck.FailureThreshold,
				newCfg.HealthCheck.SuccessThreshold,
			)
			if err != nil {
				fmt.Printf("Error creating backend %s: %v\n", bc.Name, err)
				continue
			}
			pool.AddBackend(b)
		}
	}

	checker := backend.NewHealthChecker(
		newCfg.HealthCheck.Path,
		newCfg.HealthCheck.IntervalDuration(),
		newCfg.HealthCheck.TimeoutDuration(),
	)
	checker.SetBackends(pool.Backends())
	pool.SetHealthChecker(checker)

	var lb balancer.LoadBalancer
	switch newCfg.LoadBalancing.Strategy {
	case "least_conn":
		lb = balancer.NewLeastConn(pool.Backends())
	case "consistent_hash":
		lb = balancer.NewConsistentHash(pool.Backends(), newCfg.LoadBalancing.HashHeader)
	case "round_robin":
		fallthrough
	default:
		lb = balancer.NewRoundRobin(pool.Backends())
	}

	rp := proxy.NewReverseProxy(
		lb,
		newCfg.Retry.MaxRetries,
		newCfg.Retry.RetryOnStatus,
		newCfg.Retry.BackoffDuration(),
	)

	newSnap := &ServerSnapshot{
		Config:   newCfg,
		Pool:     pool,
		Balancer: lb,
		Proxy:    rp,
		Checker:  checker,
	}

	s.proxyHandler.set(newSnap)
	s.snapshot.Store(newSnap)
	checker.Start()

	if oldSnap != nil && oldSnap.Checker != nil {
		go func() {
			time.Sleep(30 * time.Second)
			oldSnap.Checker.Stop()
			fmt.Println("Old health checker stopped after graceful period")
		}()
	}

	fmt.Println("Configuration reloaded successfully")
}

func (s *LoadBalancerServer) currentSnapshot() *ServerSnapshot {
	v := s.snapshot.Load()
	if v == nil {
		return nil
	}
	return v.(*ServerSnapshot)
}

func (s *LoadBalancerServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	cfg := s.configMgr.Get()

	go s.startProxyServer(cfg.Listen)
	go s.startAdminServer(cfg.AdminListen)
	go s.configMgr.Watch(2 * time.Second)

	fmt.Printf("Proxy server listening on %s\n", cfg.Listen)
	fmt.Printf("Admin server listening on %s\n", cfg.AdminListen)

	return nil
}

func (s *LoadBalancerServer) startProxyServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/", s.proxyHandler)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("Proxy server error: %v\n", err)
	}
}

func (s *LoadBalancerServer) startAdminServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/backends", s.handleBackends)
	mux.HandleFunc("/api/reload", s.handleReload)
	mux.HandleFunc("/api/config", s.handleConfig)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("Admin server error: %v\n", err)
	}
}

func (s *LoadBalancerServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *LoadBalancerServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.currentSnapshot()
	if snap == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	status := map[string]interface{}{
		"config_version":   s.configMgr.Version(),
		"strategy":         snap.Config.LoadBalancing.Strategy,
		"total_backends":   len(snap.Pool.Backends()),
		"healthy_backends": len(snap.Pool.HealthyBackends()),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

type BackendStatusResp struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Weight      int    `json:"weight"`
	Status      string `json:"status"`
	ActiveConns int64  `json:"active_conns"`
	FailCount   int    `json:"fail_count"`
	SuccessCount int   `json:"success_count"`
	LastError   string `json:"last_error,omitempty"`
	LastCheck   string `json:"last_check,omitempty"`
}

func (s *LoadBalancerServer) handleBackends(w http.ResponseWriter, r *http.Request) {
	snap := s.currentSnapshot()
	if snap == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	backends := snap.Pool.Backends()
	resp := make([]BackendStatusResp, 0, len(backends))
	for _, b := range backends {
		statusStr := "unknown"
		switch b.Status() {
		case backend.StatusHealthy:
			statusStr = "healthy"
		case backend.StatusUnhealthy:
			statusStr = "unhealthy"
		}

		lastCheck := ""
		if !b.LastCheck().IsZero() {
			lastCheck = b.LastCheck().Format(time.RFC3339)
		}

		resp = append(resp, BackendStatusResp{
			Name:         b.Name,
			URL:          b.URL.String(),
			Weight:       b.Weight,
			Status:       statusStr,
			ActiveConns:  b.ActiveConns(),
			FailCount:    b.FailCount(),
			SuccessCount: b.SuccessCount(),
			LastError:    b.LastError(),
			LastCheck:    lastCheck,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *LoadBalancerServer) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.configMgr.Load(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("config reloaded"))
}

func (s *LoadBalancerServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	snap := s.currentSnapshot()
	if snap == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap.Config)
}

func (s *LoadBalancerServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.running = false
	close(s.stopCh)

	snap := s.currentSnapshot()
	if snap != nil && snap.Checker != nil {
		snap.Checker.Stop()
	}
}
