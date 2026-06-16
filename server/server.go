package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

type runningServer struct {
	server *http.Server
	addr   string
}

type LoadBalancerServer struct {
	configMgr *config.ConfigManager

	snapshot atomic.Value

	proxyHandler *atomicProxyHandler

	mu            sync.Mutex
	stopCh        chan struct{}
	running       bool

	proxyServersMu sync.Mutex
	proxyServers   []*runningServer

	adminServersMu sync.Mutex
	adminServers   []*runningServer
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
			if err := oldB.UpdateConfig(
				bc.URL, bc.Weight,
				newCfg.HealthCheck.FailureThreshold,
				newCfg.HealthCheck.SuccessThreshold,
			); err != nil {
				fmt.Printf("Error updating backend %s config: %v, creating new one instead\n", bc.Name, err)
				b, err := backend.NewBackend(
					bc.Name, bc.URL, bc.Weight,
					newCfg.HealthCheck.FailureThreshold,
					newCfg.HealthCheck.SuccessThreshold,
				)
				if err != nil {
					fmt.Printf("Error creating backend %s: %v\n", bc.Name, err)
					continue
				}
				pool.AddBackend(b)
			} else {
				pool.AddBackend(oldB)
			}
		} else {
			b, err := backend.NewBackend(
				bc.Name, bc.URL, bc.Weight,
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

	if oldCfg == nil {
		oldCfg = &config.Config{}
	}
	if oldCfg.Listen != newCfg.Listen {
		go s.switchProxyServer(newCfg.Listen)
	}
	if oldCfg.AdminListen != newCfg.AdminListen {
		go s.switchAdminServer(newCfg.AdminListen)
	}

	fmt.Println("Configuration reloaded successfully")
}

func (s *LoadBalancerServer) switchProxyServer(newAddr string) {
	fmt.Printf("Switching proxy server to new address: %s\n", newAddr)

	mux := http.NewServeMux()
	mux.Handle("/", s.proxyHandler)
	newServer := &http.Server{
		Addr:    newAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", newAddr)
	if err != nil {
		fmt.Printf("Failed to switch proxy server: listen %v (keeping old server running)\n", err)
		return
	}

	go func() {
		fmt.Printf("Proxy server starting on new address %s\n", newAddr)
		if serveErr := newServer.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Printf("New proxy server error: %v\n", serveErr)
		}
	}()

	s.proxyServersMu.Lock()
	oldServers := s.proxyServers
	s.proxyServers = append(s.proxyServers, &runningServer{server: newServer, addr: newAddr})
	s.proxyServersMu.Unlock()

	go s.gracefulShutdownOldServers(oldServers, "proxy")
}

func (s *LoadBalancerServer) switchAdminServer(newAddr string) {
	fmt.Printf("Switching admin server to new address: %s\n", newAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/backends", s.handleBackends)
	mux.HandleFunc("/api/reload", s.handleReload)
	mux.HandleFunc("/api/config", s.handleConfig)

	newServer := &http.Server{
		Addr:    newAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", newAddr)
	if err != nil {
		fmt.Printf("Failed to switch admin server: listen %v (keeping old server running)\n", err)
		return
	}

	go func() {
		fmt.Printf("Admin server starting on new address %s\n", newAddr)
		if serveErr := newServer.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Printf("New admin server error: %v\n", serveErr)
		}
	}()

	s.adminServersMu.Lock()
	oldServers := s.adminServers
	s.adminServers = append(s.adminServers, &runningServer{server: newServer, addr: newAddr})
	s.adminServersMu.Unlock()

	go s.gracefulShutdownOldServers(oldServers, "admin")
}

func (s *LoadBalancerServer) gracefulShutdownOldServers(oldServers []*runningServer, kind string) {
	if len(oldServers) == 0 {
		return
	}
	time.Sleep(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, rs := range oldServers {
		fmt.Printf("Gracefully shutting down old %s server on %s\n", kind, rs.addr)
		if err := rs.server.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down old %s server %s: %v\n", kind, rs.addr, err)
		} else {
			fmt.Printf("Old %s server on %s shut down gracefully\n", kind, rs.addr)
		}
	}
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

	proxyMux := http.NewServeMux()
	proxyMux.Handle("/", s.proxyHandler)
	proxySrv := &http.Server{
		Addr:    cfg.Listen,
		Handler: proxyMux,
	}
	proxyLn, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("proxy listen %s: %w", cfg.Listen, err)
	}
	s.proxyServersMu.Lock()
	s.proxyServers = append(s.proxyServers, &runningServer{server: proxySrv, addr: cfg.Listen})
	s.proxyServersMu.Unlock()
	go func() {
		fmt.Printf("Proxy server listening on %s\n", cfg.Listen)
		if serveErr := proxySrv.Serve(proxyLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Printf("Proxy server error: %v\n", serveErr)
		}
	}()

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", s.handleHealth)
	adminMux.HandleFunc("/api/status", s.handleStatus)
	adminMux.HandleFunc("/api/backends", s.handleBackends)
	adminMux.HandleFunc("/api/reload", s.handleReload)
	adminMux.HandleFunc("/api/config", s.handleConfig)
	adminSrv := &http.Server{
		Addr:    cfg.AdminListen,
		Handler: adminMux,
	}
	adminLn, err := net.Listen("tcp", cfg.AdminListen)
	if err != nil {
		proxySrv.Close()
		return fmt.Errorf("admin listen %s: %w", cfg.AdminListen, err)
	}
	s.adminServersMu.Lock()
	s.adminServers = append(s.adminServers, &runningServer{server: adminSrv, addr: cfg.AdminListen})
	s.adminServersMu.Unlock()
	go func() {
		fmt.Printf("Admin server listening on %s\n", cfg.AdminListen)
		if serveErr := adminSrv.Serve(adminLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Printf("Admin server error: %v\n", serveErr)
		}
	}()

	go s.configMgr.Watch(2 * time.Second)

	return nil
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
		"listen":         snap.Config.Listen,
		"admin_listen":   snap.Config.AdminListen,
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.proxyServersMu.Lock()
	proxySrvs := s.proxyServers
	s.proxyServers = nil
	s.proxyServersMu.Unlock()
	for _, rs := range proxySrvs {
		rs.server.Shutdown(ctx)
	}

	s.adminServersMu.Lock()
	adminSrvs := s.adminServers
	s.adminServers = nil
	s.adminServersMu.Unlock()
	for _, rs := range adminSrvs {
		rs.server.Shutdown(ctx)
	}
}
