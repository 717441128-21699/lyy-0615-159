package config

import (
	"fmt"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
	"os"
)

type BackendConfig struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

type HealthCheckConfig struct {
	Interval         string `yaml:"interval"`
	Timeout          string `yaml:"timeout"`
	Path             string `yaml:"path"`
	FailureThreshold int    `yaml:"failure_threshold"`
	SuccessThreshold int    `yaml:"success_threshold"`
}

type RetryConfig struct {
	MaxRetries     int      `yaml:"max_retries"`
	RetryOnStatus  []int    `yaml:"retry_on_status"`
	Backoff        string   `yaml:"backoff"`
}

type LoadBalancingConfig struct {
	Strategy   string `yaml:"strategy"`
	HashHeader string `yaml:"hash_header"`
}

type Config struct {
	Listen         string              `yaml:"listen"`
	AdminListen    string              `yaml:"admin_listen"`
	HealthCheck    HealthCheckConfig   `yaml:"health_check"`
	Retry          RetryConfig         `yaml:"retry"`
	LoadBalancing  LoadBalancingConfig `yaml:"load_balancing"`
	Backends       []BackendConfig     `yaml:"backends"`
}

type ConfigManager struct {
	mu       sync.RWMutex
	config   *Config
	filePath string
	version  int64
	listeners []func(*Config, *Config)
}

func NewConfigManager(filePath string) (*ConfigManager, error) {
	cm := &ConfigManager{
		filePath:  filePath,
		listeners: make([]func(*Config, *Config), 0),
	}
	if err := cm.Load(); err != nil {
		return nil, err
	}
	return cm, nil
}

func (cm *ConfigManager) Load() error {
	data, err := os.ReadFile(cm.filePath)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	cm.mu.Lock()
	oldConfig := cm.config
	cm.config = &cfg
	cm.version++
	newVersion := cm.version
	listeners := make([]func(*Config, *Config), len(cm.listeners))
	copy(listeners, cm.listeners)
	cm.mu.Unlock()

	for _, listener := range listeners {
		listener(oldConfig, &cfg)
	}

	fmt.Printf("Config loaded (version %d)\n", newVersion)
	return nil
}

func validateConfig(cfg *Config) error {
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.HealthCheck.Interval == "" {
		cfg.HealthCheck.Interval = "5s"
	}
	if cfg.HealthCheck.Timeout == "" {
		cfg.HealthCheck.Timeout = "2s"
	}
	if cfg.HealthCheck.Path == "" {
		cfg.HealthCheck.Path = "/health"
	}
	if cfg.HealthCheck.FailureThreshold <= 0 {
		cfg.HealthCheck.FailureThreshold = 3
	}
	if cfg.HealthCheck.SuccessThreshold <= 0 {
		cfg.HealthCheck.SuccessThreshold = 2
	}
	if cfg.Retry.MaxRetries < 0 {
		cfg.Retry.MaxRetries = 0
	}
	if cfg.Retry.Backoff == "" {
		cfg.Retry.Backoff = "100ms"
	}
	if cfg.LoadBalancing.Strategy == "" {
		cfg.LoadBalancing.Strategy = "round_robin"
	}
	if len(cfg.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}
	for i, b := range cfg.Backends {
		if b.Name == "" {
			cfg.Backends[i].Name = fmt.Sprintf("backend-%d", i+1)
		}
		if b.URL == "" {
			return fmt.Errorf("backend %d has empty URL", i)
		}
		if b.Weight <= 0 {
			cfg.Backends[i].Weight = 1
		}
	}
	return nil
}

func (cm *ConfigManager) Get() *Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config
}

func (cm *ConfigManager) Version() int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.version
}

func (cm *ConfigManager) OnChange(fn func(old, new *Config)) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.listeners = append(cm.listeners, fn)
}

func (cm *ConfigManager) Watch(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastModTime time.Time
	for range ticker.C {
		info, err := os.Stat(cm.filePath)
		if err != nil {
			fmt.Printf("Error stat config file: %v\n", err)
			continue
		}
		if info.ModTime().After(lastModTime) {
			lastModTime = info.ModTime()
			if err := cm.Load(); err != nil {
				fmt.Printf("Error reloading config: %v\n", err)
			}
		}
	}
}

func (h HealthCheckConfig) IntervalDuration() time.Duration {
	d, err := time.ParseDuration(h.Interval)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

func (h HealthCheckConfig) TimeoutDuration() time.Duration {
	d, err := time.ParseDuration(h.Timeout)
	if err != nil {
		return 2 * time.Second
	}
	return d
}

func (r RetryConfig) BackoffDuration() time.Duration {
	d, err := time.ParseDuration(r.Backoff)
	if err != nil {
		return 100 * time.Millisecond
	}
	return d
}
