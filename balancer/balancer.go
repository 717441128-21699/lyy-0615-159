package balancer

import (
	"hash/crc32"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"loadbalancer/backend"
)

const virtualNodes = 150

type LoadBalancer interface {
	Next(req *http.Request, exclude map[string]bool) (*backend.Backend, error)
	Name() string
}

type RoundRobin struct {
	mu        sync.Mutex
	backends  []*backend.Backend
	current   int
	weights   []int
	maxWeight int
	gcdWeight int
	position  int
}

func NewRoundRobin(backends []*backend.Backend) *RoundRobin {
	rr := &RoundRobin{
		backends: backends,
	}
	if len(backends) > 0 {
		rr.weights = make([]int, len(backends))
		for i, b := range backends {
			rr.weights[i] = b.Weight
		}
		rr.maxWeight = maxInt(rr.weights)
		rr.gcdWeight = gcdInts(rr.weights)
		rr.position = len(backends) - 1
	}
	return rr
}

func (rr *RoundRobin) Name() string {
	return "round_robin"
}

func (rr *RoundRobin) Next(req *http.Request, exclude map[string]bool) (*backend.Backend, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if len(rr.backends) == 0 {
		return nil, ErrNoHealthyBackends
	}

	n := len(rr.backends)
	for i := 0; i < n*rr.maxWeight; i++ {
		rr.position = (rr.position + 1) % n
		if rr.position == 0 {
			rr.current = rr.current - rr.gcdWeight
			if rr.current <= 0 {
				rr.current = rr.maxWeight
				if rr.current == 0 {
					return nil, ErrNoHealthyBackends
				}
			}
		}
		b := rr.backends[rr.position]
		if _, excluded := exclude[b.Name]; excluded {
			continue
		}
		if rr.weights[rr.position] >= rr.current && b.IsEligible() {
			return b, nil
		}
	}

	for _, b := range rr.backends {
		if excluded := exclude[b.Name]; excluded {
			continue
		}
		if b.IsEligible() {
			return b, nil
		}
	}

	for _, b := range rr.backends {
		if b.IsEligible() {
			return b, nil
		}
	}
	return nil, ErrNoHealthyBackends
}

type LeastConn struct {
	mu       sync.RWMutex
	backends []*backend.Backend
}

func NewLeastConn(backends []*backend.Backend) *LeastConn {
	return &LeastConn{backends: backends}
}

func (lc *LeastConn) Name() string {
	return "least_conn"
}

func (lc *LeastConn) Next(req *http.Request, exclude map[string]bool) (*backend.Backend, error) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	var best *backend.Backend
	var least int64 = -1

	for _, b := range lc.backends {
		if excluded := exclude[b.Name]; excluded {
			continue
		}
		if !b.IsEligible() {
			continue
		}
		conns := b.ActiveConns()
		weighted := conns * int64(b.Weight)
		if least == -1 || weighted < least {
			least = weighted
			best = b
		}
	}

	if best == nil {
		for _, b := range lc.backends {
			if b.IsEligible() {
				return b, nil
			}
		}
		return nil, ErrNoHealthyBackends
	}
	return best, nil
}

type ConsistentHash struct {
	mu         sync.RWMutex
	backends   []*backend.Backend
	ring       []uint32
	hashMap    map[uint32]*backend.Backend
	hashHeader string
}

func NewConsistentHash(backends []*backend.Backend, hashHeader string) *ConsistentHash {
	ch := &ConsistentHash{
		backends:   backends,
		hashMap:    make(map[uint32]*backend.Backend),
		hashHeader: hashHeader,
	}
	ch.buildRing()
	return ch
}

func (ch *ConsistentHash) Name() string {
	return "consistent_hash"
}

func (ch *ConsistentHash) buildRing() {
	ch.ring = make([]uint32, 0)
	ch.hashMap = make(map[uint32]*backend.Backend)

	for _, b := range ch.backends {
		for i := 0; i < virtualNodes; i++ {
			key := b.Name + "-vn-" + strconv.Itoa(i)
			hash := crc32.ChecksumIEEE([]byte(key))
			ch.ring = append(ch.ring, hash)
			ch.hashMap[hash] = b
		}
	}

	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i] < ch.ring[j]
	})
}

func (ch *ConsistentHash) Next(req *http.Request, exclude map[string]bool) (*backend.Backend, error) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return nil, ErrNoHealthyBackends
	}

	key := ch.getKey(req)
	hash := crc32.ChecksumIEEE([]byte(key))

	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})

	if idx == len(ch.ring) {
		idx = 0
	}

	startIdx := idx
	for {
		nodeHash := ch.ring[idx]
		b := ch.hashMap[nodeHash]
		if excluded := exclude[b.Name]; !excluded && b.IsEligible() {
			return b, nil
		}

		idx = (idx + 1) % len(ch.ring)
		if idx == startIdx {
			break
		}
	}

	for _, b := range ch.backends {
		if excluded := exclude[b.Name]; !excluded && b.IsEligible() {
			return b, nil
		}
	}

	for _, b := range ch.backends {
		if b.IsEligible() {
			return b, nil
		}
	}
	return nil, ErrNoHealthyBackends
}

func (ch *ConsistentHash) getKey(req *http.Request) string {
	if ch.hashHeader != "" {
		if val := req.Header.Get(ch.hashHeader); val != "" {
			return val
		}
	}
	ip := getClientIP(req)
	if ip != "" {
		return ip
	}
	return req.RemoteAddr
}

func getClientIP(req *http.Request) string {
	if xff := req.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if xri := req.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	return ""
}

var ErrNoHealthyBackends = &NoHealthyBackendsError{}

type NoHealthyBackendsError struct{}

func (e *NoHealthyBackendsError) Error() string {
	return "no healthy backends available"
}

func maxInt(arr []int) int {
	maxVal := 0
	for _, v := range arr {
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal
}

func gcdInts(arr []int) int {
	if len(arr) == 0 {
		return 1
	}
	result := arr[0]
	for i := 1; i < len(arr); i++ {
		result = gcd(result, arr[i])
	}
	return result
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
