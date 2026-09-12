package agentevents

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// A small process-local allocation budget, not a replacement for edge abuse
// controls. RemoteAddr is the verified peer; behind VPC many users share it.
// Never let caller-supplied forwarding headers mint fresh rate-limit buckets.
type authAllocationLimiter struct {
	mu    sync.Mutex
	start time.Time
	total int
	peers map[string]int
}

func (l *authAllocationLimiter) allow(peer string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.start.IsZero() || now.Sub(l.start) >= time.Minute {
		l.start = now
		l.total = 0
		l.peers = make(map[string]int)
	}
	if l.total >= 300 || l.peers[peer] >= 60 {
		return false
	}
	// At most300 distinct peer entries can be allocated in one window.
	l.total++
	l.peers[peer]++
	return true
}
func (s *BrowserAuthServer) allowAuthAllocation(w http.ResponseWriter, r *http.Request) bool {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	if s.authAllocations.allow(peer, time.Now()) {
		return true
	}
	w.Header().Set("Retry-After", "60")
	writeBrowserAuthJSON(w, 429, map[string]string{"error": "rate_limited"})
	return false
}
