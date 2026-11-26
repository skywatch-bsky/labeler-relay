// pattern: Imperative Shell
// ratelimit.go implements the per-client-IP rate limiter guarding the public
// query endpoints (getLabelers/getLabeler). It reuses the two-tier sliding
// window pattern from internal/slurper/ratelimit.go: a per-second window
// against bursts and a per-hour window against sustained flood.

package server

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/RussellLuo/slidingwindow"
)

const (
	// maxTrackedIPs bounds the per-IP window map so a spoofed-IP flood cannot
	// grow it without limit.
	maxTrackedIPs = 65536

	// ipIdleEviction is the idle horizon applied when the map is full: IPs
	// silent longer than this are evicted first.
	ipIdleEviction = time.Hour
)

// ipWindow holds the two sliding windows for one client IP plus its last-seen
// timestamp used by eviction.
type ipWindow struct {
	perSec      *slidingwindow.Limiter
	perSecStop  slidingwindow.StopFunc
	perHour     *slidingwindow.Limiter
	perHourStop slidingwindow.StopFunc
	lastSeen    time.Time
}

// QueryRateLimiter rate-limits requests per client IP with two-tier sliding
// windows (per-second + per-hour). Rejections are reported through an
// injected callback so this package never imports metrics (FCIS).
type QueryRateLimiter struct {
	mu      sync.Mutex
	windows map[string]*ipWindow
	perSec  int64
	perHour int64
	maxIPs  int
	// onRateLimited is called with the endpoint name when a request is
	// rejected. May be nil.
	onRateLimited func(endpoint string)
}

// NewQueryRateLimiter creates a limiter with the given per-second and
// per-hour token budgets per client IP.
func NewQueryRateLimiter(perSec, perHour int) *QueryRateLimiter {
	return newQueryRateLimiter(perSec, perHour, maxTrackedIPs)
}

// newQueryRateLimiter is the test seam with an explicit map cap.
func newQueryRateLimiter(perSec, perHour int, maxIPs int) *QueryRateLimiter {
	return &QueryRateLimiter{
		windows: make(map[string]*ipWindow),
		perSec:  int64(perSec),
		perHour: int64(perHour),
		maxIPs:  maxIPs,
	}
}

// SetRateLimitedCallback registers a function called with the endpoint name
// each time a request is rejected. Used to increment Prometheus counters
// without importing the metrics package from server (FCIS: callback
// injection).
func (q *QueryRateLimiter) SetRateLimitedCallback(fn func(endpoint string)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.onRateLimited = fn
}

// Close stops the background goroutines behind every tracked window. Local
// windows stop nothing, but this keeps the map hygienic if the window
// implementation ever changes.
func (q *QueryRateLimiter) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for ip, w := range q.windows {
		w.perSecStop()
		w.perHourStop()
		delete(q.windows, ip)
	}
}

// Allow consumes one token from both windows for ip and reports whether the
// request may proceed.
func (q *QueryRateLimiter) Allow(ip string) bool {
	return q.allowAt(ip, time.Now())
}

// allowAt is Allow at an injected timestamp so tests can drive window
// expiry deterministically (AllowN consumes at `now`).
func (q *QueryRateLimiter) allowAt(ip string, now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	w, ok := q.windows[ip]
	if !ok {
		if len(q.windows) >= q.maxIPs {
			q.evictLocked(now)
		}
		w = q.newIPWindow()
		q.windows[ip] = w
	}
	w.lastSeen = now

	// AllowN is destructive: it consumes a token from the window even if the
	// other window later rejects. When perSec allows but perHour rejects, the
	// per-second token is silently wasted. Same accepted imprecision as the
	// slurper limiter.
	return w.perSec.AllowN(now, 1) && w.perHour.AllowN(now, 1)
}

// evictLocked makes room for one more IP when the map is at cap. Idle
// entries beyond ipIdleEviction go first; otherwise the least recently seen
// entry is dropped until one slot is free. Callers hold q.mu.
func (q *QueryRateLimiter) evictLocked(now time.Time) {
	for ip, w := range q.windows {
		if now.Sub(w.lastSeen) > ipIdleEviction {
			w.perSecStop()
			w.perHourStop()
			delete(q.windows, ip)
		}
	}
	for len(q.windows) >= q.maxIPs {
		oldestIP := ""
		var oldest time.Time
		first := true
		for ip, w := range q.windows {
			if first || w.lastSeen.Before(oldest) {
				oldestIP = ip
				oldest = w.lastSeen
				first = false
			}
		}
		w := q.windows[oldestIP]
		w.perSecStop()
		w.perHourStop()
		delete(q.windows, oldestIP)
	}
}

// newIPWindow builds the two-tier window pair for one IP. Callers hold q.mu.
func (q *QueryRateLimiter) newIPWindow() *ipWindow {
	newWindow := func() (slidingwindow.Window, slidingwindow.StopFunc) {
		return slidingwindow.NewLocalWindow()
	}

	perSec, perSecStop := slidingwindow.NewLimiter(time.Second, q.perSec, newWindow)
	perHour, perHourStop := slidingwindow.NewLimiter(time.Hour, q.perHour, newWindow)

	return &ipWindow{
		perSec:      perSec,
		perSecStop:  perSecStop,
		perHour:     perHour,
		perHourStop: perHourStop,
	}
}

// Middleware wraps next with the per-IP limit for one endpoint. Rejected
// requests get an XRPC-shaped 429 with a Retry-After hint, then the injected
// rate-limited callback fires.
func (q *QueryRateLimiter) Middleware(endpoint string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Key on the connection peer. X-Forwarded-For is deliberately
		// ignored: any client can set it.
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		if !q.Allow(ip) {
			w.Header().Set("Retry-After", "1")
			writeXRPCError(w, http.StatusTooManyRequests, "RateLimitExceeded", "rate limit exceeded for this endpoint")
			q.mu.Lock()
			fn := q.onRateLimited
			q.mu.Unlock()
			if fn != nil {
				fn(endpoint)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}
