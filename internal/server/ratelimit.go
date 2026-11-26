// pattern: Imperative Shell
// ratelimit.go implements the per-client-IP rate limiter guarding the public
// query endpoints (getLabelers/getLabeler). It reuses the two-tier sliding
// window pattern from internal/slurper/ratelimit.go: a per-second window
// against bursts and a per-hour window against sustained flood.

package server

import (
	"container/list"
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
)

// ipWindow holds the two sliding windows for one client IP plus its last-seen
// timestamp and its node in the limiter's recency list.
type ipWindow struct {
	perSec      *slidingwindow.Limiter
	perSecStop  slidingwindow.StopFunc
	perHour     *slidingwindow.Limiter
	perHourStop slidingwindow.StopFunc
	lastSeen    time.Time
	element     *list.Element
}

// QueryRateLimiter rate-limits requests per client IP with two-tier sliding
// windows (per-second + per-hour). Rejections are reported through an
// injected callback so this package never imports metrics (FCIS).
type QueryRateLimiter struct {
	mu sync.Mutex
	// order holds tracked IPs, front = most recently seen, back = least
	// recently seen. Eviction takes the back: that entry is idle whenever
	// any tracked entry is idle, and otherwise it is simply the least
	// recently seen.
	order   *list.List
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
		order:   list.New(),
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
	for ip := range q.windows {
		q.removeLocked(ip)
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
			q.evictLocked()
		}
		w = q.newIPWindow(ip)
		q.windows[ip] = w
	} else {
		q.order.MoveToFront(w.element)
	}
	w.lastSeen = now

	// AllowN is destructive: it consumes a token from the window even if the
	// other window later rejects. When perSec allows but perHour rejects, the
	// per-second token is silently wasted. Same accepted imprecision as the
	// slurper limiter.
	return w.perSec.AllowN(now, 1) && w.perHour.AllowN(now, 1)
}

// evictLocked makes room for one more IP when the map is at cap by dropping
// the least recently seen entry — O(1), with no scan of the tracked set.
// That entry is idle whenever any tracked entry is idle, so the drop order
// matches "idle first, else oldest by last-seen". The list is ordered by last
// operation (creation or touch); with the monotonic clocks production uses
// that equals last-seen order, and a backward clock step could only choose a
// slightly wrong eviction victim, never break the bound. Callers hold q.mu.
func (q *QueryRateLimiter) evictLocked() {
	back := q.order.Back()
	if back == nil {
		return
	}
	q.removeLocked(back.Value.(string))
}

// removeLocked stops one IP's windows and drops it from the map and the
// recency list. Callers hold q.mu.
func (q *QueryRateLimiter) removeLocked(ip string) {
	w := q.windows[ip]
	w.perSecStop()
	w.perHourStop()
	q.order.Remove(w.element)
	delete(q.windows, ip)
}

// newIPWindow builds the two-tier window pair for one IP and records the IP
// at the front of the recency list. Callers hold q.mu.
func (q *QueryRateLimiter) newIPWindow(ip string) *ipWindow {
	newWindow := func() (slidingwindow.Window, slidingwindow.StopFunc) {
		return slidingwindow.NewLocalWindow()
	}

	perSec, perSecStop := slidingwindow.NewLimiter(time.Second, q.perSec, newWindow)
	perHour, perHourStop := slidingwindow.NewLimiter(time.Hour, q.perHour, newWindow)

	w := &ipWindow{
		perSec:      perSec,
		perSecStop:  perSecStop,
		perHour:     perHour,
		perHourStop: perHourStop,
	}
	w.element = q.order.PushFront(ip)
	return w
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
