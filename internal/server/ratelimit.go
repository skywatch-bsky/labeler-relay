// pattern: Imperative Shell
// ratelimit.go implements the per-client-IP rate limiter guarding the public
// query endpoints (getLabelers/getLabeler). It reuses the two-tier sliding
// window pattern from internal/slurper/ratelimit.go: a per-second window
// against bursts and a per-hour window against sustained flood.

package server

import (
	"container/heap"
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
	// order is a min-heap of (ip, lastSeen-at-push) so capped eviction finds
	// the least recently seen IP without scanning the whole map under the
	// mutex.
	order   lastSeenHeap
	perSec  int64
	perHour int64
	maxIPs  int
	// onRateLimited is called with the endpoint name when a request is
	// rejected. May be nil.
	onRateLimited func(endpoint string)
}

// lastSeenEntry is one heap node: an IP and the lastSeen it had when pushed.
type lastSeenEntry struct {
	ip       string
	lastSeen time.Time
}

// lastSeenHeap is a min-heap over lastSeen. Entries may go stale when an IP
// is touched after being pushed; stale entries are replaced on pop.
type lastSeenHeap []lastSeenEntry

func (h lastSeenHeap) Len() int           { return len(h) }
func (h lastSeenHeap) Less(i, j int) bool { return h[i].lastSeen.Before(h[j].lastSeen) }
func (h lastSeenHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *lastSeenHeap) Push(x any) {
	*h = append(*h, x.(lastSeenEntry))
}

func (h *lastSeenHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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
	q.order = nil
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
		w = q.newIPWindow(ip, now)
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
// entries beyond ipIdleEviction are evicted first; if none is idle, the
// least recently seen entry is dropped. The lastSeen heap orders candidates
// so admission at cap costs O(log n) per pop instead of scanning the whole
// map under the mutex. Heap entries whose key no longer matches the map's
// lastSeen are stale (the IP was touched since being pushed); a stale pop is
// replaced with the current timestamp, keeping one live heap entry per
// tracked IP. Callers hold q.mu.
func (q *QueryRateLimiter) evictLocked(now time.Time) {
	// All idle entries go first.
	for len(q.order) > 0 {
		top := q.order[0]
		w, ok := q.windows[top.ip]
		if ok && now.Sub(w.lastSeen) > ipIdleEviction {
			heap.Pop(&q.order)
			w.perSecStop()
			w.perHourStop()
			delete(q.windows, top.ip)
			continue
		}
		break
	}

	// Still full: drop the least recently seen entry.
	for len(q.windows) >= q.maxIPs && len(q.order) > 0 {
		top := heap.Pop(&q.order).(lastSeenEntry)
		w, ok := q.windows[top.ip]
		if !ok {
			continue // window already gone; discard the stale entry
		}
		if !w.lastSeen.Equal(top.lastSeen) {
			heap.Push(&q.order, lastSeenEntry{ip: top.ip, lastSeen: w.lastSeen})
			continue
		}
		w.perSecStop()
		w.perHourStop()
		delete(q.windows, top.ip)
	}
}

// newIPWindow builds the two-tier window pair for one IP and records the IP
// in the eviction heap at its first-seen timestamp. Callers hold q.mu.
func (q *QueryRateLimiter) newIPWindow(ip string, now time.Time) *ipWindow {
	newWindow := func() (slidingwindow.Window, slidingwindow.StopFunc) {
		return slidingwindow.NewLocalWindow()
	}

	perSec, perSecStop := slidingwindow.NewLimiter(time.Second, q.perSec, newWindow)
	perHour, perHourStop := slidingwindow.NewLimiter(time.Hour, q.perHour, newWindow)

	heap.Push(&q.order, lastSeenEntry{ip: ip, lastSeen: now})

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
