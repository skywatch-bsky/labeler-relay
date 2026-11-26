// pattern: Imperative Shell

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/scarndp/labeler-relay/internal/admin"
	"github.com/scarndp/labeler-relay/internal/config"
	"github.com/scarndp/labeler-relay/internal/firehose"
	"github.com/scarndp/labeler-relay/internal/metrics"
	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/slurper"
	"github.com/scarndp/labeler-relay/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintln(os.Stderr, "labeler-relay:", err)
		os.Exit(1)
	}
}

// run wires and starts the full labeler-relay process. It returns when ctx is
// cancelled (graceful shutdown) or a component returns a fatal error.
func run(ctx context.Context) error {
	log := slog.Default()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer st.Close()

	persist := store.NewLabelPersist(st)
	registry := store.NewLabelerRegistry(st)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		return fmt.Errorf("failed to register metrics: %w", err)
	}

	// Wire the HeadSeq gauge via callback (FCIS: store doesn't import metrics).
	persist.SetHeadSeqCallback(func(seq float64) {
		metrics.HeadSeq.Set(seq)
	})

	hub := server.NewHub()
	persist.SetBroadcaster(hub.Broadcast)

	sl := slurper.New(
		registry,
		persist,
		cfg.RequireSig,
		slurper.LimitConfig{
			PerSec:  cfg.UpstreamRateLimit.PerSec,
			PerHour: cfg.UpstreamRateLimit.PerHour,
		},
		log,
	)
	sl.SetUpstreamsCallback(func(n float64) {
		metrics.ConnectedUpstreams.Set(n)
	})
	sl.SetThrottledCallback(func(did string) {
		metrics.Throttled.WithLabelValues(did).Inc()
	})
	sl.SetDropUnsignedCallback(func(did string) {
		metrics.DroppedUnsigned.WithLabelValues(did).Inc()
	})
	sl.SetIngestedCallback(func(did string, n int) {
		metrics.IngestedTotal.WithLabelValues(did).Add(float64(n))
	})

	// identity.BaseDirectory{} is the zero
	// value and is directly usable as a Resolver (live HTTP DID resolution).
	baseDir := &identity.BaseDirectory{}
	resolver := firehose.NewIndigoResolver(baseDir)
	fw := firehose.NewFirehoseWatcher(
		cfg.FirehoseURL,
		registry,
		persist,
		resolver,
		st,
		sl.Poke,
		cfg.AutoSubscribeDiscovered,
		log,
	)

	adminAPI := admin.NewAPI(registry, resolver, sl.Poke, cfg.AdminToken)

	retentionSecs := int64(cfg.RetentionWindow / time.Second)
	srv := server.NewServer(hub, persist, registry, log, retentionSecs, cfg.SubscriberBufSize)
	srv.SetConnectCallback(func() {
		metrics.ConsumerCount.Inc()
	})
	srv.SetDisconnectCallback(func() {
		metrics.ConsumerCount.Dec()
	})

	mux := buildMux(srv, cfg.QueryRateLimit.PerSec, cfg.QueryRateLimit.PerHour, cfg.TrustedProxies, reg, adminAPI)

	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
		// Slowloris protection for the admin/metrics/health endpoints. The
		// subscribeLabelers WebSocket is unaffected: hijacked connections are
		// not subject to server timeouts.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	pruneJob := store.NewPruneJob(
		persist,
		cfg.RetentionWindow,
		10*time.Minute,
		nil, // use real clock
		log,
	)
	pruneJob.SetFloorCallback(func(floor float64) {
		metrics.RetentionFloorGauge.Set(floor)
	})

	g, gctx := errgroup.WithContext(ctx)

	// HTTP server lifecycle: start in background, shut down gracefully on
	// context cancellation.
	g.Go(func() error {
		log.Info("listening", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Info("shutting down http server")
		return httpServer.Shutdown(shutCtx)
	})

	g.Go(func() error {
		if err := sl.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("slurper: %w", err)
		}
		return nil
	})

	// Subscription goroutines hang off the slurper's own root context, not
	// gctx, so they must be cancelled explicitly on shutdown.
	g.Go(func() error {
		<-gctx.Done()
		sl.Shutdown()
		return nil
	})

	// Firehose watcher.
	g.Go(func() error {
		if err := fw.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("firehose watcher: %w", err)
		}
		return nil
	})

	// Prune job.
	g.Go(func() error {
		if err := pruneJob.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("prune job: %w", err)
		}
		return nil
	})

	// Initial reconcile to pick up any labelers already in the registry.
	g.Go(func() error {
		if err := sl.Reconcile(gctx); err != nil && err != context.Canceled {
			log.Error("initial reconcile failed", "err", err)
		}
		return nil
	})

	return g.Wait()
}
