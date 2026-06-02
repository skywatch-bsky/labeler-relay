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

	// Step 1: Load config from environment.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}

	// Step 2: Open the SQLite store (single store backing both persist and registry).
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer st.Close()

	persist := store.NewLabelPersist(st)
	registry := store.NewLabelerRegistry(st)

	// Step 3: Build the metrics registry and register all metrics.
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		return fmt.Errorf("failed to register metrics: %w", err)
	}

	// Wire the HeadSeq gauge via callback (FCIS: store doesn't import metrics).
	persist.SetHeadSeqCallback(func(seq float64) {
		metrics.HeadSeq.Set(seq)
	})

	// Step 4: Build the Hub and register it as the broadcaster.
	hub := server.NewHub()
	persist.SetBroadcaster(hub.Broadcast)

	// Step 5: Build the slurper and wire upstreams gauge.
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

	// poke triggers an immediate Reconcile on the slurper (used by firehose
	// watcher and admin API to react to registry changes without waiting for
	// the next ticker interval).
	poke := func() {
		go func() {
			if err := sl.Reconcile(ctx); err != nil && err != context.Canceled {
				log.Error("poke reconcile failed", "err", err)
			}
		}()
	}

	// Step 6: Build the firehose watcher. identity.BaseDirectory{} is the zero
	// value and is directly usable as a Resolver (live HTTP DID resolution).
	baseDir := &identity.BaseDirectory{}
	resolver := firehose.NewIndigoResolver(baseDir)
	fw := firehose.NewFirehoseWatcher(
		cfg.FirehoseURL,
		registry,
		persist,
		resolver,
		st,
		poke,
		log,
	)

	// Step 7: Build the admin API.
	adminAPI := admin.NewAPI(registry, resolver, poke, cfg.AdminToken)

	// Step 8: Build the HTTP server.
	retentionSecs := int64(cfg.RetentionWindow / time.Second)
	srv := server.NewServer(hub, persist, registry, log, retentionSecs)

	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/community.labeler.sync.subscribeLabelers", srv.HandleSubscribeLabelers)
	mux.HandleFunc("/_health", srv.HandleHealth)
	mux.Handle("/metrics", server.MetricsHandler(reg))

	// Mount admin routes.
	mux.Handle("/admin/", adminAPI.Routes())

	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	// Step 9: Build the prune job and wire retention-floor gauge.
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

	// Step 10: Start all background goroutines under an errgroup.
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

	// Slurper: reconcile loop.
	g.Go(func() error {
		if err := sl.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("slurper: %w", err)
		}
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
