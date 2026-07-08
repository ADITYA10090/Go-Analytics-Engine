// Command analytics runs the concurrent analytics engine: an HTTP service that
// ingests events on POST /event, aggregates them across a pool of worker
// goroutines, and serves a merged snapshot on GET /stats.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/aditya10090/go-analytics-engine/internal/dispatcher"
	"github.com/aditya10090/go-analytics-engine/internal/httpapi"
)

func main() {
	cfg := loadConfig()

	d := dispatcher.New(cfg.workers, cfg.buffer, cfg.mergeInterval)
	d.Start()

	srv := &http.Server{
		Addr:         cfg.addr,
		Handler:      httpapi.New(d).Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("analytics engine listening on %s (workers=%d buffer=%d merge=%s)",
		cfg.addr, cfg.workers, cfg.buffer, cfg.mergeInterval)

	// Run the HTTP server until a signal arrives.
	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		log.Fatalf("server error: %v", err)
	case sig := <-stop:
		log.Printf("received %s, shutting down gracefully", sig)
	}

	// 1. Stop accepting new HTTP requests and wait for in-flight handlers.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// 2. Drain worker channels and flush the final aggregation.
	final := d.Shutdown()
	log.Printf("drained: total_events=%d unique_users=%d",
		final.TotalEvents, final.UniqueUsersOverall)
}

type config struct {
	addr          string
	workers       int
	buffer        int
	mergeInterval time.Duration
}

// loadConfig reads configuration from flags, each defaulting to an environment
// variable (or a built-in default). Precedence: explicit flag > env var >
// default. See DESIGN.md / README.md for the tuning rationale.
func loadConfig() config {
	defWorkers := envInt("WORKERS", runtime.NumCPU())
	defBuffer := envInt("BUFFER", 1024)
	defAddr := envStr("ADDR", ":8080")
	defMerge := envDuration("MERGE_INTERVAL", time.Second)

	addr := flag.String("addr", defAddr, "listen address (env ADDR)")
	workers := flag.Int("workers", defWorkers, "number of worker goroutines (env WORKERS)")
	buffer := flag.Int("buffer", defBuffer, "per-worker channel buffer size (env BUFFER)")
	merge := flag.Duration("merge-interval", defMerge, "snapshot merge interval (env MERGE_INTERVAL)")
	flag.Parse()

	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "workers must be >= 1")
		os.Exit(2)
	}
	if *buffer < 0 {
		fmt.Fprintln(os.Stderr, "buffer must be >= 0")
		os.Exit(2)
	}
	return config{addr: *addr, workers: *workers, buffer: *buffer, mergeInterval: *merge}
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
