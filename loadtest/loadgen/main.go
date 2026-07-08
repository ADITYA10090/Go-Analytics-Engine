// Command loadgen is a small, self-contained HTTP load generator for
// POST /event. It exists so the benchmark is reproducible without installing
// hey or vegeta (loadtest/run.sh drives hey for the same purpose). It fires a
// fixed event body from a pool of goroutines over keep-alive connections for a
// fixed duration and reports achieved throughput.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/event", "target endpoint")
	conc := flag.Int("c", 50, "number of concurrent connections/goroutines")
	dur := flag.Duration("d", 10*time.Second, "test duration")
	body := flag.String("body", `{"event_type":"click","user_id":"u1","timestamp":0,"properties":{"page":"home"}}`, "request body")
	flag.Parse()

	// One Transport shared across goroutines, tuned for many keep-alive
	// connections to a single host so we measure the server, not connection
	// churn.
	tr := &http.Transport{
		MaxIdleConns:        *conc * 2,
		MaxIdleConnsPerHost: *conc * 2,
		MaxConnsPerHost:     *conc * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	payload := []byte(*body)

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var ok, failed, statusErr uint64
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, *url, bytes.NewReader(payload))
				if err != nil {
					atomic.AddUint64(&failed, 1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					atomic.AddUint64(&failed, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusAccepted {
					atomic.AddUint64(&ok, 1)
				} else {
					atomic.AddUint64(&statusErr, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := ok + failed + statusErr
	rps := float64(ok) / elapsed.Seconds()
	fmt.Fprintf(os.Stdout, "url=%s conc=%d duration=%s\n", *url, *conc, elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stdout, "requests: total=%d ok(202)=%d transport_errors=%d non202=%d\n", total, ok, failed, statusErr)
	fmt.Fprintf(os.Stdout, "throughput: %.0f RPS (accepted only)\n", rps)
}
