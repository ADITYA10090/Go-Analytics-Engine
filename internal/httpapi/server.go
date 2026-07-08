// Package httpapi exposes the engine over stdlib net/http — no framework, so
// the whole request path fits in one file and can be read top to bottom.
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/aditya10090/go-analytics-engine/internal/aggregator"
	"github.com/aditya10090/go-analytics-engine/internal/dispatcher"
)

// maxBodyBytes caps request bodies so a client cannot make a worker allocate
// unboundedly while decoding a single event.
const maxBodyBytes = 1 << 16 // 64 KiB

// Server adapts a Dispatcher to HTTP handlers.
type Server struct {
	dispatcher *dispatcher.Dispatcher
}

// New returns a Server backed by the given dispatcher.
func New(d *dispatcher.Dispatcher) *Server {
	return &Server{dispatcher: d}
}

// Handler returns the routed http.Handler for the service.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/event", s.handleEvent)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

// handleEvent decodes one event and hands it to the dispatcher. It returns
// 202 Accepted because ingestion is asynchronous: the event is queued for a
// worker, not yet reflected in /stats until the next merge.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var ev aggregator.Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ev.EventType == "" {
		http.Error(w, "event_type is required", http.StatusBadRequest)
		return
	}

	if err := s.dispatcher.Dispatch(ev); err != nil {
		// The only error today is shutdown-in-progress.
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// handleStats returns the current merged snapshot.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := s.dispatcher.Stats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
