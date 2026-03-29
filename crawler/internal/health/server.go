package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Status represents the health status of a component
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
	StatusDegraded  Status = "degraded"
)

// ComponentHealth represents health of a single component
type ComponentHealth struct {
	Status  Status `json:"status"`
	Message string `json:"message,omitempty"`
}

// HealthResponse is the overall health check response
type HealthResponse struct {
	Status     Status                     `json:"status"`
	Timestamp  time.Time                  `json:"timestamp"`
	Components map[string]ComponentHealth `json:"components"`
}

// Checker defines a health check function
type Checker func(ctx context.Context) ComponentHealth

// Server provides HTTP health check endpoints
type Server struct {
	port     int
	checkers map[string]Checker
	mu       sync.RWMutex
	server   *http.Server
}

// NewServer creates a health check server
func NewServer(port int) *Server {
	return &Server{
		port:     port,
		checkers: make(map[string]Checker),
	}
}

// RegisterChecker adds a health checker for a component
func (s *Server) RegisterChecker(name string, checker Checker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkers[name] = checker
}

// Start begins listening for health check requests
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleLiveness)
	mux.HandleFunc("/health/ready", s.handleReadiness)
	mux.HandleFunc("/metrics", s.handleMetricsStub)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the health server
func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *Server) checkAll(ctx context.Context) HealthResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()

	response := HealthResponse{
		Status:     StatusHealthy,
		Timestamp:  time.Now().UTC(),
		Components: make(map[string]ComponentHealth),
	}

	for name, checker := range s.checkers {
		health := checker(ctx)
		response.Components[name] = health

		if health.Status == StatusUnhealthy {
			response.Status = StatusUnhealthy
		} else if health.Status == StatusDegraded && response.Status != StatusUnhealthy {
			response.Status = StatusDegraded
		}
	}

	return response
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response := s.checkAll(ctx)

	w.Header().Set("Content-Type", "application/json")
	if response.Status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "alive",
	})
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response := s.checkAll(ctx)

	w.Header().Set("Content-Type", "application/json")
	if response.Status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleMetricsStub(w http.ResponseWriter, r *http.Request) {
	// Basic metrics stub - can be expanded with prometheus later
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "# HELP fetcher_up Whether the fetcher is running\n")
	fmt.Fprintf(w, "# TYPE fetcher_up gauge\n")
	fmt.Fprintf(w, "fetcher_up 1\n")
}

// KafkaChecker creates a health checker for Kafka connectivity
func KafkaChecker(pingFunc func(ctx context.Context) error) Checker {
	return func(ctx context.Context) ComponentHealth {
		if err := pingFunc(ctx); err != nil {
			return ComponentHealth{
				Status:  StatusUnhealthy,
				Message: fmt.Sprintf("kafka: %v", err),
			}
		}
		return ComponentHealth{Status: StatusHealthy}
	}
}

// ValkeyChecker creates a health checker for Valkey/Redis
func ValkeyChecker(pingFunc func(ctx context.Context) error) Checker {
	return func(ctx context.Context) ComponentHealth {
		if err := pingFunc(ctx); err != nil {
			return ComponentHealth{
				Status:  StatusDegraded,
				Message: fmt.Sprintf("valkey: %v", err),
			}
		}
		return ComponentHealth{Status: StatusHealthy}
	}
}

// S3Checker creates a health checker for S3/MinIO
func S3Checker(pingFunc func(ctx context.Context) error) Checker {
	return func(ctx context.Context) ComponentHealth {
		if err := pingFunc(ctx); err != nil {
			return ComponentHealth{
				Status:  StatusDegraded,
				Message: fmt.Sprintf("s3: %v", err),
			}
		}
		return ComponentHealth{Status: StatusHealthy}
	}
}
