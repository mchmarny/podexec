package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/mchmarny/rolesetter/pkg/log"
	"go.uber.org/zap"
)

// Server defines the interface for the server that handles metrics and health checks.
type Server interface {
	Serve(ctx context.Context, handlers map[string]http.Handler)
}

// Option is a functional option for configuring Server.
type Option func(*server)

// WithLogger sets the logger for the server.
func WithLogger(logger *zap.Logger) Option {
	return func(s *server) {
		s.logger = logger
	}
}

// WithPort sets the port for the server.
func WithPort(port int) Option {
	return func(s *server) {
		s.port = port
	}
}

// NewServer creates a new Server instance with the provided options.
func NewServer(opts ...Option) Server {
	s := &server{
		logger: log.GetLogger(), // default logger
		port:   8080,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.logger.Info("server initialized", zap.Int("port", s.port))
	return s
}

type server struct {
	logger *zap.Logger
	port   int
}

// Serve initializes and starts the HTTP server for metrics and health checks.
func (s *server) Serve(ctx context.Context, handlers map[string]http.Handler) {
	handler := s.buildHandler(handlers)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.logger.Info("starting metrics server", zap.Int("port", s.port))

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.logger.Fatal("failed to start metrics server", zap.Error(err))
	}
}

// buildHandler constructs the HTTP handler mux for the server.
func (s *server) buildHandler(handlers map[string]http.Handler) http.Handler {
	mux := http.NewServeMux()

	okFunc := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	mux.HandleFunc("/healthz", okFunc)
	mux.HandleFunc("/readyz", okFunc)
	mux.HandleFunc("/", okFunc)

	for path, handler := range handlers {
		mux.Handle(path, handler)
		s.logger.Info("registered handler", zap.String("path", path))
	}

	return mux
}
