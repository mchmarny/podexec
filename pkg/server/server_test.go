package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mchmarny/rolesetter/pkg/log"
)

func TestBuildHandler_HealthAndReadyEndpoints(t *testing.T) {
	logger := log.GetTestLogger()
	srv := &server{logger: logger, port: 8080}
	handler := srv.buildHandler(nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	endpoints := []string{"/healthz", "/readyz", "/"}
	for _, ep := range endpoints {
		resp, err := http.Get(ts.URL + ep)
		if err != nil {
			t.Fatalf("failed to GET %s: %v", ep, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK for %s, got %d", ep, resp.StatusCode)
		}
	}
}

func TestBuildHandler_RegistersMetricsHandler(t *testing.T) {
	logger := log.GetTestLogger()
	srv := &server{logger: logger, port: 8080}
	metricsCalled := false
	metricsHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metricsCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := srv.buildHandler(map[string]http.Handler{"/metrics": metricsHandler})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("failed to GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /metrics, got %d", resp.StatusCode)
	}
	if !metricsCalled {
		t.Error("metrics handler was not called")
	}
}

func TestWithLogger_SetsLogger(t *testing.T) {
	s := &server{}
	l := log.GetTestLogger()
	WithLogger(l)(s)
	if s.logger != l {
		t.Error("WithLogger did not set logger")
	}
}

func TestWithPort_SetsPort(t *testing.T) {
	s := &server{}
	WithPort(1234)(s)
	if s.port != 1234 {
		t.Error("WithPort did not set port")
	}
}

func TestNewServer_DefaultsAndOptions(t *testing.T) {
	s := NewServer()
	if s == nil {
		t.Error("NewServer returned nil")
	}
	custom := NewServer(WithLogger(log.GetTestLogger()), WithPort(4321))
	// Type assertion to access fields
	impl, ok := custom.(*server)
	if !ok {
		t.Error("NewServer did not return *server type")
	}
	if impl.port != 4321 {
		t.Errorf("expected port 4321, got %d", impl.port)
	}
}
