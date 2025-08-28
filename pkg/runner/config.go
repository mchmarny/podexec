package runner

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// buildRestConfig constructs a Kubernetes REST config with proper error context and logging.
// The function prioritizes in-cluster config (when running as a pod) but gracefully falls back
// to kubeconfig files. This dual-mode approach is essential for controllers that need to work
// both in development (out-of-cluster) and production (in-cluster) environments.
func buildRestConfig(kubeconfig string, qps float32, burst int, logger *slog.Logger) (*rest.Config, error) {
	// Try in-cluster configuration first - this is the preferred method in production
	// as it automatically handles service account tokens and cluster CA certificates
	if cfg, err := rest.InClusterConfig(); err == nil {
		logger.Info("using in-cluster kubernetes config")

		// Configure rate limiting to prevent overwhelming the API server
		// QPS controls sustained request rate, Burst allows short spikes
		cfg.QPS, cfg.Burst = qps, burst

		// Set reasonable timeouts to prevent hanging connections in distributed environments
		cfg.Timeout = 30 * time.Second

		return cfg, nil
	}

	// Fall back to kubeconfig - essential for development and debugging
	logger.Info("falling back to kubeconfig", "path", kubeconfig)

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build rest config from kubeconfig %q: %w", kubeconfig, err)
	}

	// Apply the same rate limiting and timeout settings
	cfg.QPS, cfg.Burst = qps, burst
	cfg.Timeout = 30 * time.Second

	return cfg, nil
}

// defaultKubeconfig returns the default kubeconfig path using standard Kubernetes conventions.
// This follows the kubectl precedence: KUBECONFIG env var, then ~/.kube/config.
// The function gracefully handles missing home directories (common in containerized environments).
func defaultKubeconfig() string {
	// Respect KUBECONFIG environment variable (supports multiple files separated by colons)
	if p := os.Getenv("KUBECONFIG"); p != "" {
		return p
	}

	// Fall back to standard location
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		// In some container environments, UserHomeDir() might fail
		// Return empty string which will cause buildRestConfig to try in-cluster first
		return ""
	}

	return filepath.Join(home, ".kube", "config")
}
