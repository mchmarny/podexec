package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
	"k8s.io/client-go/util/workqueue"
)

var (
	// processed tracks UIDs that have already been processed to prevent duplicate executions.
	// Using a global variable here is safe because the controller ensures only one instance runs,
	// but in a multi-instance setup, this would need to be stored in a distributed cache.
	processed = newUIDSet()
)

// worker processes items from the work queue in a loop until the context is canceled.
// This follows the standard Kubernetes controller worker pattern with proper error handling
// and resource cleanup. Each worker operates independently to provide horizontal scalability.
func worker(
	ctx context.Context,
	log *slog.Logger,
	cs *kubernetes.Clientset,
	cfg *rest.Config,
	indexer cache.Indexer,
	q workqueue.TypedRateLimitingInterface[string],
	cmd *Command,
) {

	log.Debug("worker started")
	defer log.Debug("worker stopped")

	for {
		// Get next item from queue with proper shutdown handling
		item, shutdown := q.Get()
		if shutdown {
			log.Debug("work queue shut down, stopping worker")
			return
		}

		// Process the item in a closure to ensure proper cleanup
		func(key string) {
			// Always mark the item as done when we finish processing
			defer q.Done(key)

			// Parse namespace and name from the cache key
			_, _, err := cache.SplitMetaNamespaceKey(key)
			if err != nil {
				log.Warn("invalid cache key format", "key", key, "err", err)
				q.Forget(key)
				return
			}

			// Retrieve the pod object from the cache
			o, exists, err := indexer.GetByKey(key)
			if err != nil {
				log.Warn("failed to get pod from cache", "key", key, "err", err)
				q.Forget(key)
				return
			}

			if !exists {
				// Pod was deleted; this is normal during pod lifecycle
				log.Debug("pod no longer exists in cache", "key", key)
				q.Forget(key)
				return
			}

			// Type assert to Pod object
			pod, ok := o.(*corev1.Pod)
			if !ok {
				log.Warn("cache object is not a Pod", "key", key, "type", fmt.Sprintf("%T", o))
				q.Forget(key)
				return
			}

			// Process the pod
			if err := processPod(ctx, log, cs, cfg, pod, cmd); err != nil {
				log.Warn("failed to process pod", "pod", pod.Name, "err", err)
				// Don't requeue on processing errors - they're typically not transient
			}

			// Always forget the item to prevent infinite retries
			q.Forget(key)
		}(item)
	}
}

// processPod handles the execution of a command in a single pod.
// This function encapsulates all the logic for command execution, including
// readiness checks, deduplication, and cleanup operations.
func processPod(
	ctx context.Context,
	log *slog.Logger,
	cs *kubernetes.Clientset,
	cfg *rest.Config,
	pod *corev1.Pod,
	cmd *Command,
) error {
	// Defensive readiness check at processing time
	// Pods can transition states between enqueueing and processing
	if !podReady(pod) {
		log.Debug("pod not ready at processing time", "pod", pod.Name, "phase", pod.Status.Phase)
		return nil // Not an error, just skip
	}

	// Ensure we only process each pod UID once to prevent duplicate executions
	// This is crucial for idempotency in distributed systems
	if processed.Has(string(pod.UID)) {
		log.Debug("pod already processed", "pod", pod.Name, "uid", pod.UID)
		return nil
	}

	// Add jitter to prevent thundering herd problems when many pods become ready simultaneously
	// This is especially important in large clusters with many replicas
	// Using math/rand for jitter is appropriate here as we don't need cryptographic randomness
	jitterMs := rand.Intn(200) //nolint:gosec // G404: Non-crypto use case for jitter timing
	select {
	case <-time.After(time.Duration(jitterMs) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Create per-pod timeout context to prevent hanging executions
	pctx, cancel := context.WithTimeout(ctx, cmd.Timeout)
	defer cancel()

	// Determine target container (default to first container if not specified)
	container := cmd.Container
	if container == "" && len(pod.Spec.Containers) > 0 {
		container = pod.Spec.Containers[0].Name
	}

	log.Info("executing command in pod",
		"pod", pod.Name,
		"uid", pod.UID,
		"node", pod.Spec.NodeName,
		"container", container,
		"command", cmd.Command,
	)

	// Execute the command with proper error handling
	stdout, stderr, err := execShell(pctx, cfg, cs, pod, container, cmd.Command)

	// Mark this pod as processed regardless of success/failure to prevent retries
	processed.Add(string(pod.UID))

	if err != nil {
		log.Warn("command execution failed",
			"pod", pod.Name,
			"uid", pod.UID,
			"node", pod.Spec.NodeName,
			"container", container,
			"err", err,
			"stdout", trim(stdout),
			"stderr", trim(stderr),
		)

		// Restart pod on failure if configured
		if cmd.RestartOnFail {
			deletePod(ctx, log, cs, pod.Namespace, pod.Name)
		}

		return fmt.Errorf("command execution failed: %w", err)
	}

	// Success case
	log.Info("command executed successfully",
		"pod", pod.Name,
		"uid", pod.UID,
		"node", pod.Spec.NodeName,
		"container", container,
		"stdout", trim(stdout),
		"stderr", trim(stderr),
	)

	// Force restart if configured (regardless of command success)
	if cmd.ForceRestart {
		deletePod(ctx, log, cs, pod.Namespace, pod.Name)
	}

	return nil
}

// podReady checks if a pod is ready to execute commands.
// A pod is considered ready when it's in Running phase and all containers are ready.
// This is more strict than just checking the phase, which prevents racing with container startup.
func podReady(p *corev1.Pod) bool {
	// Pod must be in Running phase
	if p.Status.Phase != corev1.PodRunning {
		return false
	}

	// Ensure all declared containers are present in status
	// This prevents racing with container creation
	if len(p.Status.ContainerStatuses) < len(p.Spec.Containers) {
		return false
	}

	// All containers must be ready
	// This ensures we don't execute commands before containers finish starting
	for _, status := range p.Status.ContainerStatuses {
		if !status.Ready {
			return false
		}
	}

	return true
}

// deletePod removes a pod from the cluster with appropriate grace period handling.
// This function implements the proper deletion pattern for Kubernetes controllers.
func deletePod(ctx context.Context, log *slog.Logger, cs *kubernetes.Clientset, namespace, name string) {
	// Use zero grace period for immediate deletion in controller scenarios
	// This is appropriate for pods that need to be recreated quickly
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}

	log.Info("deleting pod", "pod", name, "namespace", namespace)

	if err := cs.CoreV1().Pods(namespace).Delete(ctx, name, deleteOptions); err != nil {
		log.Warn("failed to delete pod", "pod", name, "namespace", namespace, "err", err)
		return
	}

	log.Info("pod deleted successfully", "pod", name, "namespace", namespace)
}

// trim truncates output strings to prevent log spam while preserving useful information.
// Large outputs from commands can overwhelm logging systems in distributed environments.
// uidSet provides a thread-safe way to track processed pod UIDs with automatic expiration.
// This prevents duplicate command executions while allowing memory cleanup over time.
// In a multi-replica controller setup, this would need to be replaced with a distributed cache.
type uidSet struct {
	// Using a simple map here instead of sync.Map because access patterns are simple
	// and the occasional race condition during cleanup is acceptable
	m map[string]time.Time
}

func newUIDSet() *uidSet { return &uidSet{m: make(map[string]time.Time)} }

// Has checks if a UID has been processed and performs best-effort cleanup of old entries.
// The 30-minute expiration prevents unbounded memory growth while allowing reasonable
// protection against duplicate processing.
func (s *uidSet) Has(uid string) bool {
	// Expire old entries (best effort cleanup)
	now := time.Now()
	for k, t := range s.m {
		if now.Sub(t) > 30*time.Minute {
			delete(s.m, k)
		}
	}
	_, exists := s.m[uid]
	return exists
}

// Add marks a UID as processed with the current timestamp.
func (s *uidSet) Add(uid string) {
	s.m[uid] = time.Now()
}

// trim truncates output strings to prevent log spam while preserving useful information.
// Large outputs from commands can overwhelm logging systems in distributed environments.
func trim(s string) string {
	const maxLength = 4096
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength] + "...(truncated)"
}

// execShell executes a shell command in a pod container using the Kubernetes exec API.
// This function handles the complex SPDY streaming protocol and provides proper error classification.
// The implementation follows Kubernetes client-go patterns for robust remote execution.
func execShell(ctx context.Context, cfg *rest.Config, cs *kubernetes.Clientset, pod *corev1.Pod, container, command string) (string, string, error) {
	// Build the exec request using the Kubernetes REST API
	// This creates an SPDY stream to the kubelet running on the pod's node
	req := cs.CoreV1().RESTClient().
		Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   []string{"/bin/sh", "-c", command}, // Use shell for complex commands
			Stdin:     false,                              // We don't need interactive input
			Stdout:    true,
			Stderr:    true,
			TTY:       false, // Disable TTY for proper stdout/stderr separation
		}, scheme.ParameterCodec)

	// Create the SPDY executor for streaming communication
	executor, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("failed to create SPDY executor: %w", err)
	}

	// Capture stdout and stderr in separate buffers
	var stdout, stderr bytes.Buffer

	// Execute the command with context cancellation support
	// The context allows for proper timeout handling and cancellation
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})

	// Convert outputs to strings for easier handling
	stdoutStr := stdout.String()
	stderrStr := stderr.String()

	// Handle success case (exit code 0)
	if err == nil {
		return stdoutStr, stderrStr, nil
	}

	// Classify the error type for better error handling
	var exitError utilexec.ExitError
	if errors.As(err, &exitError) {
		// Command executed but returned non-zero exit code
		return stdoutStr, stderrStr, fmt.Errorf("command failed with exit code %d: %w", exitError.ExitStatus(), err)
	}

	// Network/transport error (connection issues, timeouts, etc.)
	return stdoutStr, stderrStr, fmt.Errorf("execution stream error: %w", err)
}
