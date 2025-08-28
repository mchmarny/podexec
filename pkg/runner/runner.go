package runner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Run starts the pod execution controller with proper lifecycle management.
// This is the main entry point that orchestrates all controller components
// following Kubernetes controller-runtime patterns for robustness.
func Run() {
	// Set up signal handling for graceful shutdown - essential for cloud-native apps
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,  // Ctrl+C
		syscall.SIGTERM, // Kubernetes pod termination
	)
	defer cancel()

	cmd := NewCommand()
	if cmd == nil {
		die("failed to create command configuration")
	}

	if err := cmd.Validate(); err != nil {
		die("invalid command configuration: %v", err)
	}

	// Initialize structured logging with the configured level
	// Structured logging is crucial for observability in distributed systems
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: cmd.LogLevel,
		// Add source location for debugging in development
		AddSource: cmd.LogLevel <= slog.LevelDebug,
	}))

	// Note: Since Go 1.20, the global rand functions are automatically seeded
	// and safe for concurrent use, so we don't need to manually seed anymore

	// Build Kubernetes client configuration with proper error context
	cfg, err := buildRestConfig(cmd.Kubeconfig, cmd.QPS, cmd.Burst, logger)
	if err != nil {
		die("failed to build kubernetes rest config: %v", err)
	}

	// Create Kubernetes clientset with retry logic built into client-go
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		die("failed to create kubernetes clientset: %v", err)
	}

	// Validate label selector syntax early to fail fast
	_, err = labels.Parse(cmd.LabelSelector)
	if err != nil {
		die("invalid label selector %q: %v", cmd.LabelSelector, err)
	}

	logger.Info("starting pod execution controller",
		"namespace", cmd.Namespace,
		"selector", cmd.LabelSelector,
		"command", cmd.Command,
		"workers", cmd.Workers,
		"timeout", cmd.Timeout.String(),
		"qps", cmd.QPS,
		"burst", cmd.Burst,
	)

	// Run the controller with proper error handling
	if err := runController(ctx, logger, cs, cfg, cmd); err != nil {
		die("controller failed: %v", err)
	}
}

// runController encapsulates the main controller logic with proper error handling.
// Separating this allows for better testing and error management.
func runController(ctx context.Context, logger *slog.Logger, cs *kubernetes.Clientset, cfg *rest.Config, cmd *Command) error {
	// Create ListWatch for pod informer with proper error handling
	lw := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = cmd.LabelSelector
			return cs.CoreV1().Pods(cmd.Namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.LabelSelector = cmd.LabelSelector
			return cs.CoreV1().Pods(cmd.Namespace).Watch(ctx, opts)
		},
	}

	// Create shared informer with proper indexing for efficient lookups
	informer := cache.NewSharedIndexInformer(
		lw,
		&corev1.Pod{},
		cmd.Resync, // Use configured resync period
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	// Create rate-limiting queue to handle pod events
	// This prevents overwhelming the system during mass pod events
	// Using NewTypedRateLimitingQueue instead of deprecated NewRateLimitingQueue
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	defer q.ShutDown()

	// Helper function to enqueue pods that are ready for command execution
	enqueueIfReady := func(obj any) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			logger.Warn("received non-pod object in event handler")
			return
		}

		if !podReady(pod) {
			logger.Debug("skipping non-ready pod", "pod", pod.Name, "phase", pod.Status.Phase)
			return
		}

		key, err := cache.MetaNamespaceKeyFunc(pod)
		if err != nil {
			logger.Warn("failed to generate cache key", "pod", pod.Name, "err", err)
			return
		}

		logger.Debug("enqueueing ready pod", "pod", pod.Name, "key", key)
		q.Add(key)
	}

	// Register event handlers for pod lifecycle events
	var err error
	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: enqueueIfReady,
		UpdateFunc: func(_, newObj any) {
			// Only process update events, not every reconciliation
			enqueueIfReady(newObj)
		},
		// Note: We don't handle DeleteFunc because deleted pods can't execute commands
	})
	if err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}

	// Start the informer and wait for cache sync
	stopCh := make(chan struct{})
	defer close(stopCh)

	logger.Info("starting kubernetes informer")
	go informer.Run(stopCh)

	// Wait for cache to sync before starting workers
	// This ensures we have a consistent view of the cluster state
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return fmt.Errorf("failed to wait for informer cache sync")
	}

	logger.Info("cache synced, starting workers")

	// Start worker goroutines to process the queue
	for i := 0; i < cmd.Workers; i++ {
		logger.Debug("starting worker", "worker_id", i)
		go worker(ctx, logger.With("worker_id", i), cs, cfg, informer.GetIndexer(), q, cmd)
	}

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutdown signal received, draining work queue")

	// Graceful shutdown: wait for queue to drain
	q.ShutDownWithDrain()
	logger.Info("controller shutdown complete")

	return nil
}

// die terminates the program with an error message.
// This follows the Go convention for CLI programs that need to exit with errors.
func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(2)
}
