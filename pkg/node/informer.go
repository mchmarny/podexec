package node

import (
	context "context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/mchmarny/rolesetter/pkg/log"
	"github.com/mchmarny/rolesetter/pkg/metric"
	"github.com/mchmarny/rolesetter/pkg/role"
	"github.com/mchmarny/rolesetter/pkg/server"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	resSyncSeconds     = 30
	servicePortDefault = 8080
)

// Informer is responsible for managing the node role setter controller.
type Informer struct {
	logger    *zap.Logger
	label     string
	replace   bool
	port      int
	clientset kubernetes.Interface
	server    server.Server
}

// Option is a functional option for configuring Informer.
type Option func(*Informer)

func WithReplace(replace bool) Option {
	return func(i *Informer) {
		i.replace = replace
	}
}

// WithLogger sets the logger for the Informer.
func WithLogger(logger *zap.Logger) Option {
	return func(i *Informer) {
		i.logger = logger
	}
}

// WithLabel sets the label for the Informer.
func WithLabel(label string) Option {
	return func(i *Informer) {
		i.label = label
	}
}

// WithPort sets the port for the Informer.
func WithPort(port int) Option {
	return func(i *Informer) {
		i.port = port
	}
}

// WithClientset sets the Kubernetes clientset for the Informer.
func WithClientset(cs kubernetes.Interface) Option {
	return func(i *Informer) {
		i.clientset = cs
	}
}

// NewInformer creates a new Informer instance using functional options.
func NewInformer(opts ...Option) (*Informer, error) {
	i := &Informer{
		logger: log.GetLogger(),
		port:   servicePortDefault,
	}

	for _, opt := range opts {
		opt(i)
	}

	// set these AFTER options are applied to allow testing to override
	if i.clientset == nil {
		cs, err := newClient()
		if err != nil {
			return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
		}
		i.clientset = cs
	}

	if i.server == nil {
		i.server = server.NewServer(
			server.WithLogger(i.logger),
			server.WithPort(i.port),
		)
	}

	// still validate the Informer configuration
	if err := i.validate(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}
	return i, nil
}

// Validate checks if the Informer has valid configuration.
func (i *Informer) validate() error {
	if i.logger == nil {
		return fmt.Errorf("logger must not be nil")
	}
	if i.label == "" {
		return fmt.Errorf("roleLabel must be specified")
	}
	if i.port <= 0 {
		return fmt.Errorf("serverPort must be a positive integer")
	}
	if i.clientset == nil {
		return fmt.Errorf("kubernetes clientset must not be nil")
	}
	if i.server == nil {
		return fmt.Errorf("server must not be nil")
	}
	return nil
}

// Inform runs the node role setter controller with context, logger, and config.
func (i *Informer) Inform(ctx context.Context) error {
	if err := i.validate(); err != nil {
		return fmt.Errorf("validation error: %w", err)
	}

	i.logger.Info("starting node role setter",
		zap.String("label", i.label),
		zap.Int("port", i.port),
	)

	patcher := i.clientset.CoreV1().Nodes().Patch
	factory := informers.NewSharedInformerFactory(i.clientset, resSyncSeconds*time.Second)
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			(&role.CacheResourceHandler{
				Patcher:   patcher,
				Logger:    i.logger,
				Replace:   i.replace,
				RoleLabel: i.label,
			}).EnsureRole(obj)
		},
		UpdateFunc: func(_, newObj interface{}) {
			(&role.CacheResourceHandler{
				Patcher:   patcher,
				Logger:    i.logger,
				Replace:   i.replace,
				RoleLabel: i.label,
			}).EnsureRole(newObj)
		},
		DeleteFunc: func(_ interface{}) {
			// nothing to do here
		},
	}

	// Create the informer for Nodes
	inf := factory.Core().V1().Nodes().Informer()
	if _, err := inf.AddEventHandler(handler); err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}

	// Start the informer and metrics server
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i.server.Serve(ctx, map[string]http.Handler{
			"/metrics": metric.GetHandler(),
		})
	}()

	// Start the informer factory and wait for cache sync
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		return fmt.Errorf("cache sync failed")
	}
	<-ctx.Done()
	wg.Wait() // Wait for metrics server goroutine to exit
	return nil
}
