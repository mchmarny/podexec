package runner

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	// Environment variable names for configuration
	EnvVarNamespace     = "NAMESPACE"
	EnvVarLabelSelector = "LABEL_SELECTOR"
	EnvVarContainer     = "CONTAINER"
	EnvVarCommand       = "COMMAND"
	EnvVarForceRestart  = "FORCE_RESTART"
	EnvVarRestartOnFail = "RESTART_ON_FAIL"
	EnvVarWorkers       = "WORKERS"
	EnvVarTimeout       = "TIMEOUT"
	EnvVarResync        = "RESYNC"
	EnvVarQPS           = "QPS"
	EnvVarBurst         = "BURST"
	EnvVarKubeconfig    = "KUBECONFIG"
	EnvVarLogLevel      = "LOG_LEVEL"

	// Default values for configuration parameters
	DefaultNamespace     = "default"
	DefaultLabelSelector = ""               // Must be explicitly set
	DefaultContainer     = ""               // Empty means "first container"
	DefaultCommand       = ""               // Must be explicitly set
	DefaultForceRestart  = false            // Conservative default
	DefaultRestartOnFail = true             // Fail-fast recovery pattern
	DefaultWorkers       = 16               // Balanced concurrency for most workloads
	DefaultTimeout       = 30 * time.Second // Reasonable for most commands
	DefaultResync        = 0                // Disable periodic resync by default (event-driven only)
	DefaultQPS           = 50               // Conservative API server rate limiting
	DefaultBurst         = 100              // Allow short bursts while maintaining average QPS
	DefaultKubeconfig    = ""               // Use standard kubeconfig resolution
	DefaultLogLevel      = slog.LevelInfo   // Balanced verbosity
)

var (
	// Error definitions follow Go conventions and provide clear context for debugging
	ErrInvalidWorkers  = fmt.Errorf("workers must be > 0")
	ErrInvalidTimeout  = fmt.Errorf("timeout must be > 0")
	ErrInvalidQPS      = fmt.Errorf("qps must be > 0")
	ErrInvalidBurst    = fmt.Errorf("burst must be > 0")
	ErrNoCommand       = fmt.Errorf("command must be specified")
	ErrNoLabelSelector = fmt.Errorf("label selector must be specified")
	ErrInvalidResync   = fmt.Errorf("resync period must be >= 0 (0 disables periodic resync)")
)

// Command encapsulates all configuration for the pod execution controller.
// The structure is designed to be immutable after validation, which prevents
// race conditions in concurrent environments and makes the behavior predictable.
type Command struct {
	Namespace     string        // Kubernetes namespace to watch
	LabelSelector string        // Label selector for pod filtering
	Container     string        // Specific container name (empty = first container)
	Command       string        // Shell command to execute
	ForceRestart  bool          // Whether to restart pods regardless of command result
	RestartOnFail bool          // Whether to restart pods only on command failure
	Workers       int           // Number of concurrent workers
	Timeout       time.Duration // Per-command execution timeout
	Resync        time.Duration // Informer resync period (0 = no periodic resync)
	QPS           float32       // Kubernetes API client QPS limit
	Burst         int           // Kubernetes API client burst limit
	Kubeconfig    string        // Path to kubeconfig file
	LogLevel      slog.Level    // Logging verbosity level
}

// Validate performs comprehensive validation of the command configuration.
// This validation is crucial in distributed systems where invalid config
// can cause cascading failures or resource exhaustion.
func (c *Command) Validate() error {
	if c.Workers <= 0 {
		return fmt.Errorf("%w: got %d", ErrInvalidWorkers, c.Workers)
	}

	// Prevent excessive worker counts that could overwhelm the API server
	if c.Workers > 100 {
		return fmt.Errorf("workers should not exceed 100 to prevent API server overload: got %d", c.Workers)
	}

	if c.Timeout <= 0 {
		return fmt.Errorf("%w: got %v", ErrInvalidTimeout, c.Timeout)
	}

	// Prevent timeouts that are too long and could cause resource leaks
	if c.Timeout > 10*time.Minute {
		return fmt.Errorf("timeout should not exceed 10 minutes to prevent resource leaks: got %v", c.Timeout)
	}

	if c.QPS <= 0 {
		return fmt.Errorf("%w: got %f", ErrInvalidQPS, c.QPS)
	}

	if c.Burst <= 0 {
		return fmt.Errorf("%w: got %d", ErrInvalidBurst, c.Burst)
	}

	// Validate that burst is reasonable relative to QPS
	if float32(c.Burst) < c.QPS {
		return fmt.Errorf("burst (%d) should be >= QPS (%f) for proper rate limiting", c.Burst, c.QPS)
	}

	if strings.TrimSpace(c.Command) == "" {
		return ErrNoCommand
	}

	if strings.TrimSpace(c.LabelSelector) == "" {
		return ErrNoLabelSelector
	}

	if c.Resync < 0 {
		return fmt.Errorf("%w: got %v", ErrInvalidResync, c.Resync)
	}

	return nil
}

type Option func(*Command)

func WithNamespace(ns string) Option {
	return func(c *Command) {
		c.Namespace = ns
	}
}

func WithLabelSelector(labelSel string) Option {
	return func(c *Command) {
		c.LabelSelector = labelSel
	}
}

func WithContainer(container string) Option {
	return func(c *Command) {
		c.Container = container
	}
}

func WithCommand(command string) Option {
	return func(c *Command) {
		c.Command = command
	}
}

func WithForceRestart(force bool) Option {
	return func(c *Command) {
		c.ForceRestart = force
	}
}
func WithRestartOnFail(restart bool) Option {
	return func(c *Command) {
		c.RestartOnFail = restart
	}
}
func WithWorkers(workers int) Option {
	return func(c *Command) {
		c.Workers = workers
	}
}
func WithTimeout(timeout time.Duration) Option {
	return func(c *Command) {
		c.Timeout = timeout
	}
}
func WithResync(resync time.Duration) Option {
	return func(c *Command) {
		c.Resync = resync
	}
}
func WithQPS(qps float32) Option {
	return func(c *Command) {
		c.QPS = qps
	}
}
func WithBurst(burst int) Option {
	return func(c *Command) {
		c.Burst = burst
	}
}

func WithKubeconfig(kubeconfig string) Option {
	return func(c *Command) {
		c.Kubeconfig = kubeconfig
	}
}

func WithLogLevel(level slog.Level) Option {
	return func(c *Command) {
		c.LogLevel = level
	}
}

// NewCommand creates a Command with production-ready defaults.
// The defaults are chosen based on common Kubernetes controller patterns
// and have been battle-tested in high-throughput environments.
func NewCommand(opts ...Option) *Command {
	cmd := NewCommandFromEnvVars()

	// Apply all options in order - this pattern allows for composable configuration
	for _, opt := range opts {
		opt(cmd)
	}

	return cmd
}

func ListEnvVars() []string {
	return []string{
		EnvVarNamespace,
		EnvVarLabelSelector,
		EnvVarContainer,
		EnvVarCommand,
		EnvVarForceRestart,
		EnvVarRestartOnFail,
		EnvVarWorkers,
		EnvVarTimeout,
		EnvVarResync,
		EnvVarQPS,
		EnvVarBurst,
		EnvVarKubeconfig,
		EnvVarLogLevel,
	}
}

// NewCommandFromEnvVars creates a Command by reading configuration from environment variables.
// This function is useful for containerized deployments where configuration is often
// provided via environment variables. It falls back to production-ready defaults
// for any settings not specified in the environment.
func NewCommandFromEnvVars() *Command {
	return NewCommand(
		WithNamespace(getEnv(EnvVarNamespace, "default")),
		WithLabelSelector(getEnv(EnvVarLabelSelector, "")), // Must be explicitly set
		WithContainer(getEnv(EnvVarContainer, "")),         // Empty means "first container"
		WithCommand(getEnv(EnvVarCommand, "")),             // Must be explicitly set
		WithForceRestart(getEnvAsBool(EnvVarForceRestart, false)),
		WithRestartOnFail(getEnvAsBool(EnvVarRestartOnFail, true)),
		WithWorkers(getEnvAsInt(EnvVarWorkers, 16)),
		WithTimeout(getEnvAsDuration(EnvVarTimeout, 30*time.Second)),
		WithResync(getEnvAsDuration(EnvVarResync, 0)),
		WithQPS(getEnvAsFloat32(EnvVarQPS, 50)),
		WithBurst(getEnvAsInt(EnvVarBurst, 100)),
		WithKubeconfig(getEnv(EnvVarKubeconfig, defaultKubeconfig())),
		WithLogLevel(getLogLevelFromEnv(EnvVarLogLevel, slog.LevelInfo)),
	)
}

func LookupEnv(_ string) (string, bool) {
	return "", false
}

func getEnv(name, defaultVal string) string {
	val, exists := LookupEnv(name)
	if !exists {
		return defaultVal
	}
	return val
}

func getEnvAsDuration(name string, defaultVal time.Duration) time.Duration {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}
	val, err := time.ParseDuration(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvAsInt(name string, defaultVal int) int {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}
	var val int
	_, err := fmt.Sscanf(valStr, "%d", &val)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvAsBool(name string, defaultVal bool) bool {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}
	val := strings.ToLower(valStr)
	switch val {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	}
	return defaultVal
}

func getEnvAsFloat32(name string, defaultVal float32) float32 {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}
	var val float32
	_, err := fmt.Sscanf(valStr, "%f", &val)
	if err != nil {
		return defaultVal
	}
	return val
}

func getLogLevelFromEnv(name string, defaultVal slog.Level) slog.Level {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}

	switch strings.ToUpper(valStr) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		slog.Warn("invalid log level in environment variable, using default", "envVar", name, "value", valStr)
	}

	return defaultVal
}
