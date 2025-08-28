package node

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/mchmarny/rolesetter/pkg/log"
	"go.uber.org/zap"
)

// InformNodeRoles initializes and starts the node role setter informer.
func InformNodeRoles() {
	logger := log.GetLogger()
	defer func() { _ = logger.Sync() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
		cancel()
	}()

	// Read environment variables for role label and server port
	roleLabel := os.Getenv("ROLE_LABEL")
	if roleLabel == "" {
		logger.Fatal("environment variable ROLE_LABEL is not set")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		serverPort = "8080" // Default port if not set
	}

	roleLabelReplace := strings.TrimSpace(strings.ToLower(os.Getenv("ROLE_LABEL_REPLACE")))
	replace := roleLabelReplace == "true" || roleLabelReplace == "1" || roleLabelReplace == "yes"

	// parse integer port
	port, err := strconv.Atoi(serverPort)
	if err != nil || port <= 0 {
		logger.Fatal("invalid SERVER_PORT environment variable", zap.Error(err))
	}

	// Create a new informer instance
	inf, err := NewInformer(
		WithLogger(logger),
		WithLabel(roleLabel),
		WithPort(port),
		WithReplace(replace),
	)
	if err != nil {
		logger.Fatal("failed to create informer", zap.Error(err))
	}

	// Run the informer
	if err := inf.Inform(ctx); err != nil {
		logger.Fatal("failed to run informer", zap.Error(err))
	}
}
