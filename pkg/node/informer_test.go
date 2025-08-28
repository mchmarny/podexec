package node

import (
	"context"
	"testing"

	"github.com/mchmarny/rolesetter/pkg/log"
	"github.com/mchmarny/rolesetter/pkg/server"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInformer_Validate(t *testing.T) {
	logger := log.GetTestLogger()
	clientset := fake.NewSimpleClientset()
	srv := server.NewServer(
		server.WithLogger(logger),
		server.WithPort(8080),
	)

	inf := &Informer{
		logger:    logger,
		label:     "test-label",
		port:      8080,
		clientset: clientset,
		server:    srv,
	}
	if err := inf.validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInformer_Inform_ContextCancel(t *testing.T) {
	logger := log.GetTestLogger()
	clientset := fake.NewSimpleClientset()
	srv := server.NewServer(
		server.WithLogger(logger),
		server.WithPort(8080),
	)

	inf := &Informer{
		logger:    logger,
		label:     "test-label",
		port:      8080,
		clientset: clientset,
		server:    srv,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err := inf.Inform(ctx)
	if err == nil {
		t.Error("expected error due to context cancellation")
	}
}

func TestWithReplace_SetsReplace(t *testing.T) {
	i := &Informer{}
	WithReplace(true)(i)
	if !i.replace {
		t.Error("WithReplace did not set replace to true")
	}
}

func TestWithLogger_SetsLogger(t *testing.T) {
	l := log.GetTestLogger()
	i := &Informer{}
	WithLogger(l)(i)
	if i.logger != l {
		t.Error("WithLogger did not set logger")
	}
}

func TestWithLabel_SetsLabel(t *testing.T) {
	label := "foo"
	i := &Informer{}
	WithLabel(label)(i)
	if i.label != label {
		t.Error("WithLabel did not set label")
	}
}

func TestWithPort_SetsPort(t *testing.T) {
	port := 1234
	i := &Informer{}
	WithPort(port)(i)
	if i.port != port {
		t.Error("WithPort did not set port")
	}
}

func TestValidate_Errors(t *testing.T) {
	i := &Informer{}
	if err := i.validate(); err == nil {
		t.Error("expected error for missing fields")
	}
	i.logger = log.GetTestLogger()
	if err := i.validate(); err == nil {
		t.Error("expected error for missing label")
	}
	i.label = "foo"
	if err := i.validate(); err == nil {
		t.Error("expected error for missing port")
	}
	i.port = 1234
	if err := i.validate(); err == nil {
		t.Error("expected error for missing clientset")
	}
}
