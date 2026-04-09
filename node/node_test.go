package node

import (
	"errors"
	"testing"
)

type mockService struct {
	started   bool
	stopped   bool
	failStart bool
}

func (s *mockService) Start() error {
	if s.failStart {
		return errors.New("start failed")
	}
	s.started = true
	return nil
}

func (s *mockService) Stop() error {
	s.stopped = true
	return nil
}

func TestNodeStartStop(t *testing.T) {
	cfg := &Config{DataDir: t.TempDir()}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	svc := &mockService{}
	n.RegisterLifecycle(svc)

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if !svc.started {
		t.Fatal("service should be started")
	}

	n.Stop()
	if !svc.stopped {
		t.Fatal("service should be stopped")
	}
}

func TestNodeStartFailure(t *testing.T) {
	cfg := &Config{DataDir: t.TempDir()}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	good := &mockService{}
	bad := &mockService{failStart: true}
	n.RegisterLifecycle(good)
	n.RegisterLifecycle(bad)

	if err := n.Start(); err == nil {
		t.Fatal("should fail when service fails to start")
	}
	if !good.stopped {
		t.Fatal("previously started service should be stopped on failure")
	}
}
