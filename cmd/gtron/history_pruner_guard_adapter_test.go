package main

import (
	"context"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
)

func TestDomainPrunerGuardAdapterFailsClosed(t *testing.T) {
	for _, adapter := range []*domainPrunerChainSource{nil, {}, {prunerChainSource: &prunerChainSource{}}, {prunerChainSource: &prunerChainSource{chain: &core.BlockChain{}}}} {
		called := false
		ran, err := adapter.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, common.Hash{1}, func() error { called = true; return nil })
		if ran || err == nil || called {
			t.Fatalf("unavailable adapter ran=%v err=%v called=%v", ran, err, called)
		}
	}
	adapter := &domainPrunerChainSource{prunerChainSource: &prunerChainSource{chain: &core.BlockChain{}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ran, err := adapter.TryWithStateDomainChangePruneGuard(ctx, 2, 4, common.Hash{1}, func() error { return nil }); ran || !errors.Is(err, context.Canceled) {
		t.Fatalf("context not forwarded: ran=%v err=%v", ran, err)
	}
}

func TestDomainPrunerQueuedGuardAdapterFailsClosed(t *testing.T) {
	for _, adapter := range []*domainPrunerChainSource{nil, {}, {prunerChainSource: &prunerChainSource{}}, {prunerChainSource: &prunerChainSource{chain: &core.BlockChain{}}}} {
		called := false
		ran, err := adapter.WithStateDomainChangePruneGuard(context.Background(), 2, 4, common.Hash{1}, func() error { called = true; return nil })
		if ran || err == nil || called {
			t.Fatalf("unavailable queued adapter ran=%v err=%v called=%v", ran, err, called)
		}
	}
	adapter := &domainPrunerChainSource{prunerChainSource: &prunerChainSource{chain: &core.BlockChain{}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ran, err := adapter.WithStateDomainChangePruneGuard(ctx, 2, 4, common.Hash{1}, func() error { return nil }); ran || !errors.Is(err, context.Canceled) {
		t.Fatalf("context not forwarded to queued guard: ran=%v err=%v", ran, err)
	}
}

func TestDomainPrunerGuardProbeAdapterFailsClosed(t *testing.T) {
	for _, adapter := range []*domainPrunerChainSource{nil, {}, {prunerChainSource: &prunerChainSource{}}} {
		ready, err := adapter.TryStateDomainChangePruneGuardReady(context.Background())
		if ready || err == nil {
			t.Fatalf("unavailable probe ready=%v err=%v", ready, err)
		}
	}
	adapter := &domainPrunerChainSource{prunerChainSource: &prunerChainSource{chain: &core.BlockChain{}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ready, err := adapter.TryStateDomainChangePruneGuardReady(ctx); ready || !errors.Is(err, context.Canceled) {
		t.Fatalf("probe context not forwarded: ready=%v err=%v", ready, err)
	}
}
