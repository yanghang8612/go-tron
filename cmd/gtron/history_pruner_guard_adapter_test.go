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
