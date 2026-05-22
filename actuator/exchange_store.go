package actuator

import (
	"bytes"
	"fmt"
	"strconv"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func readExchangeForCurrentFork(ctx *Context, id int64) *corepb.Exchange {
	// Exchanges are rooted (Phase 3d): read from the system-KV via ctx.State.
	if ctx.DynProps.AllowSameTokenName() {
		return ctx.State.ReadExchangeV2(id)
	}
	return ctx.State.ReadExchange(id)
}

func writeExchangeForCurrentFork(ctx *Context, ex *corepb.Exchange) error {
	// Exchanges are rooted (Phase 3d): write through the system-KV via ctx.State.
	// Pre-AllowSameTokenName the V1 record (human-readable token names) and a
	// V2 record (numeric token ids) are both persisted; post-fork only V2.
	if ctx.DynProps.AllowSameTokenName() {
		return ctx.State.WriteExchangeV2(ex)
	}
	if err := ctx.State.WriteExchange(ex); err != nil {
		return err
	}
	exV2, err := exchangeWithV2TokenIDs(ctx, ex)
	if err != nil {
		return err
	}
	return ctx.State.WriteExchangeV2(exV2)
}

func exchangeWithV2TokenIDs(ctx *Context, ex *corepb.Exchange) (*corepb.Exchange, error) {
	v2 := proto.Clone(ex).(*corepb.Exchange)
	first, err := exchangeTokenNameToID(ctx, ex.FirstTokenId)
	if err != nil {
		return nil, fmt.Errorf("convert first token id: %w", err)
	}
	second, err := exchangeTokenNameToID(ctx, ex.SecondTokenId)
	if err != nil {
		return nil, fmt.Errorf("convert second token id: %w", err)
	}
	v2.FirstTokenId = first
	v2.SecondTokenId = second
	return v2, nil
}

func exchangeTokenNameToID(ctx *Context, tokenID []byte) ([]byte, error) {
	if bytes.Equal(tokenID, []byte("_")) {
		return append([]byte(nil), tokenID...), nil
	}
	if asset := ctx.State.ReadAssetIssueByName(tokenID); asset != nil {
		if asset.Id == "" {
			return nil, fmt.Errorf("asset name %q has empty token id", string(tokenID))
		}
		return []byte(asset.Id), nil
	}
	if id, ok := ctx.State.ReadAssetNameIndex(tokenID); ok {
		return []byte(strconv.FormatInt(id, 10)), nil
	}
	return nil, fmt.Errorf("asset name %q has no token id mapping", string(tokenID))
}
