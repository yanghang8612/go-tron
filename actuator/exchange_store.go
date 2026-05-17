package actuator

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func readExchangeForCurrentFork(ctx *Context, id int64) *corepb.Exchange {
	if ctx.DynProps.AllowSameTokenName() {
		return rawdb.ReadExchangeV2(ctx.DB, id)
	}
	return rawdb.ReadExchange(ctx.DB, id)
}

func writeExchangeForCurrentFork(ctx *Context, ex *corepb.Exchange) error {
	if ctx.DynProps.AllowSameTokenName() {
		return rawdb.WriteExchangeV2(ctx.DB, ex)
	}
	if err := rawdb.WriteExchange(ctx.DB, ex); err != nil {
		return err
	}
	exV2, err := exchangeWithV2TokenIDs(ctx, ex)
	if err != nil {
		return err
	}
	return rawdb.WriteExchangeV2(ctx.DB, exV2)
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
	if asset := rawdb.ReadAssetIssueByName(ctx.DB, tokenID); asset != nil {
		if asset.Id == "" {
			return nil, fmt.Errorf("asset name %q has empty token id", string(tokenID))
		}
		return []byte(asset.Id), nil
	}
	if id, ok := rawdb.ReadAssetNameIndex(ctx.DB, tokenID); ok {
		return []byte(strconv.FormatInt(id, 10)), nil
	}
	return nil, fmt.Errorf("asset name %q has no token id mapping", string(tokenID))
}
