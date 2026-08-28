package types

import (
	"context"
	"crypto/sha256"
	"errors"
	"unicode/utf8"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Keep this recognizer deliberately narrower than protobuf. Unsupported
// schemas/encodings use the authoritative block decoder, not a second decoder.
// The current Block graph is under six messages deep and has no field above 28.
const transactionHashWireMaxDepth = 16

type transactionHashWireField struct {
	kind     protoreflect.Kind
	repeated bool
	message  *transactionHashWireMessage
}

type transactionHashWireMessage struct {
	fields [32]transactionHashWireField
}

var transactionHashBlockWire = buildTransactionHashWireMessage((&corepb.Block{}).ProtoReflect().Descriptor(), 0)

func buildTransactionHashWireMessage(descriptor protoreflect.MessageDescriptor, depth int) *transactionHashWireMessage {
	if depth >= transactionHashWireMaxDepth || descriptor.Syntax() != protoreflect.Proto3 || descriptor.ExtensionRanges().Len() != 0 {
		return nil
	}
	message := new(transactionHashWireMessage)
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		number := int(field.Number())
		if number >= len(message.fields) || field.IsMap() || field.IsPacked() || field.ContainingOneof() != nil || field.HasOptionalKeyword() {
			continue
		}
		rule := transactionHashWireField{kind: field.Kind(), repeated: field.IsList()}
		switch rule.kind {
		case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.EnumKind, protoreflect.BytesKind, protoreflect.StringKind:
		case protoreflect.MessageKind:
			rule.message = buildTransactionHashWireMessage(field.Message(), depth+1)
			if rule.message == nil {
				continue
			}
		default:
			continue
		}
		message.fields[number] = rule
	}
	return message
}

// canonical recognizes a strict subset whose typed decode followed by marshal
// is byte-for-byte unchanged. Unknown fields, unsupported types, duplicate
// singular fields, explicit defaults, nonminimal lengths/varints and reordered
// fields all fall back. Any.Value remains opaque bytes, as in proto.Marshal.
// Validate the complete block, including header/results/PQ envelopes, before
// yielding any hash: a malformed trailing field must not cause partial output.
func (message *transactionHashWireMessage) canonical(ctx context.Context, data []byte, depth int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if message == nil || depth >= transactionHashWireMaxDepth {
		return false, nil
	}
	var previous protowire.Number
	for len(data) != 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 || number <= 0 || int64(number) >= int64(len(message.fields)) || tagLen != protowire.SizeTag(number) || number < previous {
			return false, nil
		}
		field := &message.fields[number]
		if field.kind == 0 || (number == previous && !field.repeated) {
			return false, nil
		}
		previous = number
		data = data[tagLen:]
		switch field.kind {
		case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.EnumKind:
			if wireType != protowire.VarintType {
				return false, nil
			}
			value, n := protowire.ConsumeVarint(data)
			if n < 0 || n != protowire.SizeVarint(value) || (!field.repeated && value == 0) {
				return false, nil
			}
			if field.kind != protoreflect.Int64Kind && value != uint64(int64(int32(value))) {
				return false, nil
			}
			data = data[n:]
		case protoreflect.BytesKind, protoreflect.StringKind, protoreflect.MessageKind:
			if wireType != protowire.BytesType {
				return false, nil
			}
			value, n := protowire.ConsumeBytes(data)
			if n < 0 || n != protowire.SizeBytes(len(value)) {
				return false, nil
			}
			if field.kind == protoreflect.MessageKind {
				ok, err := field.message.canonical(ctx, value, depth+1)
				if err != nil || !ok {
					return false, err
				}
			} else if (!field.repeated && len(value) == 0) || (field.kind == protoreflect.StringKind && !utf8.Valid(value)) {
				return false, nil
			}
			data = data[n:]
		default:
			return false, nil
		}
	}
	return true, nil
}

// IterateBlockTransactionHashes visits exactly the transaction IDs produced by
// UnmarshalBlockBorrowed(data).Transactions()[i].Hash(), in ordinal order. A
// conservative canonical-wire fast path avoids constructing and re-encoding a
// full protobuf graph for immutable freezer bodies. Every other encoding uses
// the established decoder, including its pre-PQ compatibility fallback.
//
// data must remain immutable during the call. No input bytes escape. The full
// block is validated before the first callback; callback errors and context
// cancellation stop iteration immediately, without replaying previous results.
func IterateBlockTransactionHashes(ctx context.Context, data []byte, yield func(int, common.Hash) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if yield == nil {
		return errors.New("nil block transaction hash iterator")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	canonical := false
	if blockDecodeReserveLayoutOK {
		var err error
		canonical, err = transactionHashBlockWire.canonical(ctx, data, 0)
		if err != nil {
			return err
		}
	}
	if !canonical {
		block, err := UnmarshalBlockBorrowed(data)
		if err != nil {
			return err
		}
		for ordinal, tx := range block.Transactions() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := yield(ordinal, tx.Hash()); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	ordinal := 0
	for len(data) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		number, _, tagLen := protowire.ConsumeTag(data)
		transaction, n := protowire.ConsumeBytes(data[tagLen:])
		data = data[tagLen+n:]
		if number != 1 {
			continue
		}
		var hash common.Hash
		// The complete canonical check above guarantees raw_data, if present,
		// is the first field and occurs once. Absent raw_data has a zero ID;
		// an explicitly present empty raw_data hashes the empty byte string.
		if len(transaction) != 0 && transaction[0] == byte(protowire.EncodeTag(1, protowire.BytesType)) {
			raw, _ := protowire.ConsumeBytes(transaction[1:])
			hash = sha256.Sum256(raw)
		}
		if err := yield(ordinal, hash); err != nil {
			return err
		}
		ordinal++
	}
	return ctx.Err()
}
