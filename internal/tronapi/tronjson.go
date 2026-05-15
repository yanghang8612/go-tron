// Package tronapi provides TRON-compatible JSON serialization.
//
// java-tron encodes bytes fields as hex strings and outputs int64 as numbers.
// Standard protojson uses base64 for bytes and quotes int64 as strings.
// This file bridges that gap so go-tron API responses match java-tron's format.
package tronapi

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// marshalTronJSON converts a protobuf message to TRON-compatible JSON.
func marshalTronJSON(msg proto.Message) ([]byte, error) {
	if msg == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(marshalMessage(msg.ProtoReflect()))
}

func marshalMessage(msg protoreflect.Message) map[string]any {
	result := make(map[string]any)
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !msg.Has(fd) {
			continue
		}
		result[string(fd.Name())] = marshalValue(fd, msg.Get(fd))
	}
	return result
}

func marshalValue(fd protoreflect.FieldDescriptor, val protoreflect.Value) any {
	if fd.IsList() {
		return marshalList(fd, val.List())
	}
	if fd.IsMap() {
		return marshalMap(fd, val.Map())
	}
	return marshalSingular(fd, val)
}

func marshalList(fd protoreflect.FieldDescriptor, list protoreflect.List) any {
	out := make([]any, list.Len())
	for i := 0; i < list.Len(); i++ {
		out[i] = marshalSingular(fd, list.Get(i))
	}
	return out
}

// marshalMap emits a proto map as a repeated [{key, value}] array. java-tron's
// JsonFormat predates the proto map type and serializes map fields as their
// underlying repeated MapEntry messages, so e.g. assetV2 is an array, not an
// object. Entries are sorted by key for deterministic output.
func marshalMap(fd protoreflect.FieldDescriptor, m protoreflect.Map) any {
	keyFD := fd.MapKey()
	valFD := fd.MapValue()
	type entry struct {
		sortKey string
		obj     map[string]any
	}
	entries := make([]entry, 0, m.Len())
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		entries = append(entries, entry{
			sortKey: fmt.Sprint(k.Value().Interface()),
			obj: map[string]any{
				"key":   marshalSingular(keyFD, k.Value()),
				"value": marshalSingular(valFD, v),
			},
		})
		return true
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].sortKey < entries[j].sortKey })
	out := make([]any, len(entries))
	for i, e := range entries {
		out[i] = e.obj
	}
	return out
}

func marshalSingular(fd protoreflect.FieldDescriptor, val protoreflect.Value) any {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		b := val.Bytes()
		if len(b) == 0 {
			return ""
		}
		// java-tron's Util.convertOutput decodes Account.asset_issued_ID from
		// its raw bytes to a UTF-8 string; every other bytes field stays hex.
		if fd.FullName() == "protocol.Account.asset_issued_ID" {
			return string(b)
		}
		return hex.EncodeToString(b)

	case protoreflect.MessageKind, protoreflect.GroupKind:
		if fd.Message().FullName() == "google.protobuf.Any" {
			return marshalAny(val.Message())
		}
		return marshalMessage(val.Message())

	case protoreflect.EnumKind:
		ed := fd.Enum().Values().ByNumber(val.Enum())
		if ed != nil {
			return string(ed.Name())
		}
		return int(val.Enum())

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(val.Int())

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return val.Int()

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(val.Uint())

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return val.Uint()

	case protoreflect.FloatKind:
		return float32(val.Float())

	case protoreflect.DoubleKind:
		return val.Float()

	case protoreflect.BoolKind:
		return val.Bool()

	case protoreflect.StringKind:
		return val.String()

	default:
		return fmt.Sprint(val.Interface())
	}
}

// marshalAny handles google.protobuf.Any: shows @type + unpacked inner fields.
func marshalAny(msg protoreflect.Message) map[string]any {
	typeURLFD := msg.Descriptor().Fields().ByName("type_url")
	valueFD := msg.Descriptor().Fields().ByName("value")

	typeURL := msg.Get(typeURLFD).String()
	valueBytes := msg.Get(valueFD).Bytes()

	result := map[string]any{
		"@type": typeURL,
	}

	// Try to unpack and inline the inner message fields
	if len(valueBytes) > 0 {
		inner, err := unpackAny(typeURL, valueBytes)
		if err == nil {
			for k, v := range marshalMessage(inner.ProtoReflect()) {
				result[k] = v
			}
			return result
		}
	}

	// Fallback: show raw value as hex
	if len(valueBytes) > 0 {
		result["value"] = hex.EncodeToString(valueBytes)
	}
	return result
}

// unpackAny resolves a type URL to a message and unmarshals the value bytes.
func unpackAny(typeURL string, value []byte) (proto.Message, error) {
	// type URL format: "type.googleapis.com/package.MessageName"
	name := protoreflect.FullName(typeURL)
	// Strip the URL prefix if present
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			name = name[i+1:]
			break
		}
	}

	mt, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return nil, err
	}
	msg := mt.New().Interface()
	if err := proto.Unmarshal(value, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// --- Block / Transaction helpers ---

// MarshalBlock converts a Block proto to JSON, adding blockID and txID fields.
func MarshalBlock(msg proto.Message) ([]byte, error) {
	if msg == nil {
		return []byte("{}"), nil
	}
	result := marshalMessage(msg.ProtoReflect())
	addBlockID(result, msg)
	addTxIDsToBlock(result, msg)
	return json.Marshal(result)
}

// addBlockID computes and adds the blockID field.
// BlockID = 8 bytes block number (big-endian) + bytes 8..31 of SHA256(raw_data).
func addBlockID(result map[string]any, msg proto.Message) {
	headerFD := msg.ProtoReflect().Descriptor().Fields().ByName("block_header")
	if headerFD == nil || !msg.ProtoReflect().Has(headerFD) {
		return
	}
	headerMsg := msg.ProtoReflect().Get(headerFD).Message()
	rawFD := headerMsg.Descriptor().Fields().ByName("raw_data")
	if rawFD == nil || !headerMsg.Has(rawFD) {
		return
	}
	rawMsg := headerMsg.Get(rawFD).Message()
	rawBytes, err := proto.Marshal(rawMsg.Interface())
	if err != nil {
		return
	}
	numFD := rawMsg.Descriptor().Fields().ByName("number")
	blockNum := rawMsg.Get(numFD).Int()

	hash := sha256.Sum256(rawBytes)
	id := make([]byte, 32)
	binary.BigEndian.PutUint64(id[:8], uint64(blockNum))
	copy(id[8:], hash[8:])
	result["blockID"] = hex.EncodeToString(id)
}

// addTxIDsToBlock adds txID and raw_data_hex to each transaction in a block.
func addTxIDsToBlock(result map[string]any, msg proto.Message) {
	txsSlice, ok := result["transactions"].([]any)
	if !ok || len(txsSlice) == 0 {
		return
	}
	txsFD := msg.ProtoReflect().Descriptor().Fields().ByName("transactions")
	txsList := msg.ProtoReflect().Get(txsFD).List()

	for i, txRaw := range txsSlice {
		txMap, ok := txRaw.(map[string]any)
		if !ok || i >= txsList.Len() {
			continue
		}
		addTxComputedFields(txMap, txsList.Get(i).Message())
	}
}

// addTxComputedFields adds txID and raw_data_hex to a transaction map.
func addTxComputedFields(txMap map[string]any, txMsg protoreflect.Message) {
	rawFD := txMsg.Descriptor().Fields().ByName("raw_data")
	if rawFD == nil || !txMsg.Has(rawFD) {
		return
	}
	rawBytes, err := proto.Marshal(txMsg.Get(rawFD).Message().Interface())
	if err != nil {
		return
	}
	h := sha256.Sum256(rawBytes)
	txMap["txID"] = hex.EncodeToString(h[:])
	txMap["raw_data_hex"] = hex.EncodeToString(rawBytes)
}

// MarshalBroadcastResult creates a java-tron compatible broadcast response.
func MarshalBroadcastResult(success bool, txID string, errMsg string) ([]byte, error) {
	result := map[string]any{
		"result": success,
		"txid":   txID,
	}
	if success {
		result["code"] = "SUCCESS"
		result["message"] = ""
	} else {
		result["code"] = "OTHER_ERROR"
		result["message"] = hex.EncodeToString([]byte(errMsg))
	}
	return json.Marshal(result)
}
