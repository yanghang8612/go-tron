// Inbound counterpart to internal/tronapi/tronjson.go: parses java-tron's
// HTTP-style JSON into a proto.Message. java-tron's JsonFormat differs from
// the standard protojson in three ways relevant to capture:
//   - bytes fields are hex strings, not base64
//   - int64/uint64 are bare JSON numbers, not quoted strings
//   - keys are the proto field names verbatim (mostly snake_case, with the
//     occasional camelCase like `frozenV2` matching the literal proto field)
//
// Used by the capture driver to convert wallet/getaccount and listwitnesses
// responses into corepb.Account / corepb.Witness proto bytes for snapshot.json.

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func unmarshalTronJSON(data []byte, msg proto.Message) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parse JSON object: %w", err)
	}
	return populateMessage(msg.ProtoReflect(), raw)
}

func populateMessage(m protoreflect.Message, raw map[string]json.RawMessage) error {
	// java-tron HTTP encodes google.protobuf.Any non-standardly: instead of
	// putting raw proto bytes into Any.value (with @type), it inlines the
	// inner message JSON under "value" and puts the type URL under "type_url".
	// We invert that: resolve the inner type, populate it, marshal, then set
	// Any.value to the marshaled bytes.
	if m.Descriptor().FullName() == "google.protobuf.Any" {
		return populateAny(m, raw)
	}

	fields := m.Descriptor().Fields()
	for k, v := range raw {
		fd := fields.ByName(protoreflect.Name(k))
		if fd == nil {
			fd = fields.ByJSONName(k)
		}
		if fd == nil {
			// Unknown fields are deliberately tolerated — java-tron may add new fields.
			continue
		}
		if err := setField(m, fd, v); err != nil {
			return fmt.Errorf("field %q: %w", k, err)
		}
	}
	return nil
}

func populateAny(m protoreflect.Message, raw map[string]json.RawMessage) error {
	typeURLFD := m.Descriptor().Fields().ByName("type_url")
	valueFD := m.Descriptor().Fields().ByName("value")
	if typeURLFD == nil || valueFD == nil {
		return fmt.Errorf("Any message missing expected fields")
	}

	var typeURL string
	if t, ok := raw["type_url"]; ok {
		if err := json.Unmarshal(t, &typeURL); err != nil {
			return fmt.Errorf("Any.type_url: %w", err)
		}
	} else if t, ok := raw["@type"]; ok {
		if err := json.Unmarshal(t, &typeURL); err != nil {
			return fmt.Errorf("Any.@type: %w", err)
		}
	} else {
		return fmt.Errorf("Any: missing type_url")
	}

	innerName := protoreflect.FullName(typeURL)
	if i := strings.LastIndex(string(innerName), "/"); i >= 0 {
		innerName = innerName[i+1:]
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(innerName)
	if err != nil {
		return fmt.Errorf("Any: resolve %q: %w", typeURL, err)
	}
	inner := mt.New()

	valueRaw, ok := raw["value"]
	if ok {
		var sub map[string]json.RawMessage
		if err := json.Unmarshal(valueRaw, &sub); err != nil {
			return fmt.Errorf("Any.value: parse object: %w", err)
		}
		if err := populateMessage(inner, sub); err != nil {
			return fmt.Errorf("Any.value: populate %s: %w", innerName, err)
		}
	}

	innerBytes, err := proto.Marshal(inner.Interface())
	if err != nil {
		return fmt.Errorf("Any.value: marshal %s: %w", innerName, err)
	}
	m.Set(typeURLFD, protoreflect.ValueOfString(typeURL))
	m.Set(valueFD, protoreflect.ValueOfBytes(innerBytes))
	return nil
}

func setField(m protoreflect.Message, fd protoreflect.FieldDescriptor, raw json.RawMessage) error {
	if fd.IsList() {
		return setList(m.Mutable(fd).List(), fd, raw)
	}
	if fd.IsMap() {
		return setMap(m.Mutable(fd).Map(), fd, raw)
	}
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		var sub map[string]json.RawMessage
		if err := json.Unmarshal(raw, &sub); err != nil {
			return fmt.Errorf("parse nested object: %w", err)
		}
		return populateMessage(m.Mutable(fd).Message(), sub)
	}
	val, err := parseScalar(fd, raw)
	if err != nil {
		return err
	}
	m.Set(fd, val)
	return nil
}

func setList(list protoreflect.List, fd protoreflect.FieldDescriptor, raw json.RawMessage) error {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("parse list: %w", err)
	}
	for i, item := range items {
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			var sub map[string]json.RawMessage
			if err := json.Unmarshal(item, &sub); err != nil {
				return fmt.Errorf("list[%d]: parse object: %w", i, err)
			}
			elem := list.AppendMutable().Message()
			if err := populateMessage(elem, sub); err != nil {
				return fmt.Errorf("list[%d]: %w", i, err)
			}
			continue
		}
		v, err := parseScalar(fd, item)
		if err != nil {
			return fmt.Errorf("list[%d]: %w", i, err)
		}
		list.Append(v)
	}
	return nil
}

// java-tron's JsonFormat encodes proto maps two ways:
//
//   - object form (standard protojson):   {"k1": v1, "k2": v2, ...}
//   - array form (java JsonFormat):       [{"key": k1, "value": v1}, ...]
//
// wallet/getaccount returns asset / assetV2 / latest_asset_operation_timeV2
// in the array form. proposalCreate accepts both. setMap handles either.
func setMap(mp protoreflect.Map, fd protoreflect.FieldDescriptor, raw json.RawMessage) error {
	keyFD := fd.MapKey()
	valFD := fd.MapValue()

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var entries []struct {
			Key   json.RawMessage `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(raw, &entries); err != nil {
			return fmt.Errorf("parse map (array form): %w", err)
		}
		for i, e := range entries {
			ks, err := unmarshalMapKeyToString(e.Key)
			if err != nil {
				return fmt.Errorf("map[%d].key: %w", i, err)
			}
			if err := setMapEntry(mp, keyFD, valFD, ks, e.Value); err != nil {
				return fmt.Errorf("map[%d]: %w", i, err)
			}
		}
		return nil
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("parse map (object form): %w", err)
	}
	for k, v := range obj {
		if err := setMapEntry(mp, keyFD, valFD, k, v); err != nil {
			return fmt.Errorf("map[%q]: %w", k, err)
		}
	}
	return nil
}

func setMapEntry(mp protoreflect.Map, keyFD, valFD protoreflect.FieldDescriptor, keyStr string, valRaw json.RawMessage) error {
	mk, err := parseMapKey(keyFD, keyStr)
	if err != nil {
		return fmt.Errorf("key %q: %w", keyStr, err)
	}
	if valFD.Kind() == protoreflect.MessageKind || valFD.Kind() == protoreflect.GroupKind {
		var sub map[string]json.RawMessage
		if err := json.Unmarshal(valRaw, &sub); err != nil {
			return fmt.Errorf("parse value object: %w", err)
		}
		return populateMessage(mp.Mutable(mk).Message(), sub)
	}
	mv, err := parseScalar(valFD, valRaw)
	if err != nil {
		return err
	}
	mp.Set(mk, mv)
	return nil
}

// unmarshalMapKeyToString accepts either a JSON string ("123") or bare number
// (123) and returns the string form so parseMapKey can do the typed parse.
func unmarshalMapKeyToString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return "", err
	}
	return n.String(), nil
}

func parseMapKey(fd protoreflect.FieldDescriptor, s string) (protoreflect.MapKey, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(s).MapKey(), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfInt64(n).MapKey(), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfInt32(int32(n)).MapKey(), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfUint64(n).MapKey(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfUint32(uint32(n)).MapKey(), nil
	case protoreflect.BoolKind:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfBool(b).MapKey(), nil
	}
	return protoreflect.MapKey{}, fmt.Errorf("unsupported map key kind %s", fd.Kind())
}

func parseScalar(fd protoreflect.FieldDescriptor, raw json.RawMessage) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return protoreflect.Value{}, fmt.Errorf("bytes: not a JSON string: %w", err)
		}
		if s == "" {
			return protoreflect.ValueOfBytes(nil), nil
		}
		// java-tron's JsonFormat.escapeBytes prefers a printable-ASCII
		// fast-path for bytes values: e.g. asset_issued_ID = "1000595"
		// is a literal ASCII id string, NOT hex. We try hex first (the
		// strict default) and fall back to raw ASCII when the input
		// fails hex's odd-length or non-hex-character rules.
		if b, err := hex.DecodeString(s); err == nil {
			return protoreflect.ValueOfBytes(b), nil
		}
		return protoreflect.ValueOfBytes([]byte(s)), nil
	case protoreflect.StringKind:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BoolKind:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		v, err := parseAsInt(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(v)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		v, err := parseAsInt(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(v), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		v, err := parseAsUint(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		v, err := parseAsUint(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(v), nil
	case protoreflect.DoubleKind:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.FloatKind:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.EnumKind:
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			if ev := fd.Enum().Values().ByName(protoreflect.Name(s)); ev != nil {
				return protoreflect.ValueOfEnum(ev.Number()), nil
			}
			return protoreflect.Value{}, fmt.Errorf("enum %s: unknown name %q", fd.Enum().Name(), s)
		}
		v, err := parseAsInt(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)), nil
	}
	return protoreflect.Value{}, fmt.Errorf("unsupported field kind %s", fd.Kind())
}

func parseAsInt(raw json.RawMessage) (int64, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strconv.ParseInt(s, 10, 64)
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return 0, err
	}
	return n.Int64()
}

func parseAsUint(raw json.RawMessage) (uint64, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strconv.ParseUint(s, 10, 64)
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return 0, err
	}
	return strconv.ParseUint(n.String(), 10, 64)
}
