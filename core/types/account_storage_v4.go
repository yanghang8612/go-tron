package types

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var accountStorageV4Magic = [...]byte{0x47, 0x54, 0x52, 0x43} // "GTRC"

const (
	accountStorageV4CodecVersion byte   = 1
	accountStorageV4KnownMask    uint32 = 1<<28 - 1
)

// IsAccountStorageCoreV4 distinguishes the internal account storage codec from
// java-tron's protobuf wire encoding. 0x47 has protobuf wire type 7, which is
// invalid, so a valid protobuf Account cannot share this prefix.
func IsAccountStorageCoreV4(data []byte) bool {
	return len(data) >= len(accountStorageV4Magic) && bytes.Equal(data[:len(accountStorageV4Magic)], accountStorageV4Magic[:])
}

func accountStorageV4Signed(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func accountStorageV4Unsigned(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

func accountStorageV4VarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func accountStorageV4BytesSize(value []byte) int {
	return accountStorageV4VarintSize(uint64(len(value))) + len(value)
}

func accountStorageV4Presence(pb *corepb.Account) uint32 {
	var fields uint32
	setBytes := func(bit uint, value []byte) {
		if len(value) != 0 {
			fields |= 1 << bit
		}
	}
	setInt := func(bit uint, value int64) {
		if value != 0 {
			fields |= 1 << bit
		}
	}
	setBytes(0, pb.AccountName)
	if pb.Type != 0 {
		fields |= 1 << 1
	}
	setBytes(2, pb.Address)
	setInt(3, pb.Balance)
	setInt(4, pb.NetUsage)
	setInt(5, pb.AcquiredDelegatedFrozenBalanceForBandwidth)
	setInt(6, pb.DelegatedFrozenBalanceForBandwidth)
	setInt(7, pb.OldTronPower)
	if pb.AssetOptimized {
		fields |= 1 << 8
	}
	setInt(9, pb.CreateTime)
	setInt(10, pb.LatestOprationTime)
	setInt(11, pb.Allowance)
	setInt(12, pb.LatestWithdrawTime)
	setBytes(13, pb.Code)
	if pb.IsWitness {
		fields |= 1 << 14
	}
	if pb.IsCommittee {
		fields |= 1 << 15
	}
	setBytes(16, pb.AssetIssuedName)
	setBytes(17, pb.AssetIssued_ID)
	setInt(18, pb.FreeNetUsage)
	setInt(19, pb.LatestConsumeTime)
	setInt(20, pb.LatestConsumeFreeTime)
	setBytes(21, pb.AccountId)
	setInt(22, pb.NetWindowSize)
	if pb.NetWindowOptimized {
		fields |= 1 << 23
	}
	setBytes(24, pb.CodeHash)
	setInt(25, pb.DelegatedFrozenV2BalanceForBandwidth)
	setInt(26, pb.AcquiredDelegatedFrozenV2BalanceForBandwidth)
	setBytes(27, pb.ProtoReflect().GetUnknown())
	return fields
}

// StorageCoreV4Size returns the exact size of MarshalStorageCoreV4. A presence
// bitmap keeps sparse accounts compact while signed varints avoid the ten-byte
// penalty protobuf applies to negative int64 values.
func (a *Account) StorageCoreV4Size() (int, error) {
	if a == nil || a.pb == nil {
		return 0, nil
	}
	if !accountDirectMapLayoutOK {
		return 0, errors.New("account protobuf schema changed; update storage-v4 codec")
	}
	pb := a.pb
	fields := accountStorageV4Presence(pb)
	size := len(accountStorageV4Magic) + 1 + 4
	addBytes := func(bit uint, value []byte) {
		if fields&(1<<bit) != 0 {
			size += accountStorageV4BytesSize(value)
		}
	}
	addInt := func(bit uint, value int64) {
		if fields&(1<<bit) != 0 {
			size += accountStorageV4VarintSize(accountStorageV4Signed(value))
		}
	}
	addBytes(0, pb.AccountName)
	addInt(1, int64(pb.Type))
	addBytes(2, pb.Address)
	addInt(3, pb.Balance)
	addInt(4, pb.NetUsage)
	addInt(5, pb.AcquiredDelegatedFrozenBalanceForBandwidth)
	addInt(6, pb.DelegatedFrozenBalanceForBandwidth)
	addInt(7, pb.OldTronPower)
	addInt(9, pb.CreateTime)
	addInt(10, pb.LatestOprationTime)
	addInt(11, pb.Allowance)
	addInt(12, pb.LatestWithdrawTime)
	addBytes(13, pb.Code)
	addBytes(16, pb.AssetIssuedName)
	addBytes(17, pb.AssetIssued_ID)
	addInt(18, pb.FreeNetUsage)
	addInt(19, pb.LatestConsumeTime)
	addInt(20, pb.LatestConsumeFreeTime)
	addBytes(21, pb.AccountId)
	addInt(22, pb.NetWindowSize)
	addBytes(24, pb.CodeHash)
	addInt(25, pb.DelegatedFrozenV2BalanceForBandwidth)
	addInt(26, pb.AcquiredDelegatedFrozenV2BalanceForBandwidth)
	addBytes(27, pb.ProtoReflect().GetUnknown())
	return size, nil
}

func appendAccountStorageV4Bytes(dst, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendAccountStorageV4Int64(dst []byte, value int64) []byte {
	return binary.AppendUvarint(dst, accountStorageV4Signed(value))
}

// AppendStorageCoreV4 appends the deterministic non-protobuf account storage
// core. The field bitmap is the schema; present values follow in bit order.
// Unknown protobuf fields are retained as an opaque final field so a
// load/write cycle never discards data introduced by a newer java-tron schema.
func (a *Account) AppendStorageCoreV4(dst []byte) ([]byte, error) {
	if a == nil || a.pb == nil {
		return dst, nil
	}
	if !accountDirectMapLayoutOK {
		return dst, errors.New("account protobuf schema changed; update storage-v4 codec")
	}
	pb := a.pb
	fields := accountStorageV4Presence(pb)
	dst = append(dst, accountStorageV4Magic[:]...)
	dst = append(dst, accountStorageV4CodecVersion)
	var bitmap [4]byte
	binary.BigEndian.PutUint32(bitmap[:], fields)
	dst = append(dst, bitmap[:]...)
	putBytes := func(bit uint, value []byte) {
		if fields&(1<<bit) != 0 {
			dst = appendAccountStorageV4Bytes(dst, value)
		}
	}
	putInt := func(bit uint, value int64) {
		if fields&(1<<bit) != 0 {
			dst = appendAccountStorageV4Int64(dst, value)
		}
	}
	putBytes(0, pb.AccountName)
	putInt(1, int64(pb.Type))
	putBytes(2, pb.Address)
	putInt(3, pb.Balance)
	putInt(4, pb.NetUsage)
	putInt(5, pb.AcquiredDelegatedFrozenBalanceForBandwidth)
	putInt(6, pb.DelegatedFrozenBalanceForBandwidth)
	putInt(7, pb.OldTronPower)
	putInt(9, pb.CreateTime)
	putInt(10, pb.LatestOprationTime)
	putInt(11, pb.Allowance)
	putInt(12, pb.LatestWithdrawTime)
	putBytes(13, pb.Code)
	putBytes(16, pb.AssetIssuedName)
	putBytes(17, pb.AssetIssued_ID)
	putInt(18, pb.FreeNetUsage)
	putInt(19, pb.LatestConsumeTime)
	putInt(20, pb.LatestConsumeFreeTime)
	putBytes(21, pb.AccountId)
	putInt(22, pb.NetWindowSize)
	putBytes(24, pb.CodeHash)
	putInt(25, pb.DelegatedFrozenV2BalanceForBandwidth)
	putInt(26, pb.AcquiredDelegatedFrozenV2BalanceForBandwidth)
	putBytes(27, pb.ProtoReflect().GetUnknown())
	return dst, nil
}

func (a *Account) MarshalStorageCoreV4() ([]byte, error) {
	size, err := a.StorageCoreV4Size()
	if err != nil || size == 0 {
		return nil, err
	}
	return a.AppendStorageCoreV4(make([]byte, 0, size))
}

type accountStorageV4Decoder struct {
	data []byte
	off  int
}

func (d *accountStorageV4Decoder) take(size int) ([]byte, error) {
	if size < 0 || d.off > len(d.data)-size {
		return nil, errors.New("truncated account storage-v4 value")
	}
	value := d.data[d.off : d.off+size]
	d.off += size
	return value, nil
}
func (d *accountStorageV4Decoder) uvarint() (uint64, error) {
	value, size := binary.Uvarint(d.data[d.off:])
	if size == 0 {
		return 0, errors.New("truncated account storage-v4 varint")
	}
	if size < 0 {
		return 0, errors.New("overflowing account storage-v4 varint")
	}
	if size != accountStorageV4VarintSize(value) {
		return 0, errors.New("non-canonical account storage-v4 varint")
	}
	d.off += size
	return value, nil
}
func (d *accountStorageV4Decoder) bytes() ([]byte, error) {
	size, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if size > uint64(len(d.data)-d.off) {
		return nil, errors.New("truncated account storage-v4 byte field")
	}
	value, _ := d.take(int(size))
	return append([]byte(nil), value...), nil
}
func (d *accountStorageV4Decoder) i64() (int64, error) {
	value, err := d.uvarint()
	if err != nil {
		return 0, err
	}
	return accountStorageV4Unsigned(value), nil
}

// UnmarshalAccountStorageCoreV4 decodes only the internal v4 storage core.
// Complete java-tron protobuf accounts continue to use UnmarshalAccount.
func UnmarshalAccountStorageCoreV4(data []byte) (*Account, error) {
	if !IsAccountStorageCoreV4(data) {
		return nil, errors.New("account storage-v4 magic not found")
	}
	if len(data) < 9 {
		return nil, errors.New("truncated account storage-v4 header")
	}
	if data[4] != accountStorageV4CodecVersion {
		return nil, fmt.Errorf("unsupported account storage codec version %d", data[4])
	}
	fields := binary.BigEndian.Uint32(data[5:9])
	if fields&^accountStorageV4KnownMask != 0 {
		return nil, fmt.Errorf("account storage-v4 has unknown bitmap bits %08x", fields&^accountStorageV4KnownMask)
	}
	d := &accountStorageV4Decoder{data: data, off: 9}
	pb := new(corepb.Account)
	getBytes := func(bit uint, target *[]byte) error {
		if fields&(1<<bit) == 0 {
			return nil
		}
		value, err := d.bytes()
		if err != nil {
			return err
		}
		if len(value) == 0 {
			return fmt.Errorf("account storage-v4 field bit %d is present with empty bytes", bit)
		}
		*target = value
		return nil
	}
	getInt := func(bit uint, target *int64) error {
		if fields&(1<<bit) == 0 {
			return nil
		}
		value, err := d.i64()
		if err != nil {
			return err
		}
		if value == 0 {
			return fmt.Errorf("account storage-v4 field bit %d is present with zero value", bit)
		}
		*target = value
		return nil
	}
	if err := getBytes(0, &pb.AccountName); err != nil {
		return nil, err
	}
	if fields&(1<<1) != 0 {
		value, err := d.i64()
		if err != nil {
			return nil, err
		}
		if value == 0 {
			return nil, errors.New("account storage-v4 account type is present with zero value")
		}
		if int64(int32(value)) != value {
			return nil, errors.New("account storage-v4 account type exceeds int32 range")
		}
		pb.Type = corepb.AccountType(value)
	}
	if err := getBytes(2, &pb.Address); err != nil {
		return nil, err
	}
	for _, field := range []struct {
		bit    uint
		target *int64
	}{
		{3, &pb.Balance}, {4, &pb.NetUsage},
		{5, &pb.AcquiredDelegatedFrozenBalanceForBandwidth},
		{6, &pb.DelegatedFrozenBalanceForBandwidth}, {7, &pb.OldTronPower},
		{9, &pb.CreateTime}, {10, &pb.LatestOprationTime}, {11, &pb.Allowance},
		{12, &pb.LatestWithdrawTime},
	} {
		if err := getInt(field.bit, field.target); err != nil {
			return nil, err
		}
	}
	pb.AssetOptimized = fields&(1<<8) != 0
	if err := getBytes(13, &pb.Code); err != nil {
		return nil, err
	}
	pb.IsWitness = fields&(1<<14) != 0
	pb.IsCommittee = fields&(1<<15) != 0
	if err := getBytes(16, &pb.AssetIssuedName); err != nil {
		return nil, err
	}
	if err := getBytes(17, &pb.AssetIssued_ID); err != nil {
		return nil, err
	}
	for _, field := range []struct {
		bit    uint
		target *int64
	}{
		{18, &pb.FreeNetUsage}, {19, &pb.LatestConsumeTime}, {20, &pb.LatestConsumeFreeTime},
	} {
		if err := getInt(field.bit, field.target); err != nil {
			return nil, err
		}
	}
	if err := getBytes(21, &pb.AccountId); err != nil {
		return nil, err
	}
	if err := getInt(22, &pb.NetWindowSize); err != nil {
		return nil, err
	}
	pb.NetWindowOptimized = fields&(1<<23) != 0
	if err := getBytes(24, &pb.CodeHash); err != nil {
		return nil, err
	}
	if err := getInt(25, &pb.DelegatedFrozenV2BalanceForBandwidth); err != nil {
		return nil, err
	}
	if err := getInt(26, &pb.AcquiredDelegatedFrozenV2BalanceForBandwidth); err != nil {
		return nil, err
	}
	var unknown []byte
	if err := getBytes(27, &unknown); err != nil {
		return nil, err
	}
	if d.off != len(data) {
		return nil, fmt.Errorf("account storage-v4 has %d trailing bytes", len(data)-d.off)
	}
	pb.ProtoReflect().SetUnknown(unknown)
	return NewAccountFromPB(pb), nil
}
