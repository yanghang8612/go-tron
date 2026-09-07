package types

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// AccountStorageCoreV4CodeHash validates the entire canonical storage core and
// returns its code hash as a borrowed slice. It does not materialize an Account
// or copy variable-width fields. The caller must not retain the result after
// the input changes. Envelope codecs use this to reference, rather than repeat,
// an identical code hash already present in the core.
func AccountStorageCoreV4CodeHash(data []byte) ([]byte, error) {
	if !IsAccountStorageCoreV4(data) || len(data) < 9 {
		return nil, errors.New("invalid account storage-v4 header")
	}
	if data[4] != accountStorageV4CodecVersion {
		return nil, fmt.Errorf("unsupported account storage codec version %d", data[4])
	}
	fields := binary.BigEndian.Uint32(data[5:9])
	if fields&^accountStorageV4KnownMask != 0 {
		return nil, errors.New("account storage-v4 has unknown bitmap bits")
	}
	d := accountStorageV4Decoder{data: data, off: 9}
	var hash []byte
	for bit := uint(0); bit < 28; bit++ {
		if fields&(1<<bit) == 0 {
			continue
		}
		switch bit {
		case 8, 14, 15, 23: // Boolean values live in the bitmap itself.
		case 0, 2, 13, 16, 17, 21, 24, 27:
			n, err := d.uvarint()
			if err != nil {
				return nil, err
			}
			if n == 0 || n > uint64(len(data)-d.off) {
				return nil, errors.New("invalid account storage-v4 byte field length")
			}
			value, _ := d.take(int(n))
			if bit == 24 {
				hash = value
			}
		default:
			value, err := d.i64()
			if err != nil {
				return nil, err
			}
			if value == 0 || bit == 1 && int64(int32(value)) != value {
				return nil, errors.New("invalid account storage-v4 integer field")
			}
		}
	}
	if d.off != len(data) {
		return nil, errors.New("account storage-v4 trailing bytes")
	}
	return hash, nil
}
