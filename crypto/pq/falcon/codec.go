package falcon

import (
	"errors"
)

// Encode a small polynomial into bytes, with a fixed number of bits (at
// most 8) per coefficient. Values are truncated if needed. The parameters
// MUST be such that an integral number of bytes is generated. The total
// written size (in bytes) is returned.
func trim_i8_encode(logn uint, f []int8, nbits int, dst []byte) int {
	n := 1 << logn
	acc := uint32(0)
	acc_len := 0
	mask := (uint32(1) << nbits) - 1
	j := 0
	for i := 0; i < n; i++ {
		acc = (acc << nbits) | (uint32(f[i]) & mask)
		acc_len += nbits
		for acc_len >= 8 {
			acc_len -= 8
			dst[j] = uint8(acc >> acc_len)
			j++
		}
	}
	return j
}

// Decode a small polynomial from bytes, with a fixed number of bits (at
// most 8) per coefficient. Output values are written into slice f[], which
// must be large enough. The actual number of read bytes is returned. An
// error is returned if the source is invalid, which happens in the
// following cases:
//   - The source slice does not contain enough bytes for the complete
//     polynomial.
//   - One of the coefficients would decode as -2^(nbits-1), which is not
//     valid.
//   - Some bits are unused in the last byte, and these bits are not all zero.
func trim_i8_decode(logn uint, src []byte, f []int8, nbits int) (int, error) {
	needed := ((nbits << logn) + 7) >> 3
	if len(src) < needed {
		return 0, errors.New("Truncated source")
	}
	n := 1 << logn
	j := 0
	acc := uint32(0)
	acc_len := 0
	mask1 := (uint32(1) << nbits) - 1
	mask2 := uint32(1) << (nbits - 1)
	for i := 0; i < needed; i++ {
		acc = (acc << 8) | uint32(src[i])
		acc_len += 8
		for acc_len >= nbits {
			acc_len -= nbits
			w := (acc >> acc_len) & mask1
			w |= -(w & mask2)
			if w == -mask2 {
				return 0, errors.New("Invalid coefficient value")
			}
			f[j] = int8(w)
			j++
			if j >= n {
				break
			}
		}
	}
	if (acc & ((uint32(1) << acc_len) - 1)) != 0 {
		return 0, errors.New("Non-zero padding bits")
	}
	return needed, nil
}

// Encode polynomial h modulo q into bytes, 14 bits per value.
// All source values MUST be in the [0,q-1] range. The number of source
// values MUST be a multiple of 4 (i.e. logn >= 2). The output size (in
// bytes) is returned.
func modq_encode(logn uint, h []uint16, dst []byte) int {
	n := 1 << logn
	j := 0
	for i := 0; i < n; i += 4 {
		x0 := uint64(h[i+0])
		x1 := uint64(h[i+1])
		x2 := uint64(h[i+2])
		x3 := uint64(h[i+3])
		x := (x0 << 42) | (x1 << 28) | (x2 << 14) | x3
		for k := 48; k >= 0; k -= 8 {
			dst[j] = uint8(x >> k)
			j++
		}
	}
	return j
}

// Decode polynomial h modulo q from bytes, 14 bits per coefficients.
//
// The degree MUST be at least 4 (i.e. logn >= 2). If the source slice
// does not contain enough bytes, of if one of the decoded coefficients
// is not in the [0,q-1] range, then an error is returned. Otherwise,
// the number of read bytes is returned.
func modq_decode(logn uint, src []byte, h []uint16) (int, error) {
	needed := 7 << (logn - 2)
	if len(src) < needed {
		return 0, errors.New("Truncated input")
	}
	n := 1 << logn
	i := 0
	for j := 0; j < n; j += 4 {
		x := uint64(0)
		for k := 0; k < 7; k++ {
			x = (x << 8) | uint64(src[i+k])
		}
		i += 7
		h0 := uint32((x >> 42) & 0x3FFF)
		h1 := uint32((x >> 28) & 0x3FFF)
		h2 := uint32((x >> 14) & 0x3FFF)
		h3 := uint32(x & 0x3FFF)
		if h0 >= q || h1 >= q || h2 >= q || h3 >= q {
			return 0, errors.New("Invalid coefficient value")
		}
		h[j+0] = uint16(h0)
		h[j+1] = uint16(h1)
		h[j+2] = uint16(h2)
		h[j+3] = uint16(h3)
	}
	return needed, nil
}

// Encode a signed polynomial s into bytes using a compressed (Golomb-Rice)
// format. An integer x is encoded as a sign bit (0 = non-negative,
// 1 = negative), then abs(x) mod 128 (over 7 bits), then floor(abs(x)/128)
// bits of value 0, then one bit of value 1. Source coefficients are
// supposed to be in the [-2047,+2047] range, thus the full encoding of
// a coefficient cannot exceed 1 + 7 + 15 + 1 = 24 bits.
//
// False is returned if one of the source value is not in [-2047,+2047],
// or if the encoded polynomial does not fit in the provided destination
// slice. Otherwise, the entire destination slice is set; unused bits (up
// to the slice end) are set to zero; true is returned.
func comp_encode(logn uint, s []int16, dst []byte) bool {
	n := 1 << logn
	acc := uint32(0)
	acc_len := 0
	j := 0
	for i := 0; i < n; i++ {
		x := int32(s[i])
		// We check that the value is in-range. For an in-range value,
		// we add at most 24 bits to the current accumulator, which, at
		// this point, contains at most 7 bits, thus not overflowing the
		// 32-bit value.
		if x < -2047 || x > +2047 {
			return false
		}
		// Get sign mask and absolute value.
		sw := uint32(x >> 16)
		w := (uint32(x) ^ sw) - sw
		// Encode sign bit then low 7 bits.
		acc <<= 8
		acc |= sw & 0x80
		acc |= w & 0x7F
		acc_len += 8
		// Encode the high bits.
		w = (w >> 7) + 1
		acc = (acc << w) | 1
		acc_len += int(w)
		// Output full bytes.
		for acc_len >= 8 {
			acc_len -= 8
			if j >= len(dst) {
				return false
			}
			dst[j] = uint8(acc >> acc_len)
			j++
		}
	}
	// Flush remaining bits (if any).
	if acc_len > 0 {
		if j >= len(dst) {
			return false
		}
		dst[j] = uint8(acc << (8 - acc_len))
		j++
	}
	// Pad with zeros.
	for j < len(dst) {
		dst[j] = uint8(0)
		j++
	}
	return true
}

// Decode a signed polynomial f using a compressed (Golomb-Rice) format
// (see comp_encode() for details). The entire source slice (src) is read.
// An error is returned in the following cases:
//
//   - The source does not contain enough bytes for the requested output
//     polynomial degree.
//   - An invalid coefficient encoding is encountered.
//   - Any of the remaining unused bits (after all coefficients have been
//     decoded) is non-zero.
//
// Valid encodings cover exactly the integers in the [-2047,+2047] range,
// and each such integer has a unique valid encoding. In particular, the
// "minus zero" encoding (sign bit is 1 but value is zero) is invalid.
func comp_decode(logn uint, src []byte, f []int16) error {
	n := 1 << logn
	i := 0
	acc := uint32(0)
	acc_len := 0
	for j := 0; j < n; j++ {
		// Get next 8 bits and plit them into sign bit (s) and absolute
		// value (m).
		if i >= len(src) {
			return errors.New("Truncated input")
		}
		acc = (acc << 8) | uint32(src[i])
		i++
		s := (acc >> (acc_len + 7)) & 1
		m := (acc >> acc_len) & 0x7F

		// Get next bits until a 1 is reached.
		for {
			if acc_len == 0 {
				if i >= len(src) {
					return errors.New("Truncated input")
				}
				acc = (acc << 8) | uint32(src[i])
				i++
				acc_len = 8
			}
			acc_len--
			if ((acc >> acc_len) & 1) != 0 {
				break
			}
			m += 0x80
			if m > 2047 {
				return errors.New("Out-of-range coefficient")
			}
		}

		// Reject "minus zero".
		if (s & ((m - 1) >> 31)) != 0 {
			return errors.New("Invalid minus zero encoding")
		}

		// Apply the sign to get the value.
		f[j] = int16((m ^ -s) + s)
	}

	// We got all the values. Check that all unused bits are zero.
	if acc_len > 0 {
		if (acc & ((uint32(1) << acc_len) - 1)) != 0 {
			return errors.New("Non-zero padding bits")
		}
	}
	for i < len(src) {
		if src[i] != 0 {
			return errors.New("Non-zero padding bits")
		}
		i++
	}
	return nil
}
