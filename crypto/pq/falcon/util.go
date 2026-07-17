package falcon

import (
	"crypto"
	"errors"
	sha3 "golang.org/x/crypto/sha3"
)

// Utility functions.

// Get number of bits to use for coefficients of f and g in a signing key.
func nbits_fg(logn uint) int {
	switch logn {
	case 2, 3, 4, 5:
		return 8
	case 6, 7:
		return 7
	case 8, 9:
		return 6
	default:
		return 5
	}
}

// Get the size of a signing key, in bytes, for a given degree. The
// degree n is provided logarithmically as logn, with n = 2^logn.
// Standard degrees are 512 (logn = 9) and 1024 (logn = 10).
func SigningKeySize(logn uint) int {
	n := 1 << logn
	return 1 + (nbits_fg(logn) << (logn - 2)) + n
}

// Get the size of a verifying key, in bytes, for a given degree. The
// degree n is provided logarithmically as logn, with n = 2^logn.
// Standard degrees are 512 (logn = 9) and 1024 (logn = 10).
func VerifyingKeySize(logn uint) int {
	return 1 + (7 << (logn - 2))
}

// Get the size of a signature, in bytes, for a given degree. The
// degree n is provided logarithmically as logn, with n = 2^logn.
// Standard degrees are 512 (logn = 9) and 1024 (logn = 10).
func SignatureSize(logn uint) int {
	return 44 + 3*(256>>(10-logn)) + 2*(128>>(10-logn)) +
		3*(64>>(10-logn)) + 2*(16>>(10-logn)) -
		2*(2>>(10-logn)) - 8*(1>>(10-logn))
}

// An alias for a domain context, which is an arbitrary sequence of up
// to 255 bytes that is meant to be used for domain separation.
type DomainContext []byte

// A pre-allocated empty context string.
var DOMAIN_NONE = DomainContext([]byte{})

// Hash the message into a polynomial.
//
//	logn              degree
//	nonce             signature nonce value (normally 40 bytes)
//	hashed_vrfy_key   SHAKE256 of the verifying key (64 bytes)
//	ctx               domain separation context
//	id                identifier for pre-hash function
//	data              pre-hashed data to sign/verify
//	c                 output slice
//
// If id is 0 then the data is supposed to be "raw" (no pre-hashing).
// As a special case, if id is -1 (0xFFFFFFFF) then the original Falcon
// mode is used (data is supposed to be raw, domain and hashed verifying
// key are ignored).
// The output polynomial is in "ext" representation (values in [0,q-1]).
// An error is returned if the hash identifier is unrecognized, or if the
// context length is greater than 255 bytes.
func hash_to_point(logn uint, nonce []byte, hashed_vrfy_key []byte,
	ctx DomainContext, id crypto.Hash, data []byte, c []uint16) error {

	n := 1 << logn
	sh := sha3.NewShake256()
	sh.Write(nonce)
	if id == crypto.Hash(0xFFFFFFFF) {
		sh.Write(data)
	} else {
		sh.Write(hashed_vrfy_key)
		var hb [2]byte
		if id == crypto.Hash(0) {
			hb[0] = 0x00
		} else {
			hb[0] = 0x01
		}
		if len(ctx) > 255 {
			return errors.New("Oversized domain separation context")
		}
		hb[1] = uint8(len(ctx))
		sh.Write(hb[:])
		sh.Write(ctx)
		var hash_id []byte
		switch id {
		case 0:
			hash_id = oid_NONE
		case crypto.SHA256:
			hash_id = oid_SHA256
		case crypto.SHA384:
			hash_id = oid_SHA384
		case crypto.SHA512:
			hash_id = oid_SHA512
		case crypto.SHA512_256:
			hash_id = oid_SHA512_256
		case crypto.SHA3_256:
			hash_id = oid_SHA3_256
		case crypto.SHA3_384:
			hash_id = oid_SHA3_384
		case crypto.SHA3_512:
			hash_id = oid_SHA3_512
		// TODO: add OIDs for SHAKE256 and SHAKE512, when stdlib defines
		// the relevant crypto.Hash constants.
		// TODO: maybe revise the API to allow the caller to provide the
		// OID (making it compatible with future hash functions)?
		default:
			return errors.New("Unknown pre-hash function identifier")
		}
		// TODO: maybe validate len(data) with regard to the identified
		// hash function? (This cannot be done if the API is modified to
		// allow arbitrary OIDs)
		sh.Write(hash_id)
		sh.Write(data)
	}
	i := 0
	for i < n {
		var v [2]byte
		sh.Read(v[:])
		w := (uint32(v[0]) << 8) | uint32(v[1])
		if w < 61445 {
			for w >= q {
				w -= q
			}
			c[i] = uint16(w)
			i += 1
		}
	}
	return nil
}

var oid_NONE = []byte("")
var oid_SHA256 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01,
}
var oid_SHA384 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02,
}
var oid_SHA512 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x03,
}
var oid_SHA512_256 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x06,
}
var oid_SHA3_256 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x08,
}
var oid_SHA3_384 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x09,
}
var oid_SHA3_512 = []byte{
	0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x0A,
}

// Hash the provided verifying (public) key into 64 bytes, using SHAKE256.
func hash_verifying_key(vkey []byte) [64]byte {
	sh := sha3.NewShake256()
	sh.Write(vkey)
	var d [64]byte
	sh.Read(d[:])
	return d
}

// A PRNG based on SHAKE256.
//
// This is just SHAKE256 with an extra output buffer.
type shake256prng struct {
	state sha3.ShakeHash
	buf   [136]byte
	ptr   int
}

// Create a new SHAKE256 PRNG instance, initialized with the provided seed.
func newSHAKE256prng(seed []byte) *shake256prng {
	r := new(shake256prng)
	r.state = sha3.NewShake256()
	r.state.Write(seed)
	r.ptr = len(r.buf)
	return r
}

// Get next byte from a SHAKE256prng instance.
func (r *shake256prng) next_u8() uint8 {
	ptr := r.ptr
	if ptr == len(r.buf) {
		r.state.Read(r.buf[:])
		ptr = 0
	}
	r.ptr = ptr + 1
	return r.buf[ptr]
}

// Get next 16-bit value from a SHAKE256prng instance.
func (r *shake256prng) next_u16() uint16 {
	ptr := r.ptr
	if ptr >= (len(r.buf) - 1) {
		x := uint16(r.next_u8())
		return x + (uint16(r.next_u8()) << 8)
	}
	r.ptr = ptr + 2
	return uint16(r.buf[ptr]) + (uint16(r.buf[ptr+1]) << 8)
}

// Get next 64-bit value from a SHAKE256prng instance.
func (r *shake256prng) next_u64() uint64 {
	ptr := r.ptr
	if ptr >= (len(r.buf) - 7) {
		x := uint64(0)
		for i := 0; i < 8; i++ {
			x += uint64(r.next_u8()) << (i << 3)
		}
		return x
	}
	x := uint64(0)
	r.ptr = ptr + 8
	for i := 0; i < 8; i++ {
		x += uint64(r.buf[ptr+i]) << (i << 3)
	}
	return x
}
