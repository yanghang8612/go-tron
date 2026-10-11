package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// This is a framing fixture, not a real chain/sample compression measurement.
func makeAccessorFixture(t *testing.T, version uint32, blocks int) string {
	t.Helper()
	headerSize := uint64(112)
	if version == 7 {
		headerSize = 120
	}
	var keyData []byte
	var entries [][]byte
	dirLen := uint64(blocks * 8)
	for i := 0; i < blocks; i++ {
		dirLen += 31
	}
	for i := 0; i < blocks; i++ {
		raw := bytes.Repeat([]byte{byte('a' + i)}, 2048)
		entry := make([]byte, 31)
		binary.BigEndian.PutUint16(entry[:2], 1)
		entry[2] = byte('a' + i)
		tail := entry[3:]
		binary.BigEndian.PutUint64(tail[:8], headerSize+dirLen+uint64(len(keyData)))
		binary.BigEndian.PutUint32(tail[8:12], uint32(len(raw))|storedRaw)
		binary.BigEndian.PutUint32(tail[12:16], uint32(len(raw)))
		binary.BigEndian.PutUint32(tail[16:20], crc32.ChecksumIEEE(raw))
		binary.BigEndian.PutUint32(tail[20:24], 128)
		checksum := crc32.NewIEEE()
		checksum.Write(entry[2:3])
		checksum.Write(tail[:24])
		binary.BigEndian.PutUint32(tail[24:28], checksum.Sum32())
		entries = append(entries, entry)
		keyData = append(keyData, raw...)
	}
	headerRaw := make([]byte, headerSize)
	copy(headerRaw[:8], "gtsdckv1")
	binary.BigEndian.PutUint32(headerRaw[8:12], version)
	binary.BigEndian.PutUint32(headerRaw[36:40], 128)
	binary.BigEndian.PutUint64(headerRaw[28:36], uint64(blocks*128))
	binary.BigEndian.PutUint64(headerRaw[40:48], uint64(blocks*128))
	binary.BigEndian.PutUint64(headerRaw[48:56], uint64(blocks))
	binary.BigEndian.PutUint64(headerRaw[56:64], dirLen)
	binary.BigEndian.PutUint64(headerRaw[64:72], uint64(len(keyData)))
	postingLen := uint64(blocks * 128 * 18)
	crcOff := 104
	if version == 7 {
		postingLen = 1 << 30
		binary.BigEndian.PutUint64(headerRaw[104:112], postingLen)
		crcOff = 112
	}
	binary.BigEndian.PutUint32(headerRaw[crcOff:], crc32.ChecksumIEEE(headerRaw[:crcOff]))
	var out bytes.Buffer
	out.Write(headerRaw)
	for i := 0; i < blocks; i++ {
		var off [8]byte
		binary.BigEndian.PutUint64(off[:], headerSize+uint64(blocks*8+i*31))
		out.Write(off[:])
	}
	for _, entry := range entries {
		out.Write(entry)
	}
	out.Write(keyData)
	path := filepath.Join(t.TempDir(), "framing-fixture.kv")
	if err := os.WriteFile(path, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, int64(headerSize+dirLen+uint64(len(keyData))+postingLen)); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestBoundedAccessorSampling(t *testing.T) {
	for _, version := range []uint32{6, 7} {
		path := makeAccessorFixture(t, version, 10)
		captured, err := captureSources([]string{path}, 3, 64<<10, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		file := captured.Files[0]
		if len(file.Samples) != 3 || file.Samples[0].Index != 0 || file.Samples[1].Index != 4 || file.Samples[2].Index != 9 {
			t.Fatalf("sample indices=%+v", file.Samples)
		}
		if file.ReadBytes > 8<<10 {
			t.Fatalf("read %d bytes from file of %d bytes", file.ReadBytes, file.Header.FileBytes)
		}
		if _, err := captureSources([]string{path}, 3, 256, 64<<10); err == nil {
			t.Fatal("ignored bounded read budget")
		}
	}
}
func TestSingleBlockCompressionReaderGuard(t *testing.T) {
	path := makeAccessorFixture(t, 7, 1)
	captured, err := captureSources([]string{path}, 1, 64<<10, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	result, err := benchmark(captured, 2, "fastest,default,better")
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range result.Results {
		if level.KeepBeneficialRatio >= 1 || len(level.FullySampledUnsafeCompressedFiles) != 1 {
			t.Fatalf("missed single-block reader rejection: %+v", level)
		}
		if level.KeepLargestSampleBlockRawRatio != 1 {
			t.Fatalf("single block needs raw fallback: %+v", level)
		}
	}
}
