package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"sync"
)

// Retain owned, checksum-authenticated images, never unchecked disk slices.
// Every new request opens/stats the exact reference. Reuse requires inode,
// size, mode, mtime AND OS ctime; platforms without ctime do ordinary reads.
// The charge reserves room for a validated sorted row directory and source map.
const historyRoutingImageCacheBytes = 128 << 20
const historyRoutingImageCacheEntries = 8

var historyRoutingImages = struct {
	sync.Mutex
	entries map[historyRoutingReadKey]historyRoutingCachedImage
	bytes   int
	clock   uint64
}{entries: make(map[historyRoutingReadKey]historyRoutingCachedImage)}

type historyRoutingCachedImage struct {
	image  historyRoutingReadImage
	info   os.FileInfo
	change [2]int64
	charge int
	used   uint64
}

func routingImageCharge(data []byte) int {
	if len(data) < systemHistoryRoutingHeaderSize {
		return historyRoutingImageCacheBytes + 1
	}
	n, k := binary.BigEndian.Uint32(data[12:16]), binary.BigEndian.Uint32(data[16:20])
	if n > systemHistoryRoutingMaxSources || k > systemHistoryRoutingMaxKeys {
		return historyRoutingImageCacheBytes + 1
	}
	return len(data) + int(n)*128 + int(k)*4 + 1024
}
func cachedRoutingImage(key historyRoutingReadKey, info os.FileInfo) (historyRoutingReadImage, bool) {
	change, known := historyStagingChangeTime(info)
	if !known {
		return historyRoutingReadImage{}, false
	}
	historyRoutingImages.Lock()
	defer historyRoutingImages.Unlock()
	c, ok := historyRoutingImages.entries[key]
	if !ok {
		return historyRoutingReadImage{}, false
	}
	if !os.SameFile(c.info, info) || c.info.Size() != info.Size() || c.info.Mode() != info.Mode() || !c.info.ModTime().Equal(info.ModTime()) || c.change != change {
		delete(historyRoutingImages.entries, key)
		historyRoutingImages.bytes -= c.charge
		return historyRoutingReadImage{}, false
	}
	historyRoutingImages.clock++
	c.used = historyRoutingImages.clock
	historyRoutingImages.entries[key] = c
	return c.image, true
}
func cacheRoutingImage(key historyRoutingReadKey, info os.FileInfo, image historyRoutingReadImage) {
	change, known := historyStagingChangeTime(info)
	charge := routingImageCharge(image.data)
	if !known || charge > historyRoutingImageCacheBytes {
		return
	}
	historyRoutingImages.Lock()
	defer historyRoutingImages.Unlock()
	if old, ok := historyRoutingImages.entries[key]; ok {
		historyRoutingImages.bytes -= old.charge
		delete(historyRoutingImages.entries, key)
	}
	for len(historyRoutingImages.entries) >= historyRoutingImageCacheEntries || historyRoutingImages.bytes+charge > historyRoutingImageCacheBytes {
		var oldest historyRoutingReadKey
		stamp := ^uint64(0)
		for k, v := range historyRoutingImages.entries {
			if v.used < stamp {
				oldest, stamp = k, v.used
			}
		}
		historyRoutingImages.bytes -= historyRoutingImages.entries[oldest].charge
		delete(historyRoutingImages.entries, oldest)
	}
	historyRoutingImages.clock++
	historyRoutingImages.entries[key] = historyRoutingCachedImage{image, info, change, charge, historyRoutingImages.clock}
	historyRoutingImages.bytes += charge
}

// Validate every row once, then binary-search only requested memberships.
// The directory references the owned authenticated image and never mmap/disk.
// Cancelled/failed construction publishes no partial negative proofs.
type historyRoutingImageIndex struct {
	mu      sync.Mutex
	route   *systemHistoryRouting
	offsets []uint32
}

func (index *historyRoutingImageIndex) selected(ctx context.Context, data []byte, ref SegmentRef, wanted map[string]bool) (*systemHistoryRouting, error) {
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if ref.Rows > systemHistoryRoutingMaxKeys {
		return nil, errors.New("snapshots: routing directory row budget exceeded")
	}
	if index.route == nil {
		offsets := make([]uint32, 0, ref.Rows)
		pos := systemHistoryRoutingHeaderSize + int(binary.BigEndian.Uint32(data[12:16]))*32
		route, err := walkHistoryRoutingFamily(ctx, data, ref, false, map[string]bool{}, func(key []byte, _ byte, payload []byte) error {
			offsets = append(offsets, uint32(pos))
			pos += 2 + len(key) + 5 + len(payload)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if err = contextError(ctx); err != nil {
			return nil, err
		}
		index.route, index.offsets = route, offsets
	}
	r := *index.route
	r.selection = make(map[string]bool, len(wanted))
	r.keys = make(map[string][]byte, len(wanted))
	keyAt := func(i int) []byte {
		off := int(index.offsets[i])
		n := int(binary.BigEndian.Uint16(data[off:]))
		return data[off+2 : off+2+n]
	}
	for key, selected := range wanted {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		r.selection[key] = selected
		if !selected {
			continue
		}
		i := sort.Search(len(index.offsets), func(i int) bool { return bytes.Compare(keyAt(i), []byte(key)) >= 0 })
		if i == len(index.offsets) || !bytes.Equal(keyAt(i), []byte(key)) {
			continue
		}
		off := int(index.offsets[i]) + 2 + len(key)
		n := int(binary.BigEndian.Uint32(data[off+1:]))
		members := make([]byte, 1+n)
		members[0] = data[off]
		copy(members[1:], data[off+5:off+5+n])
		r.keys[key] = members
	}
	return &r, nil
}
