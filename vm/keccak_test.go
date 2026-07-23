package vm

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestKeccak256PartsMatchesConcatenation(t *testing.T) {
	tests := [][][]byte{
		nil,
		{nil},
		{[]byte("a")},
		{[]byte("parent"), nil, []byte("payload"), {0, 1, 2, 3}},
		{bytes.Repeat([]byte{0xaa}, 32), bytes.Repeat([]byte{0xbb}, 21), bytes.Repeat([]byte{0xcc}, 4096)},
	}
	for i, parts := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			joined := bytes.Join(parts, nil)
			if got, want := keccak256Parts(parts...), tcommon.Keccak256(joined); got != want {
				t.Fatalf("digest = %x, want %x", got, want)
			}
		})
	}
}

func TestKeccak256PartsConcurrent(t *testing.T) {
	const workers = 32
	payload := bytes.Repeat([]byte("parallel-keccak"), 64)
	want := tcommon.Keccak256(payload)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if got := keccak256Parts(payload[:17], payload[17:]); got != want {
					t.Errorf("digest = %x, want %x", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}
