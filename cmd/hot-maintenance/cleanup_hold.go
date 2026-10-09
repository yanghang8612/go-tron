package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type cleanupHoldState struct {
	path string
	info os.FileInfo
	hash [32]byte
}

func readCleanupHold(path string) (cleanupHoldState, error) {
	var out cleanupHoldState
	if !filepath.IsAbs(path) {
		return out, errors.New("cleanup requires absolute existing maintenance hold path")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > 16384 || info.Mode().Perm()&0o022 != 0 {
		return out, fmt.Errorf("cleanup maintenance hold is absent, unsafe, or oversized: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return out, errors.New("cleanup maintenance hold changed during open")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, 16385))
	if err != nil || n != info.Size() {
		return out, errors.New("cleanup maintenance hold changed during read")
	}
	copy(out.hash[:], h.Sum(nil))
	out.path, out.info = path, opened
	return out, nil
}

func (h cleanupHoldState) Recheck() error {
	after, err := readCleanupHold(h.path)
	if err != nil {
		return err
	}
	if !os.SameFile(h.info, after.info) || h.info.Mode() != after.info.Mode() || h.info.ModTime() != after.info.ModTime() || h.info.Size() != after.info.Size() || h.hash != after.hash {
		return errors.New("cleanup maintenance hold changed")
	}
	return nil
}
