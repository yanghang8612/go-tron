//go:build !linux

package main

import "fmt"

func evictSST(string) error {
	return fmt.Errorf("OS-cold experiment requires Linux FADV_DONTNEED; use -cold-queries=0 here")
}
