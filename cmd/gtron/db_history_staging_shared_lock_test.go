package main

import (
	"os"
	"syscall"
	"testing"
	"time"
)

type historyStagingLockTestInfo struct {
	mode os.FileMode
	stat syscall.Stat_t
}

func (i historyStagingLockTestInfo) Name() string       { return "start.lock" }
func (i historyStagingLockTestInfo) Size() int64        { return 0 }
func (i historyStagingLockTestInfo) Mode() os.FileMode  { return i.mode }
func (i historyStagingLockTestInfo) ModTime() time.Time { return time.Time{} }
func (i historyStagingLockTestInfo) IsDir() bool        { return false }
func (i historyStagingLockTestInfo) Sys() any           { return &i.stat }
func TestHistoryStagingSharedDeploymentLockOwnership(t *testing.T) {
	root := historyStagingLockTestInfo{mode: 0600, stat: syscall.Stat_t{Uid: 0, Gid: 0, Nlink: 1}}
	for _, shared := range []bool{false, true} {
		if !historyStagingLockOwnerAllowed(root, shared) {
			t.Fatal("root private lock rejected")
		}
	}
	for _, mode := range []os.FileMode{0644, 0666, 0600 | os.ModeSetuid, 0600 | os.ModeSetgid, 0600 | os.ModeSticky, 0600 | os.ModeSymlink} {
		bad := root
		bad.mode = mode
		if historyStagingLockOwnerAllowed(bad, true) {
			t.Fatalf("unsafe root mode accepted: %v", mode)
		}
	}
	linked := root
	linked.stat.Nlink = 2
	if historyStagingLockOwnerAllowed(linked, true) {
		t.Fatal("hard linked lock accepted")
	}
	uid, gid, err := historyStagingSharedLockAccount()
	if err != nil {
		t.Log("java-tron unavailable; private root branch tested")
		return
	}
	service := root
	service.mode = 0644
	service.stat.Uid = uid
	service.stat.Gid = gid
	if !historyStagingLockOwnerAllowed(service, true) || historyStagingLockOwnerAllowed(service, false) {
		t.Fatal("shared service lock contract differs")
	}
	for _, mode := range []os.FileMode{0600, 0666, 0644 | os.ModeSetuid, 0644 | os.ModeSetgid, 0644 | os.ModeSticky} {
		bad := service
		bad.mode = mode
		if historyStagingLockOwnerAllowed(bad, true) {
			t.Fatalf("unsafe service mode accepted: %v", mode)
		}
	}
	service.stat.Uid++
	if historyStagingLockOwnerAllowed(service, true) {
		t.Fatal("unknown lock owner authorized")
	}
}
