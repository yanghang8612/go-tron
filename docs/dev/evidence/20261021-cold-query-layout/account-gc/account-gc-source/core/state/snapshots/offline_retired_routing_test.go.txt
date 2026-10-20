package snapshots

import (
 "bytes"
 "context"
 "encoding/json"
 "fmt"
 "os"
 "path/filepath"
 "syscall"
 "testing"
 "time"
)

func TestOfflineRetiredRoutingReclamation(t *testing.T) {
 root:=os.Getenv("GTRON_GC_ROOT");if root=="" {t.Skip("isolated auxiliary reclamation diagnostic")}
 dir:=filepath.Join(root,"account-gc-input","state-snapshots")
 m,err:=LoadProductionManifest(dir);if err!=nil{t.Fatal(err)}
 before,err:=os.ReadFile(filepath.Join(dir,ManifestFile));if err!=nil{t.Fatal(err)}
 canonical:=map[string]os.FileInfo{}
 for _,ref:=range m.Segments {
  if historyRoutingRetiredObject(ref){continue}
  info,err:=os.Stat(filepath.Join(dir,ref.Path));if err!=nil{t.Fatal(err)};canonical[ref.Path]=info
 }
 candidates:=map[string]SegmentRef{}
 for _,ref:=range m.Retired {if historyRoutingRetiredObject(ref){if _,err:=os.Stat(filepath.Join(dir,ref.Path));err==nil {candidates[ref.Path]=ref}}}
 for _,ref:=range m.Segments {delete(candidates,ref.Path)}
 if ok,err:=protectPublishedRoutingObjects(context.Background(),dir,candidates);err!=nil||!ok{t.Fatalf("lease admission: %v %v",ok,err)}
 sizes,blocks:=uint64(0),uint64(0)
 paths:=[]string{}
 for path,ref:=range candidates {
  info,err:=os.Stat(filepath.Join(dir,path));if err!=nil{t.Fatal(err)}
  stat,ok:=info.Sys().(*syscall.Stat_t);if !ok||stat.Nlink!=1{t.Fatal("reclamation input is not exclusive",path,info.Sys())}
  sizes+=ref.Size;blocks+=uint64(stat.Blocks)*512;paths=append(paths,path)
 }
 if len(paths)==0{t.Fatal("empty reclamation experiment")}
 cursor:=retiredMetadataSweepCursor{};deleted,logical:=0,uint64(0);passes:=[]historyRoutingRetiredResult{};begin:=time.Now()
 for pass:=0;pass<256;pass++ {
  result,err:=pruneRetiredHistoryRoutingContext(context.Background(),dir,cursor);if err!=nil{t.Fatal(err)}
  if result.Scanned>256||result.FilesDeleted>16{t.Fatal("unbounded reclamation",result)}
  passes=append(passes,result);cursor=result.Next;deleted+=result.FilesDeleted;logical+=result.BytesDeleted
  if deleted==len(paths){break}
 }
 if deleted!=len(paths)||logical!=sizes{t.Fatalf("reclamation %d/%d %d/%d",deleted,len(paths),logical,sizes)}
 for _,path:=range paths {if _,err:=os.Lstat(filepath.Join(dir,path));!os.IsNotExist(err){t.Fatal("old derived file remains",path,err)}}
 after,err:=os.ReadFile(filepath.Join(dir,ManifestFile));if err!=nil||!bytes.Equal(before,after){t.Fatal("catalog bytes changed",err)}
 for path,old:=range canonical {
  current,err:=os.Stat(filepath.Join(dir,path));if err!=nil||!os.SameFile(old,current)||old.Size()!=current.Size(){t.Fatal("canonical identity changed",path,err)}
 }
 record:=map[string]any{"complete":true,"seconds":time.Since(begin).Seconds(),"files_deleted":deleted,"logical_bytes_deleted":logical,"exclusive_allocated_bytes_before_unlink":blocks,"exclusive_link_count":1,"canonical_paths_unchanged":len(canonical),"manifest_bytes_unchanged":true,"manifest_sha256":checksumBytes(before),"passes":passes,"production_mutations":false,"copy_originals_preserved":true}
 data,err:=json.MarshalIndent(record,"","  ");if err!=nil{t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(root,"account-gc-reclamation.json"),data,0600);err!=nil{t.Fatal(err)}
 fmt.Printf("RECLAIMED_DERIVED_FILES %d LOGICAL_BYTES %d EXCLUSIVE_ALLOCATED_BYTES %d\n",deleted,logical,blocks)
}
