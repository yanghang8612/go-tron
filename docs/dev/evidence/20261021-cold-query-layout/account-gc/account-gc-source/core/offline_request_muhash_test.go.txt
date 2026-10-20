package core

import (
 "context"
 "encoding/json"
 "os"
 "path/filepath"
 "testing"
 "github.com/tronprotocol/go-tron/core/rawdb"
)
func TestOfflineRequestFixedMuHash(t *testing.T) {
 dir:=os.Getenv("GTRON_FIXED_INPUT");if dir==""{t.Skip("explicit immutable input required")}
 db,err:=rawdb.NewPebbleDBReadOnly(filepath.Join(dir,"chaindata"),512,1024);if err!=nil{t.Fatal(err)};defer db.Close()
 before,present,err:=rawdb.ReadStateCommitmentHead(db);if err!=nil || !present{t.Fatal(err)}
 report,err:=rawdb.InspectStateKVGenerations(context.Background(),db,true,nil,nil)
 out,e:=os.Create(os.Getenv("GTRON_FIXED_CENSUS"));if e!=nil{t.Fatal(e)}
 if e=json.NewEncoder(out).Encode(report);e!=nil{t.Fatal(e)};out.Close()
 if err!=nil{t.Fatal(err)}
 if !report.Complete || !report.MuHashVerified{t.Fatal("full source-fold mismatch")}
 after,present,err:=rawdb.ReadStateCommitmentHead(db);if err!=nil || !present || before!=after{t.Fatal("HEAD changed")}
}
