package main
import("context";"encoding/json";"os";"time";s "github.com/tronprotocol/go-tron/core/state/snapshots")
func main(){ctx,cancel:=context.WithTimeout(context.Background(),10*time.Minute);defer cancel();start:=time.Now();r,e:=s.VerifyManifestFiles(os.Args[1],s.VerifyManifestOptions{Context:ctx,RequireRegistered:true,RequireChecksums:true});v:=map[string]any{"utc":time.Now().UTC(),"seconds":time.Since(start).Seconds(),"complete":e==nil,"report":r};if e!=nil {v["error"]=e.Error()};json.NewEncoder(os.Stdout).Encode(v);if e!=nil{os.Exit(1)}}
