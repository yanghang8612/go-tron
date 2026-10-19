from pathlib import Path
import os,json,subprocess,time,datetime
p=Path("/data/gtron/compact-v3-validation-20261019-01")
while not (p/"bench-status.json").exists():
 status=json.loads((p/"runtime-status.json").read_text())
 if status.get("error"):raise RuntimeError(status)
 time.sleep(10)
env=dict(os.environ,PATH="/data/go/bin:"+os.environ["PATH"],GOPATH="/root/go",GOCACHE="/root/.cache/go-build",GOTOOLCHAIN="local",CGO_ENABLED="0",GOMAXPROCS="1",GOMEMLIMIT="1200MiB")
steps=[("final-correctness",["/data/go/bin/go","test","./core/state","./core/rawdb","./core/rawdb/freezer","./core/state/snapshots","./core/types","./core/state/domains","./common/muhash3072","-run","Compact|VotesPage|RelativeTransactionIndexWidths|HistoryIndexPagesVirtual|AccountOwner|Owned|MuHashElement|CommitmentBeforeSurvives|BusinessSample|ModerateL0","-count=1","-timeout","10m"]),("final-build",["/data/go/bin/go","build","-o",str(p/"gtron-final-pure"),"./cmd/gtron"])]
for name,args in steps:
 with (p/(name+".log")).open("w") as f:subprocess.run(args,cwd=p/"source-final",env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
(p/"final-status.json").write_text(json.dumps({"complete":True,"source_commit":"5f04c573","source_archive_sha256":"954bc47a15c7c59de5ef83e2e2d0b7c8e6ee0a29a8cd4230ab2e45b74d1d9e2a","utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}))
