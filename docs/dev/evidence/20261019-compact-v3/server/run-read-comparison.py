from pathlib import Path
import json,os,subprocess,time,datetime
p=Path("/data/gtron/compact-v3-validation-20261019-01")
while True:
 status=json.loads((p/"runtime-status.json").read_text())
 if status.get("complete"):break
 if status.get("error"):raise RuntimeError(status)
 time.sleep(10)
env=dict(os.environ,PATH="/data/go/bin:"+os.environ["PATH"],GOPATH="/root/go",GOCACHE="/root/.cache/go-build",GOTOOLCHAIN="local",CGO_ENABLED="0",GOMAXPROCS="1",GOMEMLIMIT="1200MiB")
for source,package,pattern,name in [("source-export","./core/rawdb","BenchmarkStateLatestUint64Read","uint64-before.log"),("source-v3","./core/rawdb","BenchmarkStateLatestUint64Read","uint64-after.log"),("source-v3","./core/types","BenchmarkAccountOwnerCoreRead","account-read.log")]:
 with (p/name).open("w") as f:subprocess.run(["/data/go/bin/go","test",package,"-run","^$","-bench",pattern,"-benchmem","-benchtime=500ms","-count=3"],cwd=p/source,env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
(p/"bench-status.json").write_text(json.dumps({"complete":True,"utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}))
