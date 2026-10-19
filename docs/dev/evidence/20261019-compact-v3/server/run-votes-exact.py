from pathlib import Path
import os,json,time,subprocess,datetime
p=Path("/data/gtron/compact-v3-validation-20261019-01")
while not (p/"final-status.json").exists():
 status=json.loads((p/"runtime-status.json").read_text())
 if status.get("error"):raise RuntimeError(status)
 time.sleep(10)
env=dict(os.environ,PATH="/data/go/bin:"+os.environ["PATH"],GOPATH="/root/go",GOCACHE="/root/.cache/go-build",GOTOOLCHAIN="local",CGO_ENABLED="0",GOMAXPROCS="1",GOMEMLIMIT="1200MiB",GTRON_VOTES_MEASURE_OUT=str(p/"votes-exact"))
with (p/"votes-exact.log").open("w") as f:subprocess.run(["/data/go/bin/go","test","./core/state","-run","^TestVotesIndexSpaceMeasurement$","-count=1","-timeout","15m"],cwd=p/"source-final",env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
(p/"votes-exact-status.json").write_text(json.dumps({"complete":True,"utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}))
