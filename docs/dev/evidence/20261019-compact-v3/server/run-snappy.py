from pathlib import Path
import os,json,subprocess,datetime
p=Path("/data/gtron/compact-v3-validation-20261019-01");env=dict(os.environ,PATH="/data/go/bin:"+os.environ["PATH"],GOPATH="/root/go",GOCACHE="/root/.cache/go-build",GOTOOLCHAIN="local",CGO_ENABLED="0",GOMAXPROCS="1",GOMEMLIMIT="1200MiB")
steps=[("snappy-correctness",["test","./core","./core/state","./core/state/snapshots","-run","ArchiveQuery_ListWitnessesAt|CompactStateHistory|VotesPage|WitnessHistory|HistoryIndexPagesVirtual|BusinessSample|V7","-count=1","-timeout","10m"],{}),("history-index-snappy",["test","./core/state/snapshots","-run","^TestHistoryIndexPagesPinnedCorpus$","-v","-count=1","-timeout","15m"],{"GTRON_HISTORY_INDEX_CORPUS":"/data/gtron/other-storage-distribution-20261019-0245/cold-pinned","GTRON_INDEX_MEASURE_OUT":str(p/"history-index-snappy")}),("votes-final",["test","./core/state","-run","^TestVotesIndexSpaceMeasurement$","-count=1","-timeout","15m"],{"GTRON_VOTES_MEASURE_OUT":str(p/"votes-final")}),("snappy-build",["build","-o",str(p/"gtron-final-snappy-pure"),"./cmd/gtron"],{})]
try:
 for name,args,extra in steps:
  with (p/(name+".log")).open("w") as f:subprocess.run(["/data/go/bin/go"]+args,cwd=p/"source-final",env=dict(env,**extra),stdout=f,stderr=subprocess.STDOUT,check=True)
 (p/"snappy-status.json").write_text(json.dumps({"complete":True,"utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}))
except BaseException as e:
 (p/"snappy-status.json").write_text(json.dumps({"complete":False,"error":repr(e)}));raise
