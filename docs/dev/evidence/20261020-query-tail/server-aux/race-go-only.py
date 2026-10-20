from pathlib import Path
import os,json,subprocess,time
p=Path('/data/gtron/cold-query-tail-20261020')
e=dict(os.environ,GOTOOLCHAIN='local',GOPATH='/root/go',GOCACHE='/root/.cache/go-build',GOPROXY='off',GOMAXPROCS='2',GOMEMLIMIT='2GiB',CGO_ENABLED='1')
s={'tags':'gofuzz','purpose':'Auxiliary Linux race for changed Go history code; native Sapling correctness/build verified separately with production tags.'}
try:
 with (p/'final-gofuzz-race.log').open('wb') as f:r=subprocess.run(['/data/go/bin/go','test','-p=1','-tags=gofuzz','-race','./core/rawdb','-run','^TestQueryTail','-count=1','-timeout','60s'],cwd=str(p/'candidate'),env=e,stdout=f,stderr=subprocess.STDOUT,timeout=120)
 s['returncode']=r.returncode
except BaseException as ex:s['error']=repr(ex)
(p/'gofuzz-race-status.json').write_text(json.dumps(s,indent=2))
