from pathlib import Path
import os,subprocess
p=Path('/data/gtron/compact-v3-rollout-20261019-01/cold-query-opt-20261020');env=dict(os.environ,GOTOOLCHAIN='local',GOMAXPROCS='2',GOMEMLIMIT='4GiB');env['GTRON_COLD_QUERY_CORPUS']=str(p/'corpus');env['GTRON_COLD_QUERY_OUT']=str(p/'candidate-final-profile-results.json')
with (p/'candidate-final-profile.log').open('wb') as f:
 r=subprocess.run([str(p/'candidate-final.test'),'-test.run=^TestOfflineColdQueryCorpus$','-test.v','-test.timeout=5m','-test.cpuprofile='+str(p/'candidate-final-cpu.pb.gz')],env=env,stdout=f,stderr=subprocess.STDOUT);assert r.returncode==0
with (p/'candidate-final-cpu-top.txt').open('wb') as f:subprocess.check_call(['/data/go/bin/go','tool','pprof','-top','-nodefraction=0.01','-cum',str(p/'candidate-final-cpu.pb.gz')],env=env,stdout=f,stderr=subprocess.STDOUT)
