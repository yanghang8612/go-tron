from pathlib import Path
import os,json,time,subprocess,hashlib,traceback,datetime
p=Path('/data/gtron/compact-v3-rollout-20261019-01/cold-query-opt-20261020')
s={'complete':False,'phase':'starting'}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(p/'validation-amend-status.json').write_text(json.dumps(s,indent=2))
def run(cmd,cwd,env,log,timeout):
 with (p/log).open('wb') as f:
  r=subprocess.run(cmd,cwd=str(cwd),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
  if r.returncode: raise RuntimeError(log+' rc='+str(r.returncode)+' '+(p/log).read_text()[-2500:])
try:
 env=dict(os.environ,GOTOOLCHAIN='local',GOMAXPROCS='2',GOMEMLIMIT='4GiB',CGO_ENABLED='0')
 env['PATH']='/data/go/bin:'+env.get('PATH','');env['GOPATH']='/root/go'
 env['GTRON_COLD_QUERY_CORPUS']=str(p/'corpus')
 for mode in ['candidate-final','baseline']:
  s['phase']=mode+'-build';save()
  run(['/data/go/bin/go','test','-p=2','-c','./core/state/snapshots','-o',str(p/(mode+'.test'))],p/mode,env,mode+'-amend-build.log',900)
  s['phase']=mode+'-run';save();env['GTRON_COLD_QUERY_OUT']=str(p/(mode+'-amend-results.json'))
  run(['nice','-n','15','ionice','-c3',str(p/(mode+'.test')),'-test.run=^TestOfflineColdQueryCorpus$','-test.v','-test.timeout=10m'],p/mode,env,mode+'-amend-run.log',660)
 a=json.loads((p/'baseline-amend-results.json').read_text());b=json.loads((p/'candidate-final-amend-results.json').read_text())
 assert len(a['results'])==len(b['results'])
 mismatch=[]
 for x,y in zip(a['results'],b['results']):
  if (x['Round'],x['Query'],x['Hash'])!=(y['Round'],y['Query'],y['Hash']):mismatch.append([x,y])
 assert not mismatch,mismatch
 s.update({'complete':True,'phase':'verified','compared_results':len(a['results']),'mismatches':0,'service':subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode(),'binary_sha256':hashlib.sha256(Path('/proc/10692/exe').read_bytes()).hexdigest()});save()
except BaseException as e:
 s.update({'phase':'failed','error':repr(e),'traceback':traceback.format_exc()});save()
