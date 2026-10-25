import pathlib,json,os,time,subprocess,traceback,datetime,hashlib
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');state={'complete':False,'production_mutations':False,'phase':'waiting-layout'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'pipeline-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
def waitstatus(name,seconds):
 begin=time.monotonic()
 while True:
  p=P/name;x=json.loads(p.read_text()) if p.exists() else {}
  if x.get('phase')=='failed':raise RuntimeError(name+' failed')
  if x.get('complete'):return
  if time.monotonic()-begin>seconds:raise TimeoutError(name)
  time.sleep(3)
try:
 save();waitstatus('layout-status.json',10500);waitstatus('native-status.json',3600)
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_FIXED_INPUT=str(P/'candidate-input'),GTRON_FIXED_CENSUS=str(P/'muhash-census.json'),GTRON_ROUTING_ROOT=str(P))
 for phase,cmd in [('muhash',[str(P/'candidate.test'),'-test.run=^TestOfflineRequestFixedMuHash$','-test.v','-test.timeout=40m']),('history-restore',[str(P/'snapshots.test'),'-test.run=^TestOfflineAccountHistoryRestore$','-test.v','-test.timeout=20m'])]:
  state['phase']=phase;save()
  with (P/(phase+'.log')).open('wb') as out:subprocess.run(cmd,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=2450,check=True)
 x=json.loads((P/'muhash-census.json').read_text());assert x['complete'] and x['muhash_verified'] and bytes(x['muhash_root']).hex()=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5';assert x['source_rows']==[3484117,481,141418493]
 x=json.loads((P/'history-restore.json').read_text());assert x['complete'] and sum(w['rows'] for w in x['windows'])==22496
 state['phase']='wait-corpus-before-timing';save();waitstatus('corpus-driver-status.json',10500)
 waitstatus('census-native-status.json',22000)
 state['phase']='full-http-comparison';save()
 with (P/'comparison.log').open('wb') as out:subprocess.run(['/usr/bin/python3',str(P/'compare.py'),'tail-read'],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=7400,check=True)
 assert json.loads((P/'tail-read-comparison/comparison-status.json').read_text())['complete'];state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
