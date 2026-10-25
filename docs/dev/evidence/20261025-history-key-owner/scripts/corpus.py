import pathlib,json,subprocess,time,os,traceback,datetime,hashlib,shutil
P=pathlib.Path('/data/gtron/history-key-owner-20261025-01');OLD=pathlib.Path('/data/gtron/history-offset-pages-20261024-01');RAW=pathlib.Path('/data/gtron/kv-accessor-analysis-20261024-01');state={'complete':False,'production_mutations':False,'phase':'wait-native-binaries'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'corpus-driver-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
try:
 save();start=time.monotonic()
 while True:
  f=P/'native-status.json';s=json.loads(f.read_text()) if f.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('native validation failed')
  if s.get('experiment_binaries_ready'):break
  if time.monotonic()-start>3600:raise TimeoutError('native binary')
  time.sleep(2)
 assert shutil.disk_usage(str(P)).free>300*(1<<30)
 original=(RAW/'source-manifest.json').read_bytes();m=json.loads(original);changed={}
 for line in (OLD/'corpus-files.jsonl').open():
  x=json.loads(line);n=x['candidate']['path'];assert n not in changed;changed[n]=x['candidate']
 assert len(changed)==19949
 for i,r in enumerate(m['segments']):
  if r['path'] in changed:m['segments'][i]=changed.pop(r['path'])
 assert not changed
 mp=P/'corpus-source-manifest.json';mp.write_text(json.dumps(m));state['source_manifest_sha256']=hashlib.sha256(mp.read_bytes()).hexdigest();state['original_manifest_sha256']=hashlib.sha256(original).hexdigest();state['scope']='All 19949 V9 accessors; not a complete restorable node.'
 state['phase']='transform-all-v9-accessors';save();env=dict(os.environ,GOMAXPROCS='4',GOMEMLIMIT='3GiB',GTRON_V10_ROOT=str(P),GTRON_V10_SOURCE=str(OLD/'corpus-v9'),GTRON_V10_TARGET=str(P/'corpus-v10'),GTRON_V10_MANIFEST=str(mp),GTRON_V10_LABEL='corpus',GTRON_V10_COMPLETE_INPUT='0',GTRON_V10_WORKERS='4')
 with (P/'corpus.log').open('wb') as out:subprocess.run([str(P/'snapshots.test'),'-test.run=^TestOfflineV10Corpus$','-test.v','-test.timeout=150m'],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=9050,check=True)
 result=json.loads((P/'corpus-status.json').read_text());assert result['complete'];a=result['audit'];assert a['SourceBytes']==126009832509 and a['Records']==14302423656 and a['Keys']==2443423104
 state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
