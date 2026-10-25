import pathlib,json,subprocess,os,time,hashlib,traceback,datetime
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');state={'complete':False,'production_mutations':False,'scope':'same fixed chaindata while cold snapshot trios are converted independently'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'muhash-driver-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
def fingerprint():
 d=P/'candidate-input/chaindata';return {x.name:dict(inode=x.stat().st_ino,bytes=x.stat().st_size,mtime_ns=x.stat().st_mtime_ns)for x in d.iterdir() if x.is_file() and (x.suffix in ('.sst','.log') or x.name=='CURRENT' or x.name.startswith(('MANIFEST-','OPTIONS-')))}
try:
 state['phase']='wait-bounded-binary';save();start=time.monotonic()
 while True:
  a=json.loads((P/'native-status.json').read_text());l=json.loads((P/'layout-status.json').read_text())
  if a.get('phase')=='failed' or l.get('phase')=='failed':raise RuntimeError('final preparation failed')
  if 'cache-byte-budget' in a.get('candidate','') and a.get('experiment_binaries_ready') and l.get('phase') in ('final-convert-and-strictly-check','complete') and (P/'candidate-input/chaindata').exists():break
  if time.monotonic()-start>7200:raise TimeoutError('bounded binary')
  time.sleep(3)
 state['binary_sha256']=hashlib.sha256((P/'candidate.test').read_bytes()).hexdigest();state['chaindata_before']=fingerprint();state['phase']='full-current-state-muhash';save()
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_FIXED_INPUT=str(P/'candidate-input'),GTRON_FIXED_CENSUS=str(P/'muhash-census.json'))
 with (P/'muhash.log').open('wb')as f:subprocess.run([str(P/'candidate.test'),'-test.run=^TestOfflineRequestFixedMuHash$','-test.v','-test.timeout=30m'],env=env,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=1850)
 report=json.loads((P/'muhash-census.json').read_text());assert report['complete'] and report['muhash_verified'] and report['block_num']==11818192 and report['tx_num']==601386177
 assert state['binary_sha256']==hashlib.sha256((P/'candidate.test').read_bytes()).hexdigest();state['chaindata_after']=fingerprint();assert state['chaindata_before']==state['chaindata_after'];state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
