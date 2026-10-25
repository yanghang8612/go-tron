import pathlib,json,subprocess,time,traceback,datetime
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');s={'complete':False,'production_mutations':False,'phase':'waiting-pipeline'}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'finalizer-status.json').write_text(json.dumps(s,indent=2))
try:
 save();start=time.monotonic()
 while True:
  f=P/'pipeline-status.json';x=json.loads(f.read_text()) if f.exists() else {}
  if x.get('phase')=='failed':raise RuntimeError('pipeline failed')
  if x.get('complete'):break
  if time.monotonic()-start>24000:raise TimeoutError('pipeline')
  time.sleep(4)
 s['phase']='fsync-rehash-account-and-seal';save()
 with (P/'finalize.log').open('wb') as out:subprocess.run(['/usr/bin/python3',str(P/'finalize.py')],stdout=out,stderr=subprocess.STDOUT,check=True,timeout=5400)
 s.update(complete=True,phase='complete');save()
except BaseException as e:s.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
