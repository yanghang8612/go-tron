import pathlib,json,os,hashlib,subprocess,datetime,traceback,time,shutil
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';s={'complete':False,'production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'probe-v4-driver-status.json').write_text(json.dumps(s,indent=2))
try:
 start=time.monotonic()
 while True:
  a=json.loads((P/'native-status.json').read_text())
  if a.get('phase')=='failed':raise RuntimeError('native failed')
  if a.get('complete'):break
  if time.monotonic()-start>3600:raise TimeoutError('native preparation')
  time.sleep(3)
 for unit in ['gtron-account-delta-resume-20261025-01','gtron-account-delta-layout-20261025-01']:assert subprocess.run(['systemctl','stop',unit]).returncode in (0,5)
 for n in ['native-status.json','layout-status.json','complete-input-status.json']:
  shutil.copyfile(str(P/n),str(P/('byte-'+n)))
 old=P/'byte-source';old.mkdir()
 for n,d in json.loads((P/'source-inventory.json').read_text()).items():
  x=S/n;assert hashlib.sha256(x.read_bytes()).hexdigest()==d,n;o=old/n;o.parent.mkdir(parents=True,exist_ok=True);o.write_bytes(x.read_bytes())
 for n in ['probe-rows.jsonl','probe-status.json']:
  x=P/n;assert not (P/(n+'.v3')).exists();x.rename(P/(n+'.v3'))
 for n in ['history_account_delta.go','history_account_delta_numeric.go','history_account_delta_numeric_test.go']:
  x=S/'core/state/snapshots'/n;tmp=x.with_suffix('.numeric-next');tmp.write_bytes((P/n).read_bytes());tmp.replace(x)
 (S/'core/state/snapshots/offline_account_delta_probe_test.go').write_bytes((P/'offline_account_delta_probe_v4_test.go').read_bytes())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 for label,cmd in [('probe-v4-unit',['/data/go/bin/go','test','./core/state/snapshots','-run','^TestAccountDelta','-count=1','-timeout=120s']),('probe-v4-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'numeric-probe.test')]),('probe-v4',[str(P/'numeric-probe.test'),'-test.run=^TestOfflineAccountDeltaProbe$','-test.v','-test.timeout=30m'])]:
  s['phase']=label;save()
  with (P/(label+'.log')).open('wb') as out:subprocess.run(cmd,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,timeout=1900,check=True)
 s.update(complete=True,phase='complete');save()
except BaseException as e:s.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
