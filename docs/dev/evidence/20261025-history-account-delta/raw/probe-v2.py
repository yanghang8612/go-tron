import pathlib,json,os,hashlib,subprocess,datetime,traceback
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';s={'complete':False,'production_mutations':False}
try:
 assert json.loads((P/'probe-driver-status.json').read_text())['complete']
 for n in ['probe-rows.jsonl','probe-status.json','probe.log','probe-driver-status.json']:
  x=P/n;assert not (P/(n+'.v1')).exists();x.rename(P/(n+'.v1'))
 os.link(str(P/'probe.test'),str(P/'probe-v1.test'))
 for n in ['history_account_delta.go','history_account_delta_test.go','history_account_delta_columns.go']:
  x=S/'core/state/snapshots'/n;tmp=x.with_suffix('.next');tmp.write_bytes((P/n).read_bytes());tmp.replace(x)
 x=S/'core/state/snapshots/offline_account_delta_probe_test.go';x.write_bytes((P/'offline_account_delta_probe_v2_test.go').read_bytes())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 for label,cmd in [('probe-v2-unit',['/data/go/bin/go','test','./core/state/snapshots','-run','^TestAccountDelta','-count=1','-timeout=120s']),('probe-v2-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'probe.test')]),('probe-v2',['%s/probe.test'%P,'-test.run=^TestOfflineAccountDeltaProbe$','-test.v','-test.timeout=30m'])]:
  s['phase']=label;(P/'probe-v2-driver-status.json').write_text(json.dumps(s))
  with (P/(label+'.log')).open('wb') as out:subprocess.run(cmd,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,timeout=1900,check=True)
 s.update(complete=True,phase='complete',utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
except BaseException as e:s.update(phase='failed',error=repr(e),traceback=traceback.format_exc());raise
finally:(P/'probe-v2-driver-status.json').write_text(json.dumps(s,indent=2))
