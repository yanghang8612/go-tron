import pathlib,subprocess,os,json,datetime
p=pathlib.Path('/data/gtron/compact-v3-validation-20261019-01');s=p/'source-v3';env=dict(os.environ,PATH='/data/go/bin:'+os.environ['PATH'],GOPATH='/root/go',GOCACHE='/root/.cache/go-build',GOTOOLCHAIN='local',CGO_ENABLED='0',GOMAXPROCS='1',GOMEMLIMIT='1200MiB')
steps=[('correctness',['/data/go/bin/go','test','./core/state','./core/rawdb/freezer','./core/state/snapshots','-run','CompactStateHistory|VotesPage|BusinessSample|HistoryIndexPagesVirtual|RelativeTransactionIndexWidths','-count=1','-timeout','10m'],{}),('hot-sst',['/data/go/bin/go','run','./cmd/compact-corpus-measure','--corpus-dir',str(p/'corpus'),'--out-dir',str(p/'hot-sst')],{}),('tx-index',['/data/go/bin/go','test','./core/rawdb/freezer','-run','^TestRelativeTransactionIndexPinnedCorpus$','-count=1','-v','-timeout','40m'],{'GTRON_TX_INDEX_CORPUS':'/data/gtron/other-storage-distribution-20261019-0245/txindex-pinned','GTRON_INDEX_MEASURE_OUT':str(p/'tx-index')}),('history-index',['/data/go/bin/go','test','./core/state/snapshots','-run','^TestHistoryIndexPagesPinnedCorpus$','-count=1','-v','-timeout','15m'],{'GTRON_HISTORY_INDEX_CORPUS':'/data/gtron/other-storage-distribution-20261019-0245/cold-pinned','GTRON_INDEX_MEASURE_OUT':str(p/'history-index')}),('votes',['/data/go/bin/go','test','./core/state','-run','^TestVotesIndexSpaceMeasurement$','-count=1','-v','-timeout','15m'],{'GTRON_VOTES_MEASURE_OUT':str(p/'votes')}),('read-bench',['/data/go/bin/go','test','./core/rawdb','-run','^$','-bench','BenchmarkStateLatestUint64Read','-benchtime=500ms','-count=3'],{})]
done=[]
try:
 for name,args,extra in steps:
  (p/'runtime-status.json').write_text(json.dumps({'complete':False,'phase':name,'done':done,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}))
  with (p/(name+'.log')).open('w') as f:subprocess.run(args,cwd=s,env=dict(env,**extra),stdout=f,stderr=subprocess.STDOUT,check=True)
  done.append(name)
 (p/'runtime-status.json').write_text(json.dumps({'complete':True,'done':done,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}))
except BaseException as e:
 (p/'runtime-status.json').write_text(json.dumps({'complete':False,'phase':name,'done':done,'error':repr(e)}));raise
