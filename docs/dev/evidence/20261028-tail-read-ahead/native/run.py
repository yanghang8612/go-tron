import pathlib,sys,json,traceback,os
O=pathlib.Path('/data/gtron/maintenance/routing-bootstrap-20261028-01')
prefix=(O/'native-validation.py').read_text().split('\ntry:\n save();assert len(SOURCE)',1)[0]
exec(compile(prefix,'reused-fixed-http-host','exec'))
R=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01');R.mkdir(exist_ok=True)
s={'complete':False,'source':SOURCE,'base':'cda883baa52a1f3134e3d459209a52e8ff3b123e','production_mutations':False,'full_muhash_repeated':False,'full_restore_repeated':False,'stage':'native candidate build'};requests=[]
try:
 s['production_before']=identity();save()
 src=archive_source('source-candidate',SOURCE)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','Test(HistoryOrderedReads|HistoryPhysicalPages|HistoryMixedBatch|HistoryAccessorPages|ManagerBatchesFirst)','-count=1','-timeout=120s'],src,R/'focused.log',180)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'candidate-http.test')],src,R/'build.log')
 baseline=O/'candidate-http.test';candidate=R/'candidate-http.test';partial=O/'candidate-maintenance-input'
 for name,exe,input_ in [('baseline-1',baseline,partial),('candidate-1',candidate,partial),('candidate-2',candidate,partial),('baseline-2',baseline,partial),('candidate-complete',candidate,A/'http-performance-priority/candidate-input')]:
  http_case(name,exe,input_)
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='HTTP matrix complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
