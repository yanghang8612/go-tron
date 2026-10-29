"""Prepare optional indexes only on the private mutable clone, no production work."""
import pathlib,sys,json,traceback,time,hashlib
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/whole-query-cold-20261029-01')
s={'complete':False,'stage':'build native candidate','source':SOURCE,'production_mutations':False,'evictions':False}
try:
 s['production_before']=identity();save();assert json.loads((R/'prepare-status.json').read_text())['complete']
 assert json.loads((R/'nested-symlinks.json').read_text())==[], 'nested DB symlink requires independent audit'
 for p in json.loads((R/'mutable-copy-proof.json').read_text()):
  st=(R/'input'/p['path']).stat();origin=pathlib.Path(p['source']).stat();assert st.st_nlink==1 and (st.st_dev,st.st_ino)!=(origin.st_dev,origin.st_ino)
 src=archive_source('source-candidate',SOURCE)
 for target,name in [('core/state/snapshots/offline_whole_query_prepare_test.go','offline_whole_query_prepare_test.go.txt'),('core/rawdb/pebbledb/offline_flush_bridge.go','offline_flush_bridge.go.txt')]:
  (src/target).write_bytes(git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/'+name))
 command(['/data/go/bin/go','test','-tags=sapling','./core/rawdb','-run','TestQueryKeys|TestQueryTail|TestHistoryOwnerSpans','-count=1','-timeout=120s'],src,R/'native-focused.log',180)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/'prepare.test')],src,R/'build-prepare.log',300)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build-http.log',300)
 s['binary_sha256']={'candidate':hashlib.sha256((R/'http.test').read_bytes()).hexdigest(),'baseline':hashlib.sha256(pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-runtime/http.test').read_bytes()).hexdigest()}
 s['stage']='complete exact account coverage and hot tail directories';save()
 env=dict(E,GTRON_WHOLE_QUERY_INPUT=str(R/'input'),GTRON_WHOLE_QUERY_OUTPUT=str(R))
 with (R/'prepare-native.log').open('wb') as log:subprocess.check_call([str(R/'prepare.test'),'-test.run=^TestOfflineWholeQueryPrepare$','-test.v','-test.timeout=40m'],env=env,cwd=str(R),stdout=log,stderr=subprocess.STDOUT,timeout=2460)
 assert json.loads((R/'account-result.json').read_text())['complete'] and json.loads((R/'tail-result.json').read_text())['complete']
 s['production_after']=identity();s['production_identity_changed']=s['production_before']!=s['production_after'];s.update(complete=True,stage='all fixed-copy auxiliary preparation complete');save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
