import pathlib,sys,json,os,time,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/storage-prefix-sample');R.mkdir()
s={'complete':False,'stage':'native source dictionary sample','source':SOURCE,'production_mutations':False,'sample_only':True};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source-sample',SOURCE);fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_storage_prefix_sample_test.go.txt');(src/'core/state/snapshots/offline_storage_prefix_sample_test.go').write_bytes(fixture)
 E.update(GTRON_STORAGE_SAMPLE_INPUT=str(R.parent/'metadata-repair-ablation/input-candidate/state-snapshots'),GTRON_STORAGE_SAMPLE_OUTPUT=str(R),GTRON_STORAGE_SAMPLE_TRACE=str(R.parent/'storage-trace/batches.jsonl'))
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','^TestOfflineStoragePrefixSample$','-count=1','-v','-timeout=180s'],src,R/'sample.log',240)
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='native storage prefix sample complete');save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
