import pathlib,sys,json,time,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-distribution');R.mkdir()
s={'complete':False,'stage':'compile bounded account distribution study','source':SOURCE,'production_mutations':False,'index_outputs':False};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source-study',SOURCE)
 fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_account_distribution_test.go.txt');(src/'core/state/snapshots/offline_account_distribution_test.go').write_bytes(fixture)
 env=dict(E,GTRON_ACCOUNT_STUDY_INPUT=str(R.parent/'account-gap-ablation/input-1521-gaps/state-snapshots'),GTRON_ACCOUNT_STUDY_OUTPUT=str(R))
 s['stage']='authenticate all 1521 unknown account dictionaries';save()
 with (R/'study.log').open('wb') as log:subprocess.check_call(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','^TestOfflineAccountRoutingDistribution$','-count=1','-v','-timeout=20m'],env=env,cwd=str(src),stdout=log,stderr=subprocess.STDOUT,timeout=1260)
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='account distribution study complete');save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
