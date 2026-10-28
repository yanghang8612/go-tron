# Measure one real 64-source account refresh in the completed private ablation.
import pathlib,sys,json,subprocess,hashlib,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
P=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01');R=P/'account-build-cost';R.mkdir()
s={'complete':False,'stage':'compile native builder cost host','production_mutations':False,'canonical_data_mutations':False};requests=[]
try:
 a=json.loads((P/'account-gap-ablation/candidate-status.json').read_text());assert a['complete'];s['production_before']=identity();save()
 src=pathlib.Path('/data/gtron/query-plan-rollout-20261028-01/source');code=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/account_build_cost.go.txt');(R/'main.go').write_bytes(code)
 command(['/data/go/bin/go','build','-tags=sapling','-o',str(R/'account-cost'),str(R/'main.go')],src,R/'build.log',240)
 root=P/'account-gap-ablation/input-1521-gaps/state-snapshots';before=(root/'manifest.json').read_bytes();s['stage']='measure 64-source refresh';save()
 command([str(R/'account-cost'),str(root)],src,R/'result.json',150)
 assert (root/'manifest.json').read_bytes()==before;s['result']=json.loads((R/'result.json').read_text());s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='native account refresh cost complete',manifest_unchanged=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
