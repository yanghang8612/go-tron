"""After the primary seal, exercise a real retained 256-block canonical tail.
Only the private derived posting frontier is lowered; label this as an ablation.
"""
import pathlib,sys,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
code=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/whole_query_cold.py').decode().split('\ntry:\n until=',1)[0]
exec(compile(code,'whole-file-cold-definitions','exec'))
primary=R;R=primary/'tail-ablation-256';R.mkdir();input_=primary/'input'
proof={};readset=set();copy_started=time.monotonic();copied=0;proof_file=R/'whole-file-private-proof.json'
s={'complete':False,'stage':'wait for primary sealed full-cold matrix','source':SOURCE,'production_mutations':False,'ablation':True,'tail_blocks':256,'cache':'same independent private full-file cold closure; lowered derived watermark only'};requests=[]
try:
 save();until=time.monotonic()+1800
 while not (primary/'archive-receipt.json').exists():
  p=primary/'whole-query-status.json'
  if p.exists():assert json.loads(p.read_text())['stage']!='failed',p.read_text()
  assert time.monotonic()<until,'primary timeout';time.sleep(2)
 s['production_before']=identity();s['stage']='compile private tail ablation fixture';save()
 src=primary/'source-candidate';(src/'core/offline_tail_directory_ablation_test.go').write_bytes(git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_tail_directory_ablation_test.go.txt'))
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'prepare-tail.test')],src,R/'build-tail.log',300)
 env=dict(E,GTRON_WHOLE_QUERY_INPUT=str(input_),GTRON_WHOLE_QUERY_OUTPUT=str(R),GTRON_OFFLINE_TAIL_BLOCKS='256');s['stage']='derive directories and lower private posting watermark';save()
 with (R/'prepare-tail.log').open('wb') as log:subprocess.check_call([str(R/'prepare-tail.test'),'-test.run=^TestOfflineTailDirectoryAblation$','-test.v','-test.timeout=5m'],cwd=str(R),env=env,stdout=log,stderr=subprocess.STDOUT,timeout=360)
 assert json.loads((R/'tail-ablation-result.json').read_text())['complete']
 versions=[('baseline',pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-runtime/http.test')),('candidate',primary/'http.test')];heights=[(9000000,207),(10000000,213),(15000000,238),(20000000,249)]
 for version,exe in versions:
  name='tail-discovery-'+version;s['stage']=name;save();observer=AllDatabaseOpens()
  try:
   with host(name,exe,input_) as ready:
    for height,want in heights:request(ready,name,height,want,0,'ablation discovery; excluded')
  finally:seen=observer.close()
  dump(name+'/all-database-opens.json',sorted(seen));readset.update(seen)
 s['stage']='isolate extra tail files';save();isolate(readset);accepted=[]
 for height,want in heights:
  for version,exe in versions:
   for attempt in range(5):
    name='tail-cold-%s-%d-%d'%(version,height,attempt);s['stage']=name;save();before=residency(True);assert sum(p['resident_pages'] for p in before)==0
    observer=AllDatabaseOpens();rows=[]
    try:
     with host(name,exe,input_) as ready:
      dump(name+'/pre-start-residency.json',before);dump(name+'/startup-residency.json',residency(False));dispatch=residency(True);dump(name+'/dispatch-residency.json',dispatch)
      rows.append(request(ready,name,height,want,0,'full-file cold first request; 256 canonical blocks excluded from derived posting watermark'))
      rows.append(request(ready,name,height,want,1,'same process repeat'))
    finally:seen=observer.close()
    missing=seen-readset;dump(name+'/all-database-opens.json',{'paths':sorted(seen),'missing':sorted(missing),'overflow':False})
    if missing:isolate(missing);continue
    accepted.append({'case':name,'version':version,'height':height,'first_seconds':rows[0]['seconds_loopback_http'],'repeat_seconds':rows[1]['seconds_loopback_http'],'dispatch_resident_pages':sum(p['resident_pages'] for p in dispatch)});dump('accepted.json',accepted);break
   else:raise AssertionError('tail closure did not stabilize')
 s['production_after']=identity();s['production_identity_changed']=s['production_before']!=s['production_after'];s.update(complete=True,stage='256-block unindexed-tail full HTTP cold comparison complete',all_returns_match=True,accepted=len(accepted));save()
 target=pathlib.Path('/tmp/AAA-gtron-whole-query-tail-20261029-final.zip');assert not target.exists()
 with zipfile.ZipFile(str(target),'x',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(R.rglob('*')):
   if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log']:z.write(str(p),str(p.relative_to(R)))
 receipt={'path':str(target),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest()};dump('archive-receipt.json',receipt);print(json.dumps(receipt),flush=True)
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
