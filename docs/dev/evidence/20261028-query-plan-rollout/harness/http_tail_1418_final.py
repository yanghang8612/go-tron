"""Cold all actual HTTP database read paths; retain mandatory startup caches.
A separate traced discovery proves no DB file mmap. Constructor-only ancient
metadata reads are documented, not misrepresented as timed HTTP reads.
"""
import pathlib,sys,re,shlex
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
code=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/whole_query_cold.py').decode().split('\ntry:\n until=',1)[0]
exec(compile(code,'private-file-cold-definitions','exec'))
primary=R;R=primary/'tail-path-cold-1418-final';R.mkdir();input_=primary/'input'
proof={};readset=set();copy_started=time.monotonic();copied=0
s={'complete':False,'stage':'wait for primary cold HTTP seal','ablation':True,'tail_blocks':1418,'source':SOURCE,'production_mutations':False,'cache':'all DB files actually read by HTTP independently isolated, file pages zero before fresh engine start and again after ready; mandatory startup/application metadata caches retained; no DB mmap'};requests=[]
class AccessOnly(AllDatabaseOpens):
 def drain(self):
  while True:
   try:b=os.read(self.fd,1<<20)
   except BlockingIOError:return
   if not b:return
   p=0
   while p+16<=len(b):
    wd,mask,cookie,n=struct.unpack_from('iIII',b,p);p+=16;name=os.fsdecode(b[p:p+n].split(b'\0',1)[0]);p+=n
    if mask&0x4000:self.overflow=True
    if mask&1 and not mask&0x40000000 and wd in self.paths and name:self.seen.add(self.paths[wd]+'/'+name)
try:
 save();until=time.monotonic()+1800
 while not (primary/'tail-path-cold-1418/archive-receipt.json').exists():
  status=json.loads((primary/'tail-path-cold-1418/whole-query-status.json').read_text());assert status['stage']!='failed',status
  assert time.monotonic()<until,'primary cold HTTP timeout';time.sleep(2)
 s['production_before']=identity();s['stage']='build controlled V3 owner-span-only reader and retained-tail fixture';save()
 candidate_src=archive_source('source-candidate',SOURCE)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],candidate_src,R/'build-candidate.log',300)
 src=archive_source('source-control',SOURCE)
 for target,name in [('core/offline_tail_directory_ablation_test.go','offline_tail_directory_ablation_test.go.txt'),('core/rawdb/offline_pack_proof_bridge.go','offline_pack_proof_bridge.go.txt'),('core/rawdb/pebbledb/offline_flush_bridge.go','offline_flush_bridge.go.txt')]:
  (src/target).write_bytes(git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/'+name))
 p=src/'core/rawdb/state_history_query_keys.go';original=p.read_text();b=original.replace('"errors"','"errors"\n "os"',1);start=b.index('func iterateFirstStateHistoryKeyTail(');pos=b.index('\n',start)+1
 patch='\tif os.Getenv("GTRON_OFFLINE_DISABLE_QUERY_KEYS")=="1" { return iterateFirstStateHistoryTail(ctx,db,from,to,target,head,owner,fn) }\n';b=b[:pos]+patch+b[pos:];p.write_text(b)
 dump('control-source-patch.json',{'file':'core/rawdb/state_history_query_keys.go','original_sha256':hashlib.sha256(original.encode()).hexdigest(),'patched_sha256':hashlib.sha256(b.encode()).hexdigest(),'inserted_branch':patch,'V3_owner_spans_retained':True,'scope':'diagnostic only; candidate HTTP binary unchanged'})
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'control-http.test')],src,R/'build-control.log',300)
 env=dict(E,GTRON_WHOLE_QUERY_INPUT=str(input_),GTRON_WHOLE_QUERY_OUTPUT=str(R),GTRON_OFFLINE_TAIL_BLOCKS='1418');s['stage']='verify every retained pack and lower only the private derived posting watermark';save()
 with (R/'prepare-tail.log').open('wb') as log:subprocess.check_call([str(R/'control-http.test'),'-test.run=^TestOfflineTailDirectoryAblation$','-test.v','-test.timeout=5m'],cwd=str(R),env=env,stdout=log,stderr=subprocess.STDOUT,timeout=360)
 ablation=json.loads((R/'tail-ablation-result.json').read_text());assert ablation['complete'] and len(ablation['retained_packs'])==1418
 control=R/'span-only-http.sh';control.write_text('#!/bin/sh\nexport GTRON_OFFLINE_DISABLE_QUERY_KEYS=1\nexec '+shlex.quote(str(R/'control-http.test'))+' "$@"\n');control.chmod(0o700)
 s['production_before']=identity();save()
 versions=[('span-only',control),('candidate',R/'http.test')];heights=[(9000000,207),(10000000,213),(15000000,238),(20000000,249)]
 s['binary_sha256']={'span-only':hashlib.sha256((R/'control-http.test').read_bytes()).hexdigest(),'candidate':hashlib.sha256((R/'http.test').read_bytes()).hexdigest()};s['prepared_source']=json.loads((primary/'candidate-status.json').read_text())['source'];save()
 for version,exe in versions:
  name='traced-discovery-'+version;wrapper=R/(name+'.sh');trace=R/name/'syscalls.log'
  wrapper.write_text('#!/bin/sh\nexec /bin/strace -f -y -s 1 -e trace=read,pread64,readv,preadv,preadv2,mmap,mremap -o '+shlex.quote(str(trace))+' '+shlex.quote(str(exe))+' "$@"\n');wrapper.chmod(0o700)
  boot=AccessOnly()
  with host(name,wrapper,input_) as ready:
   boot_reads=boot.close();observer=AccessOnly()
   try:
    for height,want in heights:request(ready,name,height,want,0,'traced discovery excluded from performance')
   finally:query_reads=observer.close()
  traced_reads=set();mapped=set()
  for line in trace.read_text().splitlines():
   for path in re.findall(r'<([^>]+)>',line):
    if not path.startswith(str(input_)+'/'):continue
    rel=path[len(str(input_))+1:]
    if rel.split('/')[0] not in kinds:continue
    if 'mmap(' in line:mapped.add(rel)
    elif re.search(r'\b(?:read|pread64|readv|preadv|preadv2)\(',line):traced_reads.add(rel)
  assert not mapped,('DB file mapping requires stronger lifecycle observation',mapped)
  assert boot_reads|query_reads<=traced_reads,('trace parser lost actual access',sorted((boot_reads|query_reads)-traced_reads))
  dump(name+'/read-proof.json',{'startup_reads':sorted(boot_reads),'http_reads':sorted(query_reads),'all_traced_read_files':sorted(traced_reads),'db_file_mmaps':sorted(mapped),'inotify_overflow':False})
  readset.update(query_reads)
 s.update(stage='isolate full files actually read by HTTP across all databases',query_files=len(readset),query_file_bytes=sum((input_/p).stat().st_size for p in readset));save();isolate(readset)
 accepted=[];rejected=[]
 for round_ in range(1):
  for height,want in heights:
   for version,exe in (versions if round_==0 else list(reversed(versions))):
    for attempt in range(5):
     name='path-cold-%d-%s-%d-%d'%(round_,version,height,attempt);s['stage']=name;save();before=residency(True);assert sum(p['resident_pages'] for p in before)==0
     with host(name,exe,input_) as ready:
      dump(name+'/pre-start-residency.json',before);dump(name+'/startup-residency.json',residency(False));dispatch=residency(True);dump(name+'/dispatch-residency.json',dispatch);assert sum(p['resident_pages'] for p in dispatch)==0,'HTTP data file pages not cold at dispatch'
      observer=AccessOnly()
      try:rows=[request(ready,name,height,want,0,'all four DB HTTP read-path file pages zero at dispatch; normal startup metadata caches retained'),request(ready,name,height,want,1,'same process repeat')]
      finally:seen=observer.close()
     missing=seen-readset;stable=True
     for rel in seen&readset:
      st=(input_/rel).stat();p=proof[rel];stable=stable and st.st_nlink==1 and (st.st_dev,st.st_ino,st.st_size)==(p['private_dev'],p['private_ino'],p['bytes'])
     dump(name+'/actual-read-proof.json',{'paths':sorted(seen),'missing':sorted(missing),'overflow':False,'post_run_inodes_stable':stable,'db_mmap_source_audit_and_traced_discovery':True});assert stable
     if missing:rejected.append({'case':name,'new_read_files':sorted(missing)});dump('rejected.json',rejected);isolate(missing);continue
     accepted.append({'case':name,'round':round_,'version':version,'height':height,'first_seconds':rows[0]['seconds_loopback_http'],'repeat_seconds':rows[1]['seconds_loopback_http'],'dispatch_resident_pages':0,'post_run_inodes_stable':True});dump('accepted.json',accepted);break
    else:raise AssertionError('HTTP read closure did not stabilize')
 s['production_after']=identity();s['production_identity_changed']=s['production_before']!=s['production_after'];s.update(complete=True,stage='four-height full HTTP cold data-path matrix complete',accepted=len(accepted),all_returns_match=True);save()
 target=pathlib.Path('/tmp/AAA-gtron-http-tail-1418-borrowed-20261029-final.zip');assert not target.exists()
 with zipfile.ZipFile(str(target),'x',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(R.rglob('*')):
   if p.is_file() and p.suffix in ['.json','.jsonl','.log']:z.write(str(p),str(p.relative_to(R)))
 receipt={'path':str(target),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest()};dump('archive-receipt.json',receipt);print(json.dumps(receipt),flush=True)
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
