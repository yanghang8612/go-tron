"""Cold all actual HTTP database read paths; retain mandatory startup caches.
A separate traced discovery proves no DB file mmap. Constructor-only ancient
metadata reads are documented, not misrepresented as timed HTTP reads.
"""
import pathlib,sys,re,shlex
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
code=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/whole_query_cold.py').decode().split('\ntry:\n until=',1)[0]
exec(compile(code,'private-file-cold-definitions','exec'))
primary=R;R=pathlib.Path('/data/gtron/maintenance/system-route-demand-20261029-01');R.mkdir();input_=primary/'input'
proof={};readset=set();copy_started=time.monotonic();copied=0
s={'complete':False,'stage':'build controlled HTTP pair','ablation':True,'tail_blocks':1418,'query_filter_format':4,'source':SOURCE,'production_mutations':False,'cache':'all DB files actually read by HTTP independently isolated, file pages zero before fresh engine start and again after ready; mandatory startup/application metadata caches retained; no DB mmap'};requests=[]
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
def disable_query_filter(src):
 p=src/'core/rawdb/state_history_query_keys.go';b=p.read_text();b=b.replace('"errors"','"errors"\n "os"',1);pos=b.index('\n',b.index('func iterateFirstStateHistoryKeyTail('))+1;b=b[:pos]+'\tif os.Getenv("GTRON_OFFLINE_DISABLE_QUERY_KEYS")=="1" { return iterateFirstStateHistoryTail(ctx,db,from,to,target,head,owner,fn) }\n'+b[pos:];p.write_text(b)
def private_cold(label):
 for retry in range(15):
  rows=residency(True);remaining=[r for r in rows if r['resident_pages']]
  if not remaining:return rows
  time.sleep(.2)
 raise AssertionError(('private pages remain resident',label,remaining))
try:
 s['production_before']=identity();s['baseline_source']='a34afcd1cfb4ca8b4ede4b68cdce6e568c6e387d';save()
 srcs={}
 for name,sha in [('baseline',s['baseline_source']),('candidate',SOURCE)]:
  src=archive_source('source-'+name,sha);srcs[name]=src;disable_query_filter(src)
  command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/(name+'-http.test'))],src,R/('build-'+name+'.log'),300)
  wrapper=R/(name+'-http.sh');wrapper.write_text('#!/bin/sh\nexport GTRON_OFFLINE_DISABLE_QUERY_KEYS=1\nexec '+shlex.quote(str(R/(name+'-http.test')))+' "$@"\n');wrapper.chmod(0o700)
 candidate=srcs['candidate'];(candidate/'core/state/snapshots/offline_storage_bootstrap_test.go').write_bytes(git('show',SOURCE+':docs/dev/evidence/20261029-system-route-demand/harness/offline_storage_bootstrap_test.go.txt'))
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/'snapshots.test')],candidate,R/'build-snapshots.log',300)
 env=dict(E,GTRON_WHOLE_QUERY_INPUT=str(input_),GTRON_WHOLE_QUERY_OUTPUT=str(R))
 # Preserve original catalog evidence; only the private auxiliary storage refs change.
 shutil.copy2(str(input_/'state-snapshots/manifest.json'),str(R/'manifest-before.json'))
 versions=[('baseline',R/'baseline-http.sh'),('candidate',R/'candidate-http.sh')];heights=[(9000000,207),(10000000,213),(15000000,238),(20000000,249)]
 s['binary_sha256']={n:hashlib.sha256((R/(n+'-http.test')).read_bytes()).hexdigest() for n,_ in versions};save()
 for version,exe in versions:
  name='partial-'+version
  with host(name,exe,input_) as ready:
   request(ready,name,9000000,207,0,'partial storage catalog; fresh process OS cache uncontrolled')
   request(ready,name,9000000,207,1,'partial catalog same-process repeat')
 s['stage']='project missing lightweight storage pages on private fixed input';save()
 with (R/'bootstrap.log').open('wb') as log:subprocess.check_call([str(R/'snapshots.test'),'-test.run=^TestOfflineStorageBootstrap$','-test.v','-test.timeout=20m'],cwd=str(R),env=env,stdout=log,stderr=subprocess.STDOUT,timeout=1260)
 assert json.loads((R/'bootstrap-result.json').read_text())['complete']
 shutil.copy2(str(input_/'state-snapshots/manifest.json'),str(R/'manifest-after.json'))
 s['stage']='traced discovery on identical completed auxiliary catalog';save()
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
 for round_ in range(2):
  for height,want in heights:
   for version,exe in (versions if round_==0 else list(reversed(versions))):
    for attempt in range(5):
     name='path-cold-%d-%s-%d-%d'%(round_,version,height,attempt);s['stage']=name;save();before=private_cold(name+'-before')
     with host(name,exe,input_) as ready:
      dump(name+'/pre-start-residency.json',before);dump(name+'/startup-residency.json',residency(False));dispatch=private_cold(name+'-dispatch');dump(name+'/dispatch-residency.json',dispatch)
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
 s['production_after']=identity();assert s['production_before']==s['production_after'],s
 s.update(complete=True,stage='completed identical-catalog four-height cold HTTP matrix',accepted=len(accepted),all_returns_match=True);save()
 target=pathlib.Path('/tmp/AAA-gtron-system-route-demand-20261029-final.zip');assert not target.exists()
 with zipfile.ZipFile(str(target),'x',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(R.rglob('*')):
   if p.is_file() and p.suffix in ['.json','.jsonl','.log'] and 'source-baseline' not in p.parts and 'source-candidate' not in p.parts and not p.name.startswith('manifest-'):z.write(str(p),str(p.relative_to(R)))
 receipt={'path':str(target),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest()};dump('archive-receipt.json',receipt);print(json.dumps(receipt),flush=True)
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
