import pathlib,subprocess,os,shutil,json,traceback,datetime,hashlib,base64,zlib,time,gzip,re,zipfile,pwd
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');M=A/'metadata-v3';Q=A/'metadata-cold-v3';F=A/'metadata-final';F.mkdir(mode=0o700,exist_ok=True)
s={'complete':False,'stage':'wait independent and OS-cold checks','production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(F/'status.json').write_text(json.dumps(s,indent=2))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def put(p,b):p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.final-tmp');tmp.write_bytes(b);tmp.replace(p)
env=dict(os.environ,GOCACHE='/data/gtron/deploy-cache/go-build',GOPATH='/root/go',GOMODCACHE='/root/go/pkg/mod',GOTOOLCHAIN='local',CGO_ENABLED='1',CC='/opt/rh/devtoolset-11/root/usr/bin/gcc',CXX='/opt/rh/devtoolset-11/root/usr/bin/g++',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def run(cmd,log,ev=env,cwd=None):
 with (F/log).open('wb') as f:subprocess.check_call(cmd,env=ev,cwd=cwd,stdout=f,stderr=subprocess.STDOUT)
try:
 prior=json.loads((F/'status.json').read_text());(F/'initial-audit-failure.json').write_text(json.dumps(prior,indent=2))
 s.update(stage='repair incomplete initial private-file coverage');save()
 packet=json.loads(zlib.decompress((A/'metadata-state-final.zlib').read_bytes()));src=F/'source';assert {n:sha(src/n) for n in packet['product_after']}==packet['product_after']
 assert 'ok' in (F/'state-tests.log').read_text() and 'ok' in (F/'core-focused-tests.log').read_text()
 proof=json.loads((Q/'private-files-proof.json').read_text());extra=json.loads((Q/'private-extra-proof.json').read_text());original_count=len(proof);known={r['path'] for r in proof+extra};baseline_refs={r['path']:r for r in json.loads((Q/'baseline/manifest.json').read_text())['segments']}
 def trace_paths(path,catalog):
  refs={r['path'] for r in json.loads((catalog/'manifest.json').read_text())['segments']};paths=set();prefix=str(catalog)+'/'
  for line in path.read_text().splitlines():
   match=re.search(r'openat\([^,]+, "([^"]+)"',line)
   if match and match.group(1).startswith(prefix):
    name=match.group(1)[len(prefix):]
    if name in refs:paths.add(name)
  return paths
 def ensure_private(paths):
  for name in sorted(paths-known):
   ref=baseline_refs[name];source=A/'baseline-catalog'/name;dest=Q/'baseline'/name;temp=dest.with_name(dest.name+'.coverage-copy');assert shutil.disk_usage(str(Q)).free>source.stat().st_size+50_000_000_000;shutil.copyfile(str(source),str(temp))
   with temp.open('rb') as f:os.fsync(f.fileno())
   digest=sha(temp);assert 'sha256:'+digest==ref['checksum'];old=source.stat();st=temp.stat();assert (old.st_dev,old.st_ino)!=(st.st_dev,st.st_ino);temp.replace(dest);other=Q/'candidate'/name;other.unlink();os.link(str(dest),str(other));proof.append({'path':name,'bytes':st.st_size,'sha256':digest,'source_dev':old.st_dev,'source_ino':old.st_ino,'private_dev':st.st_dev,'private_ino':st.st_ino,'coverage_correction':True});known.add(name)
 initial=F/'trace-baseline';paths=trace_paths(initial/'openat.log',Q/'baseline');ensure_private(paths);initial.rename(F/'trace-initial-baseline')
 out=F/'preflight-candidate';out.mkdir();e=dict(env,GTRON_NINEM_CATALOG=str(Q/'candidate'),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1')
 run(['strace','-f','-e','trace=openat','-o',str(out/'openat.log'),str(M/'candidate.test'),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],'preflight-candidate/run.log',e);ensure_private(trace_paths(out/'openat.log',Q/'candidate'))
 (Q/'private-files-proof.initial.json').write_text((Q/'private-files-proof.json').read_text());(Q/'private-files-proof.json').write_text(json.dumps(proof,indent=2));(Q/'rejected-initial-samples.json').write_text(json.dumps({'reason':'initial successful-open parser omitted unfinished open attempts; cold coverage incomplete','original_private_files':original_count,'corrected_private_files':len(proof),'excluded_samples':['os-cold-0-baseline','os-cold-1-candidate','os-cold-2-candidate','os-cold-3-baseline'],'corrected_files':[r for r in proof if r.get('coverage_correction')]},indent=2))
 libc=__import__('ctypes').CDLL(None,use_errno=True);ctypes=__import__('ctypes');mmap=__import__('mmap')
 s.update(stage='repeat OS-cold after complete file-set proof');save()
 for i,mode in enumerate(['baseline','candidate','candidate','baseline']):
  catalog=Q/mode;entries=proof+(extra if mode=='candidate' else []);name='revalidated-os-cold-%d-%s'%(i,mode)
  for r in entries:
   p=catalog/r['path'];st=p.stat();assert (st.st_dev,st.st_ino)==(r['private_dev'],r['private_ino']);fd=os.open(str(p),os.O_RDONLY)
   try:os.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED)
   finally:os.close(fd)
  per=[]
  for r in entries:
   p=catalog/r['path'];size=p.stat().st_size
   if not size:continue
   with p.open('rb') as f:
    mm=mmap.mmap(f.fileno(),size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(size),vec)==0;nr=sum(int(x)&1 for x in vec);assert nr/pages<.01;per.append({'path':r['path'],'resident_pages':nr,'pages':pages});del buf;mm.close()
  (Q/(name+'-residency.json')).write_text(json.dumps({'per_file':per,'resident_pages':sum(r['resident_pages'] for r in per),'total_pages':sum(r['pages'] for r in per),'verified_independent_files':len(entries)},indent=2));out=Q/name;out.mkdir();e=dict(env,GTRON_NINEM_CATALOG=str(catalog),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1');binary=A/'final/baseline.test' if mode=='baseline' else M/'candidate.test'
  with (out/'run.log').open('wb') as log:subprocess.check_call([str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],env=e,stdout=log,stderr=subprocess.STDOUT)
  assert json.loads((out/'rows.json').read_text())==json.loads((A/'probe/baseline-first/rows.json').read_text())
 (Q/'status-initial.json').write_text((Q/'status.json').read_text());qstatus=json.loads((Q/'status.json').read_text());qstatus.update(revalidation_complete=True,verified_private_files=len(proof),utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(Q/'status.json').write_text(json.dumps(qstatus,indent=2))
 s.update(stage='trace both private read sets, excluded from latency samples');save()
 proof=json.loads((Q/'private-files-proof.json').read_text())+json.loads((Q/'private-extra-proof.json').read_text());private={r['path']:r for r in proof};receipts={}
 for mode in ['baseline','candidate']:
  out=F/('trace-'+mode);out.mkdir();cat=Q/mode;binary=A/'final/baseline.test' if mode=='baseline' else M/'candidate.test'
  e=dict(env,GTRON_NINEM_CATALOG=str(cat),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1')
  run(['strace','-f','-e','trace=openat','-o',str(out/'openat.log'),str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],str(out.relative_to(F)/'run.log'),e)
  assert json.loads((out/'rows.json').read_text())==json.loads((A/'probe/baseline-first/rows.json').read_text())
  refs={r['path']:r for r in json.loads((cat/'manifest.json').read_text())['segments']};paths=set()
  for line in (out/'openat.log').read_text().splitlines():
   m=re.search(r'openat\([^,]+, "([^"]+)"',line)
   if m and m.group(1).startswith(str(cat)+'/'):
    rel=m.group(1)[len(str(cat))+1:]
    if rel in refs:paths.add(rel)
  assert paths and paths<=private.keys(),sorted(paths-private.keys())
  for n in paths:
   st=(cat/n).stat();r=private[n];assert (st.st_dev,st.st_ino)==(r['private_dev'],r['private_ino'])
  receipts[mode]={'paths':sorted(paths),'opened_files':len(paths),'covered_by_independent_inode_proof':True,'includes_unfinished_attempts_conservatively':True,'timing_excluded':True}
 (F/'trace-private-coverage.json').write_text(json.dumps(receipts,indent=2))
 s.update(stage='canonical manifest and production identity checks');save()
 before=json.loads((A/'baseline-catalog/manifest.json').read_text());previous=json.loads((A/'full-route/catalog/manifest.json').read_text());after=json.loads((M/'catalog/manifest.json').read_text());canon=lambda m:[r for r in m['segments'] if 'routing' not in r['kind']]
 assert canon(before)==canon(after)
 assert canon(previous)==canon(after)
 assert {k:v for k,v in previous.items() if k!='segments'}=={k:v for k,v in after.items() if k!='segments'}
 for name,m in [('before',before),('previous',previous),('after',after)]: (F/(name+'-manifest.json.gz')).write_bytes(gzip.compress(json.dumps(m,separators=(',',':')).encode()))
 routes=[r for r in after['segments'] if r['kind']=='metadata-history-routing'];assert len(routes)==116 and sum(r['rows'] for r in routes)==29658
 (F/'coverage.json').write_text(json.dumps({'canonical_refs_unchanged':True,'top_level_manifest_equal_previous_candidate':True,'sources':29658,'pages':len(routes),'metadata_page_bytes':sum(r['size'] for r in routes),'refs':routes},indent=2))
 pid=subprocess.check_output(['systemctl','show','gtron','--property=MainPID']).decode().strip().split('=')[1];p=pathlib.Path('/proc')/pid
 latest={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':pid,'start_ticks':p.joinpath('stat').read_text().split(')')[-1].split()[19],'exe':os.readlink(str(p/'exe')),'sha256':sha(p/'exe'),'service':subprocess.check_output(['systemctl','show','gtron','--property=MainPID','--property=ActiveState']).decode()}
 initial=json.loads((M/'runtime-identity.json').read_text());latest['identity_unchanged']=all(latest[k]==initial[k] for k in ['pid','start_ticks','exe','sha256']);assert latest['identity_unchanged'];(F/'production-identity.json').write_text(json.dumps(latest,indent=2))
 (F/'acceptance.json').write_text(json.dumps({'complete':True,'native_state_checks_passed':True,'private_trace_coverage_passed':True,'canonical_manifest_unchanged':True,'production_identity_unchanged':True},indent=2))
 s.update(stage='sealed evidence pack');save();files=set()
 for root in [M,Q,F]:
  for pattern in ['*.json','*.jsonl','*.log','repeat-*/*','metadata-*/*','os-cold-*/*','revalidated-*/*','preflight-*/*','restoration/*','trace-*/*']:
   files.update(p for p in root.glob(pattern) if p.is_file() and p.stat().st_size<100_000_000)
 for r in routes:files.add(M/'catalog'/r['path'])
 for n in packet['product_after']:files.add(src/n)
 for p in src.rglob('*metadata*routing*test.go'):files.add(p)
 for n in packet['files']:files.add(src/n)
 for n in ['core/state/contract_history_test.go','core/state/snapshots/history_metadata_routing_test.go']:files.add(src/n)
 files.add(A/'run-metadata-repair.py')
 for n in ['metadata-packet-v3.zlib','run-metadata-v3.py','run-metadata-cold-v3.py','run-metadata-finish.py','metadata-state-final.zlib','probe/queries.json','probe/baseline-first/rows.json']:files.add(A/n)
 dest=pathlib.Path('/tmp/AAA-gtron-metadata-routing-final-20261027.zip');tmp=dest.with_suffix('.partial');members={}
 with zipfile.ZipFile(str(tmp),'w',zipfile.ZIP_DEFLATED,allowZip64=True) as z:
  for p in sorted(files):
   name=str(p.relative_to(A));b=p.read_bytes();members[name]={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()};z.writestr(name,b)
  z.writestr('files-sha256.json',json.dumps(members,indent=2))
 tmp.replace(dest);u=pwd.getpwnam('java-tron');os.chown(str(dest),u.pw_uid,u.pw_gid);os.chmod(str(dest),0o644)
 seal={'file':str(dest),'bytes':dest.stat().st_size,'sha256':sha(dest),'members':len(members)};(A/'metadata-evidence-seal.json').write_text(json.dumps(seal,indent=2));s.update(complete=True,stage='done',seal=seal);save();print(json.dumps(s))
except Exception:s['error']=traceback.format_exc();save();raise
