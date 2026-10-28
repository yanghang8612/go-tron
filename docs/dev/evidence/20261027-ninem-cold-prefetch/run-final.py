import pathlib,subprocess,os,shutil,json,traceback,datetime,hashlib,base64,zlib,time,re,ctypes,mmap,collections
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');P=A/'final';P.mkdir(mode=0o700,exist_ok=True)
s={'complete':False,'stage':'setup','production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'status.json').write_text(json.dumps(s,indent=2))
def put(p,data):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for chunk in iter(lambda:f.read(4<<20),b''):h.update(chunk)
 return h.hexdigest()
def clonefile(a,b):
 if pathlib.Path(a).name=='manifest.json':shutil.copyfile(a,b)
 else:os.link(a,b)
 return b
env=dict(os.environ,GOCACHE='/data/gtron/deploy-cache/go-build',GOPATH='/root/go',GOMODCACHE='/root/go/pkg/mod',GOTOOLCHAIN='local',CGO_ENABLED='1',CC='/opt/rh/devtoolset-11/root/usr/bin/gcc',CXX='/opt/rh/devtoolset-11/root/usr/bin/g++',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def run(mode,catalog,out,n=3,scope='',trace=False):
 out.mkdir(parents=True)
 ev=dict(env,GTRON_NINEM_CATALOG=str(catalog),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS=str(n),GTRON_NINEM_SCOPE=scope)
 cmd=[str(P/(mode+'.test')),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m']
 if trace:cmd=['strace','-f','-yy','-e','trace=openat,read,pread64,close','-o',str(out/'strace.log')]+cmd
 (out/'environment.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'catalog':str(catalog),'mode':mode,'scope':scope,'loadavg':pathlib.Path('/proc/loadavg').read_text(),'meminfo':pathlib.Path('/proc/meminfo').read_text()},indent=2))
 with (out/'run.log').open('wb') as f:subprocess.check_call(cmd,env=ev,stdout=f,stderr=subprocess.STDOUT)
 rows=json.loads((out/'rows.json').read_text());oracle=json.loads((A/'probe/baseline-first/rows.json').read_text());qs=json.loads((A/'probe/queries.json').read_text())
 if scope:oracle=[v for q,v in zip(qs,oracle) if (scope=='metadata')==(q['Domain']==metadata_domain)] # replaced with actual domain below from packet
 assert rows==oracle,(mode,scope)
 return out
try:
 save();packet=json.loads(zlib.decompress((A/'final-packet.zlib').read_bytes()));metadata_domain=packet['metadata_domain']
 # Domain is a registry identifier, never hard-code its observed integer.
 # The packet contains the independent Go-derived metadata query as its final item.
 assert json.loads((A/'probe/queries.json').read_text())[-1]['Domain']==metadata_domain
 for mode,source in [('baseline',A/'probe/baseline-source'),('full',A/'full-route/source'),('candidate',A/'full-route/source')]:
  src=P/(mode+'-source');shutil.copytree(str(source),str(src))
  harness=base64.b64decode(packet['harness'])
  if mode=='baseline':harness=harness[:harness.index(b'func TestOfflineNineMStrictRoute')]
  put(src/'core/state/snapshots/zz_offline_ninem_test.go',harness)
  if mode=='candidate':
   for name,b in packet['files'].items():put(src/name,base64.b64decode(b))
   put(src/'core/state/snapshots/zz_offline_ninem_restore_test.go',base64.b64decode(packet['restore']))
   put(src/'core/state/snapshots/zz_offline_ninem_extract_test.go',base64.b64decode(packet['extract']))
   actual={n:digest(src/n) for n in packet['product_sha256']};assert actual==packet['product_sha256'];s['product_sha256']=actual
  s.update(stage='compile '+mode);save()
  with (P/(mode+'-build.log')).open('wb') as f:subprocess.check_call(['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/(mode+'.test'))],cwd=str(src),env=env,stdout=f,stderr=subprocess.STDOUT)
 s['binaries_sha256']={mode:digest(P/(mode+'.test')) for mode in ('baseline','full','candidate')};s.update(stage='native safety tests');save()
 with (P/'native-tests.log').open('wb') as f:subprocess.check_call([str(P/'candidate.test'),'-test.run=TestSmallSystem|TestHistoryFramedHeader|TestSystemHistoryRoutingSelected|TestHistoryTxRuns','-test.v','-test.timeout=10m'],env=env,stdout=f,stderr=subprocess.STDOUT)
 with (P/'native-decoder-bench.log').open('wb') as f:subprocess.check_call([str(P/'candidate.test'),'-test.run=^$','-test.bench=^BenchmarkSystemHistoryRoutingSelected$','-test.benchtime=1s','-test.count=2'],env=env,stdout=f,stderr=subprocess.STDOUT)
 s.update(stage='trace baseline read set');save()
 trace=run('baseline',A/'baseline-catalog',P/'trace',n=1,trace=True)
 paths=set()
 for line in (trace/'strace.log').read_text().splitlines():
  m=re.search(r'openat\([^,]+, "([^"]+)".*\)\s+=\s+(\d+)',line)
  if m:
   f=pathlib.Path(m.group(1))
   try:rel=f.relative_to(A/'baseline-catalog')
   except ValueError:continue
   if str(rel)!='manifest.json' and (A/'baseline-catalog'/rel).is_file():paths.add(str(rel))
 total=sum((A/'baseline-catalog'/f).stat().st_size for f in paths)
 (P/'baseline-opened-files.json').write_text(json.dumps({'files':sorted(paths),'bytes':total,'scope':'one traced isolated cold batch; trace timings excluded from performance'},indent=2))
 s['private_copy_input_bytes']=total
 # Source SHA and inode proof for every file that will receive fadvise.
 # Unused canonical objects remain hardlinked and are never advised out.
 private=False
 if total<=180_000_000_000 and os.statvfs(str(P)).f_bavail*os.statvfs(str(P)).f_frsize>total+50_000_000_000:
  s.update(stage='independent private-file copy');save();target=P/'private-baseline';shutil.copytree(str(A/'baseline-catalog'),str(target),copy_function=clonefile);proof=[]
  manifest=json.loads((target/'manifest.json').read_text());refs={x['path']:x for x in manifest['segments']}
  for i,name in enumerate(sorted(paths)):
   a=A/'baseline-catalog'/name;b=target/name;tmp=b.with_name(b.name+'.private-copy');shutil.copyfile(str(a),str(tmp))
   with tmp.open('rb') as f:os.fsync(f.fileno())
   sha=digest(tmp);assert 'sha256:'+sha==refs[name]['checksum'];assert a.stat().st_ino!=tmp.stat().st_ino or a.stat().st_dev!=tmp.stat().st_dev;tmp.replace(b)
   proof.append({'path':name,'bytes':b.stat().st_size,'sha256':sha,'source_dev':a.stat().st_dev,'source_ino':a.stat().st_ino,'private_dev':b.stat().st_dev,'private_ino':b.stat().st_ino})
   if i%200==0:s.update(copied_files=i+1,total_copy_files=len(paths));save()
  (P/'private-files-proof.json').write_text(json.dumps(proof,indent=2));private=True
 else:s['private_copy_budget_deferred']=True;save()
 # Finish the independent full-dictionary verifier before any timed comparison.
 s.update(stage='wait independent full dictionary proof');save()
 while True:
  old=json.loads((A/'full-route/status.json').read_text())
  if old.get('error'):raise RuntimeError(old)
  if old.get('complete'):break
  time.sleep(10)
 catalog=A/'full-route/catalog'
 s.update(stage='historical full-row restoration');save()
 extract=P/'extraction';extract.mkdir();exenv=dict(env,GTRON_NINEM_CATALOG=str(catalog),GTRON_NINEM_OUTPUT=str(extract))
 with (extract/'run.log').open('wb') as f:subprocess.check_call([str(P/'candidate.test'),'-test.run=^TestOfflineNineMExtractionComparison$','-test.v','-test.timeout=20m'],env=exenv,stdout=f,stderr=subprocess.STDOUT)
 out=P/'restoration';out.mkdir();ev=dict(env,GTRON_NINEM_CATALOG=str(catalog),GTRON_NINEM_ORIGINAL=str(A/'baseline-catalog'),GTRON_NINEM_OUTPUT=str(out))
 with (out/'run.log').open('wb') as f:subprocess.check_call([str(P/'candidate.test'),'-test.run=^TestOfflineNineMRestore$','-test.v','-test.timeout=20m'],env=ev,stdout=f,stderr=subprocess.STDOUT)
 # Byte/header census authenticates the observed format identifiers, not whole files.
 counts=collections.Counter()
 for r in json.loads((A/'baseline-catalog/manifest.json').read_text())['segments']:
  if r.get('dataset')=='state-domain-change' and r['kind'] in ('history','accessor'):
   with (A/'baseline-catalog'/r['path']).open('rb') as f:b=f.read(12)
   counts[(r['kind'],b[:8].decode(errors='replace'),int.from_bytes(b[8:12],'big'))]+=1
 (P/'physical-format-census.json').write_text(json.dumps([{'kind':k[0],'magic':k[1],'version':k[2],'files':v} for k,v in counts.items()],indent=2))
 s.update(stage='interleaved process-first and warm batches');save()
 for round_ in range(2):
  for index,mode in enumerate(('baseline','full','candidate','candidate','full','baseline')):
   run(mode,A/'baseline-catalog' if mode=='baseline' else catalog,P/('repeat-%d-%d-%s'%(round_,index,mode)),n=3)
 for scope in ('system','metadata'):
  for mode in ('baseline','full','candidate'):run(mode,A/'baseline-catalog' if mode=='baseline' else catalog,P/('scope-%s-%s'%(scope,mode)),n=3,scope=scope)
 if private:
  target=P/'private-candidate';shutil.copytree(str(P/'private-baseline'),str(target),copy_function=clonefile);shutil.copyfile(str(catalog/'manifest.json'),str(target/'manifest.json'))
  extra=[]
  oldpaths={x['path'] for x in json.loads((A/'baseline-catalog/manifest.json').read_text())['segments']}
  for r in json.loads((catalog/'manifest.json').read_text())['segments']:
   if r['path'] not in oldpaths:
    a=catalog/r['path'];b=target/r['path'];b.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(str(a),str(b))
    with b.open('rb') as f:os.fsync(f.fileno())
    sha=digest(b);assert 'sha256:'+sha==r['checksum'];extra.append({'path':r['path'],'bytes':b.stat().st_size,'sha256':sha,'source_dev':a.stat().st_dev,'source_ino':a.stat().st_ino,'private_dev':b.stat().st_dev,'private_ino':b.stat().st_ino})
  (P/'private-extra-proof.json').write_text(json.dumps(extra,indent=2))
  libc=ctypes.CDLL(None,use_errno=True)
  def cold(mode,name):
   cat=P/('private-baseline' if mode=='baseline' else 'private-candidate');entries=proof+(extra if mode!='baseline' else [])
   for r in entries:
    f=os.open(str(cat/r['path']),os.O_RDONLY)
    try:os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED)
    finally:os.close(f)
   resident,totalpages=0,0;per_file=[]
   for r in entries:
    b=cat/r['path'];v=b.stat();assert (v.st_dev,v.st_ino)==(r['private_dev'],r['private_ino'])
    f=os.open(str(b),os.O_RDONLY)
    try:
     if not v.st_size:continue
     mm=mmap.mmap(f,v.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(v.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(v.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);assert nr/max(1,pages)<.01,(r['path'],nr,pages);per_file.append({'path':r['path'],'resident_pages':nr,'pages':pages});resident+=nr;totalpages+=pages;del buf;mm.close()
    finally:os.close(f)
   ratio=resident/max(1,totalpages);assert ratio<.01,(resident,totalpages,ratio)
   (P/(name+'-residency.json')).write_text(json.dumps({'per_file':per_file,'resident_pages':resident,'total_pages':totalpages,'resident_fraction':ratio,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'verified_independent_files':len(entries),'scope':'isolated private files only; no global cache drop'},indent=2))
   run(mode,cat,P/name,n=1)
  s.update(stage='private OS-cold batches');save()
  for index,mode in enumerate(('baseline','candidate','candidate','baseline')):cold(mode,'os-cold-%d-%s'%(index,mode))
 s.update(complete=True,stage='done');save()
except BaseException as e:s.update(error=repr(e),traceback=traceback.format_exc());save();raise
