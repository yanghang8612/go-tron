import sys,pathlib,hashlib,json,zipfile,gzip,struct,statistics,collections
p=pathlib.Path(sys.argv[1]);root=pathlib.Path(sys.argv[2]);root.mkdir(parents=True,exist_ok=True)
seal=json.loads(pathlib.Path(sys.argv[3]).read_text());assert p.stat().st_size==seal['bytes'];assert hashlib.sha256(p.read_bytes()).hexdigest()==seal['sha256']
with zipfile.ZipFile(p) as z:
 names=z.namelist();assert len(set(names))==len(names)
 expected=json.loads(z.read('files-sha256.json'));assert set(names)==set(expected)|{'files-sha256.json'}
 for name in names:
  f=pathlib.PurePosixPath(name);assert not f.is_absolute() and '..' not in f.parts
  b=z.read(name)
  if name in expected:assert len(b)==expected[name]['bytes'] and hashlib.sha256(b).hexdigest()==expected[name]['sha256'],name
  target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
print('SEALED_MEMBERS',len(expected))
if (root/'source-proof/coverage.json').exists():
 c=json.loads((root/'source-proof/coverage.json').read_text());before=json.loads(gzip.decompress((root/'source-proof/before-manifest.json.gz').read_bytes()));after=json.loads(gzip.decompress((root/'source-proof/after-manifest.json.gz').read_bytes()))
 canon=lambda m:[r for r in m['segments'] if 'routing' not in r['kind']]
 assert canon(before)==canon(after)
 def pair(h,a):
  s=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
  for r in [h,a]:
   s.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
   for k in ['dataset','kind','path','checksum']:
    b=r.get(k,'').encode();s.update(struct.pack('>Q',len(b))+b)
  return s.digest()
 refs={r['path']:r for r in before['segments']};ids={pair(r,refs[str(pathlib.PurePosixPath(r['path']).with_suffix('.kv'))]) for r in before['segments'] if r.get('dataset')=='state-domain-change' and r['kind']=='history'}
 b=(root/'source-proof/small.route').read_bytes();ref=c['small_route_ref'];assert 'sha256:'+hashlib.sha256(b).hexdigest()==ref['checksum'] and len(b)==ref['size'];n=struct.unpack_from('>I',b,12)[0];assert {b[40+i*32:72+i*32] for i in range(n)}==ids and n==len(ids)
 old=(root/'source-proof/legacy-source-prefix.bin').read_bytes();oldn=struct.unpack_from('>I',old,12)[0];oldids={old[40+i*32:72+i*32] for i in range(oldn)};assert len(oldids&ids)==c['legacy_active_covered_sources'];assert len(ids-oldids)==c['legacy_uncovered_active_sources']
 print('COVERAGE',len(ids),c['small_route_keys'],len(ids-oldids))
# Summarize raw samples without implying that a fresh process has a cold OS cache.
def io(s):return {a:int(b) for a,b in (l.split(':',1) for l in s.strip().splitlines())}
def tv(v):return v['Sec']+v['Usec']/1e6
samples=[]
for f in root.rglob('queries.jsonl'):
 for line in f.read_text().splitlines():
  d=json.loads(line);q0,q1=io(d['io_before']),io(d['io_after']);u0,u1=d['usage_before'],d['usage_after'];samples.append(dict(path=str(f.parent.relative_to(root)),iteration=d['iteration'],seconds=d['seconds'],cpu_seconds=sum(tv(u1[k])-tv(u0[k]) for k in ['Utime','Stime']),allocated_bytes=d['allocated_bytes'],num_gc=d['num_gc'],logical_bytes=q1['rchar']-q0['rchar'],physical_read_bytes=q1['read_bytes']-q0['read_bytes'],read_syscalls=q1['syscr']-q0['syscr'],digest=d['digest']))
(root/'recomputed-samples.json').write_text(json.dumps(samples,indent=2))
for s in samples:print(json.dumps(s))
