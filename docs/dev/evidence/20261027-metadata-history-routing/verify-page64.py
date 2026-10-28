import pathlib,json,gzip,hashlib,struct,re,sys,statistics
root=pathlib.Path(sys.argv[1]);R=root/'metadata-page64';F=root/'metadata-final';M=root/'metadata-v3';Q=root/'metadata-cold-v3'
base=json.loads(gzip.decompress((F/'after-manifest.json.gz').read_bytes()));after=json.loads(gzip.decompress((R/'manifest.json.gz').read_bytes()));refs={r['path']:r for r in after['segments']}
assert [r for r in base['segments'] if r['kind']!='metadata-history-routing']==[r for r in after['segments'] if r['kind']!='metadata-history-routing']
assert {k:v for k,v in base.items() if k!='segments'}=={k:v for k,v in after.items() if k!='segments'}
def filters(manifest,folder,maxrows):
 rows={};pagecount=total=0
 for r in manifest['segments']:
  if r['kind']!='metadata-history-routing':continue
  b=(folder/r['path']).read_bytes();assert len(b)==r['size'] and 'sha256:'+hashlib.sha256(b).hexdigest()==r['checksum'];assert b[:8]==b'GTMETR01';ver,n,start,end=struct.unpack_from('>IIQQ',b,8);assert ver==1 and n==r['rows'] and 1<=n<=maxrows and start==r['fromTxNum'] and end==r['toTxNum'];pos=32;prior=None
  for i in range(n):
   key=b[pos:pos+32];count=struct.unpack_from('>Q',b,pos+32)[0];pos+=40;bits=b[pos:pos+1024] if count else b'';pos+=len(bits);assert key not in rows and (prior is None or key>prior);prior=key;rows[key]=(count,bits)
  assert pos==len(b);total+=len(b);pagecount+=1
 return rows,pagecount,total
old,n0,b0=filters(base,M/'catalog',256);new,n1,b1=filters(after,R/'catalog',64);assert old==new and len(new)==29658 and n1==464 and b1==1201168
proof=json.loads((Q/'private-files-proof.json').read_text())+json.loads((Q/'private-extra-proof.json').read_text())+json.loads((R/'new-private-files.json').read_text());private={r['path']:r for r in proof};paths=set();prefix='/data/gtron/maintenance/9m-query-20261027-01/metadata-page64/catalog/'
for line in (R/'trace/openat.log').read_text().splitlines():
 m=re.search(r'openat\([^,]+, "([^"]+)"',line)
 if m and m.group(1).startswith(prefix):
  name=m.group(1)[len(prefix):]
  if name in refs:paths.add(name)
trace=json.loads((R/'trace-coverage.json').read_text());assert paths==set(trace['paths']) and paths<=private.keys();maintraces=json.loads((F/'trace-private-coverage.json').read_text())
for p in R.glob('*-residency.json'):
 v=json.loads(p.read_text());assert all(x['resident_pages']/x['pages']<.01 for x in v);want=paths if 'candidate' in p.name else set(maintraces['baseline']['paths']);assert want<={x['path'] for x in v}
oracle=json.loads((root/'probe/baseline-first/rows.json').read_text())
for p in R.rglob('rows.json'):assert json.loads(p.read_text())==oracle

def io(s):return {a:int(b) for a,b in (l.split(':',1) for l in s.strip().splitlines())}
def tv(v):return v['Sec']+v['Usec']/1e6
samples=[]
for f in R.rglob('queries.jsonl'):
 for line in f.read_text().splitlines():
  d=json.loads(line);q0,q1=io(d['io_before']),io(d['io_after']);u0,u1=d['usage_before'],d['usage_after'];samples.append(dict(path=str(f.parent.relative_to(root)),iteration=d['iteration'],seconds=d['seconds'],cpu_seconds=sum(tv(u1[k])-tv(u0[k]) for k in ['Utime','Stime']),allocated_bytes=d['allocated_bytes'],num_gc=d['num_gc'],logical_bytes=q1['rchar']-q0['rchar'],physical_read_bytes=q1['read_bytes']-q0['read_bytes'],read_syscalls=q1['syscr']-q0['syscr'],digest=d['digest']))
(R/'recomputed-samples.json').write_text(json.dumps(samples,indent=2));groups={}
for mode in ['baseline','candidate']:
 v=[x for x in samples if '/os-cold-' in x['path'] and x['path'].endswith(mode)];groups[mode+'-os-cold']={'n':len(v),'min_seconds':min(x['seconds'] for x in v),'max_seconds':max(x['seconds'] for x in v),'medians':{k:statistics.median(x[k] for x in v) for k in ['seconds','cpu_seconds','allocated_bytes','logical_bytes','physical_read_bytes','read_syscalls']}}
v=[x for x in samples if x['path'].endswith('/warm') and x['iteration']>0 and x['physical_read_bytes']==0];groups['candidate-warm-zero-physical']={'n':len(v),'medians':{k:statistics.median(x[k] for x in v) for k in ['seconds','cpu_seconds','allocated_bytes','logical_bytes','physical_read_bytes','read_syscalls']}}
(R/'performance-summary.json').write_text(json.dumps(groups,indent=2));receipt={'complete':True,'sources':len(new),'pages':n1,'bytes':b1,'vectors_equal_independently_strict_verified_256_layout':True,'actual_trace_files':len(paths),'all_actual_reads_covered_by_cold_proof':True};(R/'independent-verification.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2));print(json.dumps(groups,indent=2))
