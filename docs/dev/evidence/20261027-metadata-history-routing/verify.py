import sys,pathlib,hashlib,json,zipfile,gzip,struct,statistics,collections,re
archive=pathlib.Path(sys.argv[1]);root=pathlib.Path(sys.argv[2]);seal=json.loads(pathlib.Path(sys.argv[3]).read_text())
assert archive.stat().st_size==seal['bytes'];assert hashlib.sha256(archive.read_bytes()).hexdigest()==seal['sha256'];root.mkdir(parents=True,exist_ok=True)
with zipfile.ZipFile(archive) as z:
 names=z.namelist();assert len(set(names))==len(names);expected=json.loads(z.read('files-sha256.json'));assert set(names)==set(expected)|{'files-sha256.json'}
 for name in names:
  p=pathlib.PurePosixPath(name);assert not p.is_absolute() and '..' not in p.parts;b=z.read(name)
  if name in expected:assert len(b)==expected[name]['bytes'] and hashlib.sha256(b).hexdigest()==expected[name]['sha256'],name
  target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
M=root/'metadata-v3';Q=root/'metadata-cold-v3';F=root/'metadata-final'
for p in [M,Q]:assert json.loads((p/'status.json').read_text())['complete']
assert json.loads((Q/'status.json').read_text())['revalidation_complete']
assert json.loads((F/'acceptance.json').read_text())['complete'];assert json.loads((M/'strict.json').read_text())==dict(json.loads((M/'strict.json').read_text()),passed=True,sources=29658,pages=116)
load=lambda n:json.loads(gzip.decompress((F/(n+'-manifest.json.gz')).read_bytes()))
before,previous,after=(load(n) for n in ['before','previous','after']);canon=lambda m:[r for r in m['segments'] if 'routing' not in r['kind']]
assert canon(before)==canon(previous)==canon(after)
assert {k:v for k,v in previous.items() if k!='segments'}=={k:v for k,v in after.items() if k!='segments'}
def pair(h,a):
 s=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
 for r in [h,a]:
  s.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
  for k in ['dataset','kind','path','checksum']:
   b=r.get(k,'').encode();s.update(struct.pack('>Q',len(b))+b)
 return s.digest()
refs={r['path']:r for r in before['segments']};ids={pair(r,refs[str(pathlib.PurePosixPath(r['path']).with_suffix('.kv'))]):r for r in before['segments'] if r.get('dataset')=='state-domain-change' and r['kind']=='history'}
covered={};totalbytes=nonempty=0;pages=[r for r in after['segments'] if r['kind']=='metadata-history-routing']
for r in pages:
 b=(M/'catalog'/r['path']).read_bytes();assert len(b)==r['size'] and 'sha256:'+hashlib.sha256(b).hexdigest()==r['checksum'];assert b[:8]==b'GTMETR01';ver,n,start,end=struct.unpack_from('>IIQQ',b,8);assert ver==1 and n==r['rows'] and start==r['fromTxNum'] and end==r['toTxNum'];pos=32;prior=None
 for i in range(n):
  key=b[pos:pos+32];count=struct.unpack_from('>Q',b,pos+32)[0];pos+=40
  assert key in ids and key not in covered and (prior is None or key>prior);prior=key;h=ids[key];assert start<=h['fromTxNum']<=h['toTxNum']<=end
  if count:pos+=1024;nonempty+=1
  covered[key]=count
 assert pos==len(b);totalbytes+=len(b)
assert covered.keys()==ids.keys() and len(ids)==29658 and len(pages)==116
# The legacy SetContract journal omission makes this particular frozen corpus
# metadata-empty. Nonempty and saturated filters are separate executable tests.
assert nonempty==0 and totalbytes==1190032
oracle=json.loads((root/'probe/baseline-first/rows.json').read_text());assert len(oracle)==141 and sum(x is not None for x in oracle)==39
for p in list(M.glob('repeat-*'))+list(Q.glob('os-cold-*'))+list(Q.glob('revalidated-os-cold-*'))+list(F.glob('trace-*')):
 if p.is_dir():assert json.loads((p/'rows.json').read_text())==oracle,p
for p in M.glob('metadata-*'):
 if p.is_dir():assert json.loads((p/'rows.json').read_text())==[None]
proof=json.loads((Q/'private-files-proof.json').read_text())+json.loads((Q/'private-extra-proof.json').read_text());private={r['path']:r for r in proof}
for r in proof:assert (r['source_dev'],r['source_ino'])!=(r['private_dev'],r['private_ino'])
traces=json.loads((F/'trace-private-coverage.json').read_text());actual_traces={}
for mode,trace in traces.items():
 assert set(trace['paths'])<=private.keys() and trace['covered_by_independent_inode_proof']
 prefix='/data/gtron/maintenance/9m-query-20261027-01/metadata-cold-v3/'+mode+'/'
 actual=set();trace_refs={r['path'] for r in (before if mode=='baseline' else after)['segments']}
 for line in (F/('trace-'+mode)/'openat.log').read_text().splitlines():
  m=re.search(r'openat\([^,]+, "([^"]+)"',line)
  if m and m.group(1).startswith(prefix):
   name=m.group(1)[len(prefix):]
   if name in trace_refs:actual.add(name)
 assert actual==set(trace['paths']) and actual<=private.keys()
 actual_traces[mode]=len(actual)
for p in Q.glob('*-residency.json'):
 v=json.loads(p.read_text());assert v['resident_pages']/v['total_pages']<.01;assert all(x['resident_pages']/x['pages']<.01 for x in v['per_file'])
 if p.name.startswith('revalidated-'):
  mode='baseline' if 'baseline' in p.name else 'candidate'
  assert set(traces[mode]['paths'])<={x['path'] for x in v['per_file']}
restoration=json.loads((M/'restoration/history-restore.json').read_text());assert restoration['complete']
for w in restoration['windows']:assert w['full_row_sha256']==w['candidate_row_sha256']==w['restored_row_sha256'] and w['range_rows_verified']==20 and w['result']['ChangesRestored']==w['rows']
assert json.loads((F/'production-identity.json').read_text())['identity_unchanged']
product=json.loads((F/'product-sha256.json').read_text());assert product==json.loads((pathlib.Path(__file__).parent/'product-sha256-final.json').read_text());assert all(hashlib.sha256((F/'source'/n).read_bytes()).hexdigest()==v for n,v in product.items())
def io(s):return {a:int(b) for a,b in (l.split(':',1) for l in s.strip().splitlines())}
def tv(v):return v['Sec']+v['Usec']/1e6
samples=[]
for p in [M,Q]:
 for f in p.rglob('queries.jsonl'):
  for line in f.read_text().splitlines():
   d=json.loads(line);q0,q1=io(d['io_before']),io(d['io_after']);u0,u1=d['usage_before'],d['usage_after'];samples.append(dict(path=str(f.parent.relative_to(root)),iteration=d['iteration'],seconds=d['seconds'],cpu_seconds=sum(tv(u1[k])-tv(u0[k]) for k in ['Utime','Stime']),allocated_bytes=d['allocated_bytes'],num_gc=d['num_gc'],logical_bytes=q1['rchar']-q0['rchar'],physical_read_bytes=q1['read_bytes']-q0['read_bytes'],read_syscalls=q1['syscr']-q0['syscr'],digest=d['digest']))
(root/'recomputed-samples.json').write_text(json.dumps(samples,indent=2))
receipt={'complete':True,'members':len(expected),'sources':len(ids),'metadata_pages':len(pages),'metadata_page_bytes':totalbytes,'nonempty_metadata_sources':nonempty,'queries':len(oracle),'non_nil_rows':sum(x is not None for x in oracle),'restored_full_rows':sum(w['rows'] for w in restoration['windows']),'restored_block_ranges':60,'private_trace_files':actual_traces,'product_files':len(product),'samples':len(samples)}
(root/'independent-verification.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2))
for s in samples:print(json.dumps(s))
