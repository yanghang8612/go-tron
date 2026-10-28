"""Verify native runtime repair using sealed receipts, not a new DB/state scan."""
import pathlib,json,hashlib,zipfile,struct,sys,collections
archive,seal_path,storage_archive,parent_archive,out=map(pathlib.Path,sys.argv[1:]);out.mkdir(parents=True,exist_ok=True)
seal=json.loads(seal_path.read_text());assert archive.stat().st_size==seal['bytes'] and hashlib.sha256(archive.read_bytes()).hexdigest()==seal['sha256']
assert storage_archive.stat().st_size==10602992 and hashlib.sha256(storage_archive.read_bytes()).hexdigest()=='b799a1df631281be649a768ad047ee33017a6f1e5e8406e61ff37d3130265ec2'
assert parent_archive.stat().st_size==42113144 and hashlib.sha256(parent_archive.read_bytes()).hexdigest()=='c57b2b0012dfa5cae5a3f7c77250d19902014f8a93c866a51fcb13003fdbdd98'
z,old,original=zipfile.ZipFile(archive),zipfile.ZipFile(storage_archive),zipfile.ZipFile(parent_archive)
assert z.testzip() is None and len(z.namelist())==seal['members']
read=lambda n:json.loads(z.read(n))
status=read('candidate-status.json');assert status['complete'] and status['all_returns_match'] and not status['production_mutations'] and status['production_before']==status['production_after']
runtime=read('runtime-result.json');assert runtime['complete'] and runtime['canonical_refs_unchanged'] and runtime['frontiers_unchanged']
assert runtime['source_reads']==runtime['removed_proofs']==runtime['covered_after']-runtime['covered_before']
assert runtime['removed_pages']==8 and runtime['covered_after']==6436 and runtime['broad_reads']==1 and runtime['idle_source_reads']==0
passes=runtime['passes'];assert sum(p['source_reads'] for p in passes)==runtime['source_reads']
assert all(not p['unsupported'] for p in passes) and not passes[-1]['built'] and passes[-1]['source_reads']==passes[-1]['broad_reads']==0
assert all(a['covered']<=b['covered'] for a,b in zip(passes,passes[1:]))
old_receipt=json.loads(old.read('storage-pages/routing-receipt.json'));receipt=read('routing-receipt.json')
assert receipt['parent_manifest_sha256']==old_receipt['indexed_manifest_sha256'] and receipt['canonical_refs_unchanged']
m=json.loads(original.read('metadata-candidate/manifest.json'));canonical=[r for r in m['segments'] if r['kind']!='storage-history-routing']
assert hashlib.sha256(json.dumps(canonical,sort_keys=True,separators=(',',':')).encode()).hexdigest()==receipt['canonical_refs_sha256']==old_receipt['canonical_refs_sha256']
def pair(h,a):
 digest=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
 for r in [h,a]:
  digest.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
  for v in [r.get('dataset',''),r['kind'],r['path'],r.get('checksum','')]:b=v.encode();digest.update(struct.pack('>Q',len(b)));digest.update(b)
 return digest.digest()
by_path=collections.defaultdict(list)
for r in canonical:by_path[r['path']].append(r)
sources={}
for h in canonical:
 if h['kind']!='history' or h.get('dataset')!='state-domain-change':continue
 a=[r for r in by_path[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))] if r['kind']=='accessor' and r.get('dataset')==h['dataset'] and r['fromTxNum']==h['fromTxNum'] and r['toTxNum']==h['toTxNum'] and (r.get('aggregationSteps',0) or 1)==(h.get('aggregationSteps',0) or 1)];assert len(a)==1;sources[pair(h,a[0])]=h
assert len(sources)==32036
def filters(refs,current):
 result={}
 for ref in refs:
  name='routes/'+ref['path']
  b=z.read(name) if current and name in z.namelist() else old.read('storage-pages/routes/'+ref['path'])
  assert len(b)==ref['size']<=1<<20 and 'sha256:'+hashlib.sha256(b).hexdigest()==ref['checksum']
  assert b[:8]==b'GTSTOR01' and struct.unpack('>IIQQ',b[8:32])==(1,ref['rows'],ref['fromTxNum'],ref['toTxNum']) and 0<ref['rows']<=256
  pos,prior=32,None
  for _ in range(ref['rows']):
   id_,count,n=struct.unpack('>32sII',b[pos:pos+40]);pos+=40
   assert count<=65536 and n==(0 if count==0 else 1<<(max(64,count*2)-1).bit_length()) and pos+n<=len(b)
   assert (prior is None or prior<id_) and id_ in sources and id_ not in result
   h=sources[id_];assert ref['fromTxNum']<=h['fromTxNum']<=h['toTxNum']<=ref['toTxNum']
   result[id_]=(count,b[pos:pos+n]);pos+=n;prior=id_
  assert pos==len(b)
 return result
before=filters(old_receipt['routing_refs'],False);after=filters(receipt['routing_refs'],True)
assert before==after and len(after)==6436
def io(x):return {k:int(v) for k,v in (line.split(':') for line in x.splitlines())}
expected={9000000:207,10000000:213,15000000:238,20000000:249};rows=[];heads=set()
for q in read('requests.json'):
 ready=read(q['case']+'/ready.json');assert ready['read_only'];heads.add(tuple(ready[k] for k in ['head','head_hash','head_tx','root']))
 events=[json.loads(line) for line in z.read(q['case']+'/server-requests.jsonl').decode().splitlines()];event=[e for e in events if str(e['id'])==str(q['response']['id'])];assert len(event)==1;event=event[0]
 assert event['params']==[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(q['height'])] and event['method']=='eth_call'
 assert int(q['response']['result'],16)==expected[q['height']] and 'error' not in q['response']
 a,b=io(event['io_before']),io(event['io_after']);rows.append(dict(q,io_delta={k:b[k]-a[k] for k in a},server_seconds=event['elapsed_ns']/1e9))
assert len(rows)==32 and len(heads)==1
head=next(iter(heads));assert head[0]==45836827 and head[2]==4240407293 and head[3]=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
groups=[]
for family in ['control','runtime']:
 for height in expected:
  for repeat in [0,1]:
   selected=[r for r in rows if r['case'].startswith(family+'-') and r['height']==height and r['repeat']==repeat];assert len(selected)==2
   groups.append(dict(family=family,height=height,repeat=repeat,seconds=[r['seconds_loopback_http'] for r in selected],rchar=[r['io_delta']['rchar'] for r in selected],syscr=[r['io_delta']['syscr'] for r in selected]))
verification=dict(archive=seal,source=status['source'],production_identity=status['production_after'],canonical_refs_unchanged=True,frontiers_unchanged=True,exact_storage_pairs=6436,all_filter_rows_identical_to_prototype=True,removed_pages=8,removed_proofs=runtime['removed_proofs'],source_reads=runtime['source_reads'],broad_reads=1,build_publish_seconds=sum(p['seconds_build_publish'] for p in passes),trial_seconds_with_coverage_checks=runtime['seconds_trial_including_coverage_checks'],new_page_files=len(receipt['new_refs']),http_returns=32,head=head,cache=status['cache'])
for name,data in [('verification.json',verification),('grouped-http.json',groups),('verified-requests.json',rows)]: (out/name).write_text(json.dumps(data,indent=2)+'\n')
for name in z.namelist():
 if name.endswith(('.json','.jsonl','.log','.txt')) and name!='runtime-progress.json':
  p=out/'native'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(z.read(name))
print(json.dumps(verification,indent=2))
