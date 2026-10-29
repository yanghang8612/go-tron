import collections,hashlib,json,pathlib,struct,sys,zipfile
archive,parent_archive,study_archive=map(pathlib.Path,sys.argv[1:4]);out=pathlib.Path(sys.argv[4]);out.mkdir(parents=True,exist_ok=True)
assert hashlib.sha256(parent_archive.read_bytes()).hexdigest()=='c6e176434939589570402361d2d238823236bcf5fe66620cb469a1c7b03d5319'
assert hashlib.sha256(study_archive.read_bytes()).hexdigest()=='803e1ed63c4974e1cfba75d54e121e0dffe05e5502069113718f07352d8705cd'
z=zipfile.ZipFile(archive);assert z.testzip() is None;read=lambda n:json.loads(z.read(n))
s=read('candidate-status.json');assert s['complete'] and s['source']=='0f93254f81b8c49738cc7c0cae79b84f63b32f26' and not s['production_mutations'] and s['production_before']==s['production_after']
assert s['compact_manifest_sha256_before']==s['compact_manifest_sha256_after']
assert s['binary_sha256']['before']=='12827bd067d3632ac1bd10852cffbc3a06bec9846931846e1561a74197cff9c0'
with zipfile.ZipFile(parent_archive) as p:parent=json.loads(p.read('routing-receipt.json'))['variants']['gaps'];old_proof=json.loads(p.read('private-index-cold/private-proof.json'))
with zipfile.ZipFile(study_archive) as p:missing={r['pair_id'] for r in json.loads(p.read('distribution.json'))['rows']}
receipt=read('routing-receipt.json');assert receipt['canonical_and_other_auxiliary_refs_sha256']==parent['canonical_and_other_auxiliary_refs_sha256'] and receipt['progress']==parent['progress'] and receipt['visible']==parent['visible']
ids=set();file_bytes=allocated=rows=0
for obj in receipt['objects']:
 r=obj['ref'];data=z.read(obj['metadata']);assert data[:8]==b'GTACRT01' and data[56:64]==bytes(8)
 assert 'sha256:'+hashlib.sha256(data).hexdigest()==r['lookupRoot']
 version,n=struct.unpack('>II',data[8:16]);count,pages,dirs,lo,hi=struct.unpack('>QQQQQ',data[16:56]);assert version==1 and count==r.get('rows',0)<=8_000_000
 assert pages==(count+140)//141 and dirs==(pages+76)//77 and len(data)==64+32*n+53*dirs and r['size']==len(data)+(dirs+pages)*4096<=256<<20
 assert [lo,hi]==[r['fromTxNum'],r['toTxNum']]
 current={data[64+32*i:96+32*i].hex() for i in range(n)};assert len(current)==n and current<=missing and not ids&current
 ids|=current;file_bytes+=r['size'];allocated+=obj['allocated_bytes'];rows+=count
runtime=read('runtime-result.json');assert runtime['complete'] and runtime['canonical_refs_unchanged'] and runtime['frontiers_unchanged'] and runtime['new_exact_proofs']==len(ids)
assert len(runtime['passes'])==3 and sum(p['sources'] for p in runtime['passes'])==sum(p['source_reads'] for p in runtime['passes'])==len(ids)
for p in runtime['passes']:assert 1<=p['sources']<=8 and p['declared_input_bytes']<=128<<20 and p['cpu_seconds']>=0
proof=read('private-inode-proof.json');assert proof==old_proof
expected={9000000:207,10000000:213,11000000:220,15000000:238,20000000:249};requests=read('requests.json');assert len(requests)==64
verified=[];head=(45836827,'0000000002bb6a1bcb3ab2e1ac8af3817bec7a121f92f1413d2552eceef0a30c',4240407293,'4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627')
def io(v):return {k:int(n) for k,n in (line.split(':') for line in v.splitlines())}
for q in requests:
 assert int(q['response']['result'],16)==expected[q['height']] and 'error' not in q['response']
 ready=read(q['case']+'/ready.json');assert ready['read_only'] and tuple(ready[k] for k in ['head','head_hash','head_tx','root'])==head
 server=[json.loads(x) for x in z.read(q['case']+'/server-requests.jsonl').decode().splitlines()];r,=[r for r in server if str(r['id'])==str(q['response']['id'])]
 assert r['method']=='eth_call' and r['params']==q['request']['params']==[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(q['height'])]
 if q['case'].startswith('private-cold-'):
  resident=read(q['case']+'/dispatch-residency.json');assert {r['path'] for r in resident}=={r['path'] for r in proof} and all(r['resident_pages']==0 for r in resident)
 before,after=io(r['io_before']),io(r['io_after']);verified.append(dict(q,io_delta={k:after[k]-before[k] for k in before}))
groups=[]
for family,prefix in [('before','before-'),('candidate','candidate-'),('private-account-index-cold','private-cold-'),('runtime-built-input','runtime-built-input')]:
 for height in expected:
  for repeat in [0,1]:
   qs=[q for q in verified if q['case'].startswith(prefix) and q['height']==height and q['repeat']==repeat]
   if qs:groups.append(dict(family=family,height=height,repeat=repeat,seconds=[q['seconds_loopback_http'] for q in qs],rchar=[q['io_delta']['rchar'] for q in qs],syscr=[q['io_delta']['syscr'] for q in qs],read_bytes=[q['io_delta']['read_bytes'] for q in qs]))
summary={'complete':True,'source':s['source'],'production_identity':s['production_after'],'requests':len(verified),'head':head,'runtime':runtime,'delta_objects':len(receipt['objects']),'delta_rows':rows,'delta_file_bytes':file_bytes,'delta_allocated_bytes':allocated,'new_exact_proofs':len(ids),'archive_bytes':archive.stat().st_size,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'scope':'three bounded native policy passes and 64 full HTTP values; only private new index pages cold; no repeated full-state MuHash or full-node restore'}
for name,value in [('verification.json',summary),('grouped-http.json',groups),('verified-requests.json',verified)]: (out/name).write_text(json.dumps(value,indent=2)+'\n')
for name in z.namelist():
 assert not pathlib.Path(name).is_absolute() and '..' not in pathlib.Path(name).parts
 if name.endswith(('.json','.jsonl','.log')):
  p=out/'native'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(z.read(name))
print(json.dumps(summary,indent=2))
