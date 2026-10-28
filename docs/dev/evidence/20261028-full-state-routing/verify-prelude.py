import pathlib,json,zipfile,hashlib,gzip,struct,sys
archive=pathlib.Path(sys.argv[1]);root=pathlib.Path(sys.argv[2]);seal=json.loads((pathlib.Path(__file__).parent/'prelude-seal.json').read_text());assert archive.stat().st_size==seal['bytes'] and hashlib.sha256(archive.read_bytes()).hexdigest()==seal['sha256'];root.mkdir(parents=True,exist_ok=True)
with zipfile.ZipFile(archive) as z:
 names=z.namelist();expected=json.loads(z.read('files-sha256.json'));assert len(names)==len(set(names))==seal['members'] and set(names)==set(expected)|{'files-sha256.json'}
 for n in names:
  p=pathlib.PurePosixPath(n);assert not p.is_absolute() and '..' not in p.parts;b=z.read(n)
  if n in expected:assert len(b)==expected[n]['bytes'] and hashlib.sha256(b).hexdigest()==expected[n]['sha256'],n
  p=root/n;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
load=lambda n:json.loads((root/n).read_text())
cap=load('capture-status.json');assert cap['complete'] and cap['original_service_restored'] and cap['deployment_timer_restored'] and not cap['new_release_deployed'] and not cap['source_database_deleted'];assert cap['before_identity']['sha256']==cap['after_identity']['sha256']=='936005635809717f01c5661ef0d1f8519e61cb2d5e511f69e8c3d88fe16882b8';assert cap['before_identity']['argv']==cap['after_identity']['argv'];assert cap['first_head_after_start']>=cap['head_before_stop']
b=gzip.decompress((root/'capture-files.json.gz').read_bytes());assert hashlib.sha256(b).hexdigest()==cap['inventory_sha256'];inventory=json.loads(b);assert len(inventory)==cap['files'];assert sum(r['bytes'] for r in inventory if r['linked'])==cap['linked_bytes'];assert sum(r['bytes'] for r in inventory if not r['linked'])==cap['copied_bytes']
for r in inventory:assert ((r['source_dev'],r['source_ino'])==(r['dest_dev'],r['dest_ino']))==r['linked'];assert r['linked']==(r['sha256'] is None)
integ=load('capture-integrity.json');assert integ['complete'] and integ['files']==len(inventory) and integ['copied_files_rehashed']==sum(not r['linked'] for r in inventory)
before,after=[json.loads(gzip.decompress((root/('manifests/'+mode+'.json.gz')).read_bytes())) for mode in ['baseline','candidate']]
def canon(m):return {r['path']:{**r,'aggregationSteps':r.get('aggregationSteps',0) or 1} for r in m['segments'] if r['kind'] not in ['system-history-routing','small-system-history-routing','metadata-history-routing','account-history-routing']}
assert canon(before)==canon(after) and len(canon(before))==169920
rawcanon=lambda m:{r['path']:r for r in m['segments'] if 'routing' not in r['kind']}
assert rawcanon(before)==rawcanon(after)
refs={r['path']:r for r in before['segments']}
def pair(h,a):
 s=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
 for r in [h,a]:
  s.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
  for k in ['dataset','kind','path','checksum']:
   b=r.get(k,'').encode();s.update(struct.pack('>Q',len(b))+b)
 return s.digest()
pairs={pair(r,refs[str(pathlib.PurePosixPath(r['path']).with_suffix('.kv'))]):r for r in before['segments'] if r.get('dataset')=='state-domain-change' and r['kind']=='history'};assert len(pairs)==32036
covered={};pages=total=nonempty=0;small=[]
for r in after['segments']:
 if r['kind'] not in ['metadata-history-routing','small-system-history-routing']:continue
 b=(root/'routes'/r['path']).read_bytes();assert len(b)==r['size'] and 'sha256:'+hashlib.sha256(b).hexdigest()==r['checksum']
 if r['kind']=='small-system-history-routing':small.append(r);continue
 assert b[:8]==b'GTMETR01';ver,n,start,end=struct.unpack_from('>IIQQ',b,8);assert ver==1 and 1<=n<=64 and n==r['rows'] and start==r['fromTxNum'] and end==r['toTxNum'];pos=32;prior=None
 for i in range(n):
  key=b[pos:pos+32];count=struct.unpack_from('>Q',b,pos+32)[0];pos+=40;bits=b[pos:pos+1024] if count else b'';pos+=len(bits);assert key in pairs and key not in covered and (prior is None or key>prior);prior=key;h=pairs[key];assert start<=h['fromTxNum']<=h['toTxNum']<=end;covered[key]=(count,bits);nonempty+=bool(count)
 assert pos==len(b);pages+=1;total+=len(b)
assert covered.keys()==pairs.keys() and pages==501 and total==1297472 and nonempty==0
strict=load('indexes/status.json');assert strict['complete'] and strict['full_dictionary_sources_verified']==len(pairs) and strict['small_keys']==62
identity=load('source-identity.json');current=load('source/current-source-hashes.json');local=json.loads((pathlib.Path(__file__).parents[1]/'20261027-metadata-history-routing/product-sha256-final.json').read_text())
assert identity['candidate_sources']==local
for mode,rows in current.items():
 for n,h in rows.items():
  n='core/offline_full_acceptance_http_test.go' if n=='http_final_harness' else n;assert hashlib.sha256((root/'source'/mode/n).read_bytes()).hexdigest()==h
assert current['baseline']['http_final_harness']==current['candidate']['http_final_harness']=='86c19219adb56ce0c5dae4f8d134ae3796ba3132b99063a37de338119a1e4fe0'
ready=load('http-readiness/status.json');assert ready['complete'] and ready['performance_samples_excluded'] and len(ready['checks'])==7
receipt={'complete':True,'phase':'capture/routes/preliminary HTTP correctness only; full MuHash/restore/performance pending','members':len(expected),'capture_files':len(inventory),'copied_files_rehashed_on_server':integ['copied_files_rehashed'],'stop_to_original_api_seconds':cap['stop_to_api_seconds'],'captured_head':load('source-muhash/head.json')['BlockNum'],'canonical_refs':len(canon(before)),'history_pairs':len(pairs),'metadata_pages':pages,'metadata_bytes':total,'metadata_nonempty_sources':nonempty,'small_route_refs':len(small),'small_keys':strict['small_keys'],'complete_http_smoke_checks':7,'candidate_product_files':len(local)}
(root/'independent-prelude-verification.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2))
