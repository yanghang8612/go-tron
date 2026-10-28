"""Independent byte/identity/oracle verification; never opens a live database."""
import pathlib,json,hashlib,struct,collections,statistics,sys
R=pathlib.Path(sys.argv[1]);load=lambda n:json.loads((R/n).read_text())

def pair_id(h,a):
 d=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
 for r in [h,a]:
  d.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r.get('size',0),r.get('rows',0),r.get('domain',0)))
  for s in [r.get('dataset',''),r['kind'],r['path'],r.get('checksum','')]:
   b=s.encode();d.update(struct.pack('>Q',len(b)));d.update(b)
 return d.digest()

def check_route(manifest,name):
 refs=[r for r in manifest['segments'] if r['kind']=='small-system-history-routing'];assert len(refs)==1
 ref=refs[0];b=(R/name).read_bytes();assert len(b)==ref['size'] and 'sha256:'+hashlib.sha256(b).hexdigest()==ref['checksum']
 assert b[:8]==b'GTSMLR01';v,n,k,res=struct.unpack('>IIII',b[8:24]);assert v==1 and res==0 and k==ref['rows'] and 0<n<=262144 and k<=4096
 assert struct.unpack('>QQ',b[24:40])==(ref['fromTxNum'],ref['toTxNum'])
 ids=[b[40+32*i:72+32*i] for i in range(n)];assert len(set(ids))==n
 paths={r['path']:r for r in manifest['segments']};expected=[]
 for h in manifest['segments']:
  if h['kind']=='history' and h.get('dataset')=='state-domain-change':
   a=paths[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))]
   assert all(h.get(x,0)==a.get(x,0) for x in ['fromTxNum','toTxNum','aggregationSteps'])
   expected.append(pair_id(h,a))
 assert set(expected)==set(ids) and len(expected)==len(ids)
 off=40+n*32;prior=b'';families=collections.Counter();charge=n*128
 for _ in range(k):
  length=struct.unpack_from('>H',b,off)[0];off+=2;key=b[off:off+length];off+=length;assert 21<=length<=1024 and key>prior;prior=key
  mode,size=struct.unpack_from('>BI',b,off);off+=5;payload=b[off:off+size];off+=size;assert len(payload)==size and size>0
  if mode==0:
   assert size==(n+7)//8 and any(payload)
   if n%8:assert payload[-1]>>(n%8)==0
  else:
   assert mode==1 and size%4==0 and size<(n+7)//8
   cols=list(struct.unpack('>'+'I'*(size//4),payload));assert cols==sorted(set(cols)) and cols[-1]<n
  if length==21:
   assert key[0] in [1,3] and key[1:] in [bytes(20),bytes.fromhex('ff'*19+'fe')];family='account' if key[0]==1 else 'generation'
  else:
   assert length>=31 and key[0]==2 and key[1:21]==bytes.fromhex('ff'*19+'fe');domain=struct.unpack_from('>H',key,29)[0];assert domain in [1,4];family='dynamic-property' if domain==1 else 'fork-vote'
  families[family]+=1;charge+=256+len(key)+1+len(payload)
 assert off==len(b) and charge<=64<<20
 return dict(sources=n,keys=k,file_bytes=len(b),cache_charge_bytes=charge,families=dict(families))

def unchanged(before,after):
 a=[r for r in before['segments'] if 'routing' not in r['kind']];b=[r for r in after['segments'] if 'routing' not in r['kind']];assert a==b
 for k in ['sourceRows','commitmentScheme','elementVersion','visibleTxStart','visibleTxEnd','progress','latestStateTxNum']:assert before.get(k)==after.get(k),k
 return len(a)

s=load('status.json');assert s['complete'] and not s['production_mutations'] and s['production_identity_unchanged']
c=load('catalog-status.json');assert c['complete'] and c['all_query_results_equal'] and c['canonical_refs_and_frontiers_unchanged']
f=load('http-comparison/comparison-status.json');assert f['complete'] and f['all_responses_correct'] and not f['environment_identity_changed']
fixed=check_route(load('fixed-after.json'),'fixed-small.route');catalog=check_route(load('catalog-after.json'),'catalog-small.route')
assert unchanged(load('fixed-before.json'),load('fixed-after.json'))>0;assert unchanged(load('catalog-before.json'),load('catalog-after.json'))>0
for stats,path in [(fixed,'small-route.json'),(catalog,'catalog-proof/small-route.json')]:
 x=load(path);assert stats['sources']==x['sources']==x['covered'] and stats['keys']==x['keys'] and stats['cache_charge_bytes']==x['cache_charge_bytes'] and stats['file_bytes']==x['ref']['size']
mu=load('muhash-census.json');assert mu['complete'] and mu['muhash_verified'];root=bytes(mu['muhash_root']).hex();assert root=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
assert bytes.fromhex(f['head']['root']).decode()==root and f['head']['head']==11818192
restore=load('history-restore.json');assert restore['complete'] and len(restore['windows'])==3
for w in restore['windows']:assert w['rows']>0 and w['full_row_sha256']==w['candidate_row_sha256']==w['restored_row_sha256'] and w['range_rows_verified']==20
req=[json.loads(x) for x in (R/'http-comparison/requests.jsonl').read_text().splitlines()];assert all(x['correct'] and x['response']['result']==x['expected'] for x in req)
group=collections.defaultdict(list)
for x in req:
 if x['cache'] in ['warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process']:group[x['mode'],x['cache'],x['height']].append(x['milliseconds'])
summary=[dict(mode=m,cache=k,height=h,n=len(v),median_ms=statistics.median(v),min_ms=min(v),max_ms=max(v)) for (m,k,h),v in sorted(group.items())]
assert len(summary)==42 and all(x['n']==(4 if x['cache']=='warm' else 2) for x in summary)
residency=list((R/'http-comparison').glob('*-residency.json'));assert len(residency)==28 and all(load(str(p.relative_to(R)))['fraction']<.01 for p in residency)
a,b=load('baseline-probe.json'),load('candidate-probe.json');assert all(a[k]==b[k] for k in ['queries','from_tx','to_tx']);expected=a['samples'][0]['results'];assert all(r['results']==expected for obj in [a,b] for r in obj['samples'])
# All additional cache regimes are kept separate from the initial time window.
x=load('extra-cold-comparison/comparison-status.json');assert x['complete'] and x['all_responses_correct'] and x['before_identity']==x['after_identity']
extra=[json.loads(line) for line in (R/'extra-cold-comparison/requests.jsonl').read_text().splitlines()];assert len(extra)==32 and all(r['correct'] and r['response']['result']==r['expected'] for r in extra)
extra_groups=collections.defaultdict(list)
for r in extra:extra_groups[r['mode'],r['height']].append(r['milliseconds'])
assert len(extra_groups)==8 and all(len(v)==4 for v in extra_groups.values())
extra_summary=[dict(mode=m,height=h,n=len(v),median_ms=statistics.median(v),min_ms=min(v),max_ms=max(v)) for (m,h),v in sorted(extra_groups.items())]
proofs=load('extra-cold-comparison/private-proof-reuse.json')
for mode in ['baseline','candidate']:
 raw=(R/('http-comparison/'+mode+'-private-files.json')).read_bytes();assert hashlib.sha256(raw).hexdigest()==proofs[mode]['manifest_sha256']
 assert proofs[mode]['same_private_inodes_sizes_and_no_newer_mtimes']
residency=list((R/'extra-cold-comparison').glob('*-residency.json'));assert len(residency)==32 and all(load(str(p.relative_to(R)))['fraction']<.01 for p in residency)
catalog_samples={};rss={}
for mode,initial in [('baseline',a),('candidate',b)]:
 repeated=load('extra-cold-comparison/'+mode+'-abba-probe.json');assert all(initial[k]==repeated[k] for k in ['queries','from_tx','to_tx'])
 assert all(r['results']==expected for r in repeated['samples'])
 # Initial B,C probes were traced for open counts; repeated C,B probes
 # were native. Keep their latency distributions separate.
 def dist(obj):
  warm=[r['seconds'] for r in obj['samples'][1:]];assert len(warm)==4
  return dict(n=4,median_seconds=statistics.median(warm),min_seconds=min(warm),max_seconds=max(warm),fresh_process_os_uncontrolled_seconds=obj['samples'][0]['seconds'])
 resource=load('extra-cold-comparison/catalog-abba-'+mode+'-resources.json');assert resource['returncode']==0
 rss[mode]=resource['max_rss_kib']
 catalog_samples[mode]=dict(traced_open_calls=dist(initial),native_untraced=dist(repeated))
# Old-route source IDs are authenticated by the server census and native reader.
header=(R/'broad-source-header.bin').read_bytes();assert header[:8]==b'GTSYSR01';old_n=struct.unpack_from('>I',header,12)[0];assert len(header)==40+32*old_n
old_ids={header[40+32*i:72+32*i] for i in range(old_n)};new=(R/'catalog-small.route').read_bytes();new_n=struct.unpack_from('>I',new,12)[0];new_ids={new[40+32*i:72+32*i] for i in range(new_n)}
broad=load('broad-census.json');assert broad['sources']==old_n and broad['file_bytes']==broad['ref']['size']
assert 40+32*old_n+sum(f['encoded_row_bytes'] for f in broad['families'].values())==broad['file_bytes']
assert old_n*128+sum(f['cache_charge_bytes'] for f in broad['families'].values())==broad['cache_charge_bytes']
coverage=dict(old_source_ids=old_n,old_active_covered=len(old_ids&new_ids),old_active_uncovered=len(new_ids-old_ids),new_active_covered=new_n)
# Separate post-seal server census: full old bytes were rehashed there;
# locally, bind its reference/counts and independently parse the new key set.
key_difference=json.loads(pathlib.Path(__file__).with_name('system-key-difference.json').read_text())
assert key_difference['system-history-routing']['ref']==broad['ref']
assert key_difference['small-system-history-routing']['ref']==next(r for r in load('catalog-after.json')['segments'] if r['kind']=='small-system-history-routing')
off=40+32*new_n;new_keys=[]
for _ in range(struct.unpack_from('>I',new,16)[0]):
 size=struct.unpack_from('>H',new,off)[0];off+=2;new_keys.append(new[off:off+size].hex());off+=size;length=struct.unpack_from('>I',new,off+1)[0];off+=5+length
assert new_keys==key_difference['small-system-history-routing']['small_family_keys_hex']
old_keys=key_difference['system-history-routing']['small_family_keys_hex'];assert len(old_keys)==59
assert set(new_keys)-set(old_keys)==set(key_difference['new_keys_hex']) and not set(old_keys)-set(new_keys)
assert len(key_difference['new_keys_hex'])==1 and bytes.fromhex(key_difference['new_keys_hex'][0])[31:]==b'allow_tvm_london'
opens={}
for mode in ['baseline','candidate']:
 lines=(R/(mode+'-catalog-opens.trace')).read_text(errors='replace').splitlines();success=[l for l in lines if __import__('re').search(r'= [0-9]+',l)]
 opens[mode]=dict(broad=sum('/system-routing-' in l for l in success),small=sum('/small-system-routing-' in l for l in success),history_kv=sum('/history/' in l and '.kv' in l for l in success))
assert opens['candidate']['broad']==0 and opens['candidate']['small']>0 and opens['baseline']['broad']>0
native=load('native-source-proof.json');local=json.loads(pathlib.Path(__file__).with_name('local-validation.json').read_text());assert native['product_files']==local['product_files']
assert native['http_binaries']['baseline']==f['baseline_binary_sha256'] and native['http_binaries']['candidate']==f['candidate_binary_sha256']
live=load('final-production-identity.json');assert not live['production_mutations'] and live['binary_sha256']=='936005635809717f01c5661ef0d1f8519e61cb2d5e511f69e8c3d88fe16882b8'
result=dict(complete=True,fixed=fixed,catalog=catalog,muhash_root=root,source_rows=mu['source_rows'],restored_blocks=60,restored_rows=sum(w['rows'] for w in restore['windows']),http_samples=summary,additional_cold_samples=extra_summary,catalog_probe_warm=catalog_samples,catalog_probe_max_rss_kib=rss,catalog_route_opens=opens,coverage=coverage,production_unchanged=True)
(R/'independent-verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
