"""Recompute the frozen-cohort and full HTTP acceptance summaries."""
from pathlib import Path
import json,gzip,hashlib,collections,statistics,re
P=Path(__file__).resolve().parent

def data(n):
 b=(P/n).read_bytes();return gzip.decompress(b) if n.endswith('.gz') else b

def read(n):return json.loads(data(n))
def lines(n):return list(map(json.loads,data(n).splitlines()))
for n,d in read('SHA256SUMS.json').items():assert hashlib.sha256((P/n).read_bytes()).hexdigest()==d,n
rows=lines('corpus-files.jsonl.gz');totals=collections.Counter();byname={};ends={x['path']:x for x in read('end-output-checksums.json.gz')};versions=collections.Counter()
for x in rows:
 name=x['source']['path'];assert name not in byname;byname[name]=x;v=x['audit'];end=ends[name]
 assert v['SourceBytes']==x['source']['size'] and v['CandidateBytes']==x['candidate']['size']==end['size']
 assert x['source']['checksum']=='sha256:'+end['source_sha256'] and x['candidate']['checksum']=='sha256:'+end['sha256']
 assert x['source_inode']!=x['candidate_inode'] and v['CandidateBytes']<=v['SourceBytes']
 assert v['SourceBytes']-v['CandidateBytes']==v['SourceKeyBytes']-v['CandidateKeyBytes']-v['OwnerDataBytes']-v['OwnerDirectoryBytes']-v['OwnerFooterBytes']
 versions[v['Version']]+=1
 for k,value in v.items():
  if isinstance(value,int) and k!='Version':totals[k]+=value
 if v['Version']==10:totals['StoredOwners']+=v['Owners']
space=read('space-final.json');assert dict(totals)==space['counts'];assert {str(k):v for k,v in versions.items()}==space['formats'];assert len(rows)==len(ends)==space['files']==19949
assert sum(v['allocated_bytes'] for v in ends.values())==space['candidate_allocated']
assert space['allocated_saved_bytes']==space['source_allocated']-space['candidate_allocated']
assert space['saved_bytes']==totals['SourceBytes']-totals['CandidateBytes']==6508598105
assert totals['Keys']==2443423104 and totals['Records']==14302423656
assert sum(c['source'] for c in space['components'])==totals['SourceBytes'];assert sum(c['candidate'] for c in space['components'])==totals['CandidateBytes']
assert sum(x['candidate']['size']<x['source']['size'] for x in rows)==space['smaller_files']==19718
assert sum(x['candidate']['size']>x['source']['size'] for x in rows)==space['larger_files']==0
assert sum(x['candidate']['size']==x['source']['size'] for x in rows)==space['equal_files']==231
complete=lines('complete-input-files.jsonl.gz');assert len(complete)==4609
assert sum(x['audit']['Keys'] for x in complete)==292521921
assert sum(x['audit']['SourceBytes']-x['audit']['CandidateBytes'] for x in complete)==437626392
allocation=read('complete-input-space-final.json');assert allocation['complete'] and allocation['files']==4609
for field in ['source_bytes','candidate_bytes','source_allocated_bytes','candidate_allocated_bytes']:assert allocation[field]==sum(r[field] for r in allocation['per_file'])
assert allocation['source_bytes']-allocation['candidate_bytes']==437626392
assert allocation['source_allocated_bytes']-allocation['candidate_allocated_bytes']==allocation['saved_allocated_bytes']==437239808
routing=read('complete-input-routing.json');assert routing['complete'] and routing['strict_reconstruction_passed'] and routing['canonical_frontiers_unchanged']
assert all(r['covered_pairs']==4609 and r['membership_payload_unchanged'] for r in routing['routes'])
requests=lines('http-requests.jsonl.gz');assert len(requests)==286;groups=collections.defaultdict(list);resources={}
for x in lines('server-requests.jsonl.gz'):
 r=x['request']
 if r['id']==999999:continue
 k=x['mode'],r['id'];assert k not in resources;resources[k]=r
assert len(resources)==len(requests)
def io(s):return {k:int(v) for k,v in (line.split(':',1) for line in s.splitlines())}
def cpu(t):return t['Sec']*1000+t['Usec']/1000
metrics=collections.defaultdict(lambda:collections.defaultdict(list))
for r in requests:
 assert r['correct'] and r['response']['id']==r['id'] and 'error' not in r['response'] and r['response']['result']==r['expected']
 x=resources[r['mode'],r['id']];assert x['params'][1]==r['tag']
 if r['cache'] not in ['warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process']:continue
 k=r['mode'],r['cache'],r['tag'];groups[k].append(r['milliseconds']);before=io(x['io_before']);after=io(x['io_after'])
 v=dict(milliseconds=r['milliseconds'],server_ms=x['elapsed_ns']/1e6,alloc_bytes=x['alloc_bytes'],cpu_ms=sum(cpu(x['usage_after'][f])-cpu(x['usage_before'][f]) for f in ['Utime','Stime']),**{f:after[f]-before[f] for f in ['rchar','syscr','read_bytes']})
 for f,z in v.items():metrics[k][f].append(z)
assert sum(map(len,groups.values()))==238 and len(groups)==42
for s in read('http-summary.json.gz'):
 v=groups[s['mode'],s['cache'],s['tag']];assert len(v)==s['n'] and statistics.median(v)==s['median_ms'] and min(v)==s['min_ms'] and max(v)==s['max_ms']
for s in read('http-resource-summary.json'):
 v=metrics[s['mode'],s['cache'],s['tag']]
 for f,z in v.items():assert s[f]==dict(median=statistics.median(z),min=min(z),max=max(z)),(s['mode'],s['tag'],f)
proofs=read('cold-residency-proofs.json');h=read('http-final.json');assert len(proofs)==h['cold_residency_proofs']==56;assert max(v['fraction'] for v in proofs.values())==h['max_pre_start_residency']<0.01
for mode in ['baseline','candidate']:
 manifest=read('http-'+mode+'-private-files.json.gz');assert len(manifest)==h['private_inputs_sha_verified_files'][mode]==2254
 assert all(x['sha256'] and x['source_inode']!=x['private_inode'] for x in manifest)
 assert read('nine-million-'+mode+'-workload.json')['responses_correct']
mu=read('muhash-census.json');assert mu['complete'] and mu['muhash_verified'] and bytes(mu['muhash_root']).hex()==h['head']['root']=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
restore=read('history-restore.json');assert restore['complete'];assert sum(w['rows'] for w in restore['windows'])==22496
for w in restore['windows']:assert w['candidate_row_sha256']==w['full_row_sha256']==w['restored_row_sha256'] and w['range_rows_verified']==20
native=read('native-status.json.gz');assert native['complete'] and native['format_encoding_unchanged'] and native['read_only_decoder_refinement'];root=P.parents[3]
assert native['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf'
for name,d in native['production_source_inventory'].items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==d,name
assert len(native['production_source_inventory'])==1077
for n,c in read('local/local-verification.json')['v2_final_checks'].items():assert c['successful'] and hashlib.sha256((P/'local'/n).read_bytes()).hexdigest()==c['sha256']
identity=read('local/reader-refinement-byte-identity.json');assert identity['source_and_candidate_sha256_unchanged'] and identity['fixtures']==3
assert data('initial/local-real-fixtures.json')==data('initial/local-real-fixtures-v1.json')
assert hashlib.sha256(data('initial/local-real-fixtures.json')).hexdigest()==identity['receipt_sha256']
state=read('http-comparison-status.json');assert state['complete'] and state['all_responses_correct'] and not state['environment_identity_changed'] and state['before_identity']==state['after_identity']
context=read('final-context.json');assert context['ActiveState']=='active' and context['MainPID']==str(state['after_identity']['pid']) and context['sha256']==state['after_identity']['sha256']
assert native['binaries']['candidate.test']==h['candidate_sha256']==state['candidate_binary_sha256']
assert native['binaries']['baseline.test']==h['baseline_sha256']==state['baseline_binary_sha256']
for expected in read('query-trace-summary.json'):
 name=expected['name'];mode=expected['mode'];markers=0;pending={};counts=collections.defaultdict(collections.Counter)
 prefix=Path('/data/gtron/history-key-owner-20261025-01')/(mode+'-input')
 for line in data('traces/'+name+'-'+mode+'.trace.gz').decode(errors='replace').splitlines():
  if re.search(r'read\(\d+</proc/\d+/io>, "rchar:',line):markers+=1;continue
  if markers!=1:continue
  m=re.match(r'\s*(\d+)\s+(.*)',line)
  if not m:continue
  tid,body=m.groups()
  if '<unfinished ...>' in body:pending[tid]=body.split('<unfinished ...>')[0];continue
  if body.startswith('<... ') and ' resumed>' in body:
   before=pending.pop(tid,None)
   if before is None:continue
   body=before+body.split(' resumed>',1)[1]
  m=re.match(r'(read|pread64)\(\d+<([^>]+)>',body);n=re.search(r'= (\d+)\s*$',body)
  if not m or not n or int(n.group(1))==0:continue
  file=Path(m.group(2));size=int(n.group(1))
  try:file.relative_to(prefix)
  except ValueError:continue
  counts[file.suffix or '(no suffix)'].update(calls=1,bytes=size,small_reads=int(size<256))
 assert markers==expected['query_io_markers']==2 and dict(counts)==expected['by_suffix']
 assert sum(x['calls'] for x in counts.values())==expected['database_read_calls']
 assert sum(x['bytes'] for x in counts.values())==expected['database_read_bytes']
 assert sum(x['small_reads'] for x in counts.values())==expected['small_read_calls']
previous=P.parent/'20261024-history-offset-pages';prior={x['path']:x['sha256'] for x in json.loads(gzip.decompress((previous/'end-output-checksums.json.gz').read_bytes()))}
assert len(prior)==19949 and all(prior[n]==x['source_sha256'] for n,x in ends.items())
growth=P.parent/'20261024-cold-growth';ranges={}
for x in map(json.loads,gzip.decompress((growth/'growth-spans.jsonl.gz').read_bytes()).splitlines()):
 if 'error' not in x:ranges[x['ref']['path'].removesuffix('.seg')+'.kv']=x
buckets=collections.defaultdict(collections.Counter)
for x in map(json.loads,gzip.decompress((growth/'frozen-kv-domains.jsonl.gz').read_bytes()).splitlines()):
 row=byname[x['path']];audit=row['audit'];assert x['keys']==audit['Keys'] and x['records']==audit['Records'] and x['dictionary_digest']==bytes(audit['DictionaryDigest']).hex()
 span=ranges[x['path']];lo=span['first']['BlockNum'];hi=span['last']['BlockNum'];a=(lo-1)//5000000;b=(hi-1)//5000000
 label=(str(a*5000000)+'-'+str((a+1)*5000000)) if a==b else 'boundary'
 buckets[label].update(SourceBytes=audit['SourceBytes'],CandidateBytes=audit['CandidateBytes'],files=1)
b=read('by-height.json')
for label,c in buckets.items():
 expected=b['boundary'] if label=='boundary' else b['bins'][label];assert all(expected[k]==v for k,v in c.items())
assert sum(c['files'] for c in buckets.values())==19949
print('Verified 19,949 accessors; net saved',space['saved_bytes'],'B; allocated saved',space['allocated_saved_bytes'],'B; 238 timed / 286 correct HTTP checks; MuHash and 22,496 restored rows; source files',len(native['production_source_inventory']))
