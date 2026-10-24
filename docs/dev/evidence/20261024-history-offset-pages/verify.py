"""Verify compact evidence; run from anywhere in this repository worktree."""
from pathlib import Path
import json,gzip,hashlib,collections,statistics
P=Path(__file__).resolve().parent

def data(n):
 b=(P/n).read_bytes();return gzip.decompress(b) if n.endswith('.gz') else b

def read(n):return json.loads(data(n))
def lines(n):return list(map(json.loads,data(n).splitlines()))
for n,d in read('SHA256SUMS.json').items():assert hashlib.sha256((P/n).read_bytes()).hexdigest()==d,n
rows=lines('corpus-files.jsonl.gz');totals=collections.Counter();byname={};ends={x['path']:x for x in read('end-output-checksums.json.gz')}
for x in rows:
 name=x['source']['path'];assert name not in byname;byname[name]=x;v=x['audit'];end=ends[name]
 assert v['SourceBytes']==x['source']['size'] and v['CandidateBytes']==x['candidate']['size']==end['size']
 assert x['source']['checksum']=='sha256:'+end['source_sha256'] and x['candidate']['checksum']=='sha256:'+end['sha256']
 assert v['RemovedOffsetBytes']==v['SourcePostingBytes']-v['CandidatePostingBytes'];assert v['MultiRows']+v['Singletons']==v['Records'];assert v['MultiLists']+v['Singletons']==v['Keys']
 for k,value in v.items():
  if k!='DictionaryDigest':totals[k]+=value
space=read('space-final.json');assert dict(totals)==space['counts'];assert len(rows)==len(ends)==space['files']==19949
assert sum(v['allocated_bytes'] for v in ends.values())==space['candidate_allocated']
assert space['saved_bytes']==totals['SourceBytes']-totals['CandidateBytes']==17398010359
assert sum(c['source'] for c in space['components'])==totals['SourceBytes'];assert sum(c['candidate'] for c in space['components'])==totals['CandidateBytes']
assert sum(x['candidate']['size']<x['source']['size'] for x in rows)==space['smaller_files'];assert sum(x['candidate']['size']>x['source']['size'] for x in rows)==space['larger_files']
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
assert sum(map(len,groups.values()))==238
for s in read('http-summary.json.gz'):
 v=groups[s['mode'],s['cache'],s['tag']];assert len(v)==s['n'] and statistics.median(v)==s['median_ms'] and min(v)==s['min_ms'] and max(v)==s['max_ms']
for s in read('http-resource-summary.json'):
 v=metrics[s['mode'],s['cache'],s['tag']]
 for f,z in v.items():assert s[f]==dict(median=statistics.median(z),min=min(z),max=max(z)),(s['mode'],s['tag'],f)
proofs=read('cold-residency-proofs.json');h=read('http-final.json');assert len(proofs)==h['cold_residency_proofs']==56;assert max(v['fraction'] for v in proofs.values())==h['max_pre_start_residency']<0.01
for mode in ['baseline','candidate']:
 manifest=read('http-'+mode+'-private-files.json.gz');assert len(manifest)==h['private_inputs_sha_verified_files'][mode]
 assert all(x['sha256'] and x['source_inode']!=x['private_inode'] for x in manifest)
mu=read('muhash-census.json');assert mu['complete'] and mu['muhash_verified'] and bytes(mu['muhash_root']).hex()=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
restore=read('history-restore.json');assert restore['complete'];assert sum(w['rows'] for w in restore['windows'])==22496
for w in restore['windows']:assert w['candidate_row_sha256']==w['full_row_sha256']==w['restored_row_sha256'] and w['range_rows_verified']==20
recovery=read('diagnostic-recovery.json');assert recovery['original_timed_rows_unchanged'] and recovery['initial_timed_rows']==recovery['final_timed_rows']==238
native=read('native-status.json.gz');assert native['complete'];root=P.parents[3]
for name,d in native['production_source_inventory'].items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==d,name
# Cross-check the original V8 end hashes and real block ranges without filename inference.
previous=P.parent/'20261024-history-inline-directory';prior={x['path']:x['sha256'] for x in json.loads(gzip.decompress((previous/'end-output-checksums.json.gz').read_bytes()))}
assert len(prior)==19949 and all(prior[n]==x['source_sha256'] for n,x in ends.items())
growth=P.parent/'20261024-cold-growth';ranges={}
for x in map(json.loads,gzip.decompress((growth/'growth-spans.jsonl.gz').read_bytes()).splitlines()):
 if 'error' not in x:ranges[(x['ref']['fromTxNum'],x['ref']['toTxNum'])]=x
buckets=collections.defaultdict(collections.Counter)
for x in map(json.loads,gzip.decompress((growth/'frozen-kv-domains.jsonl.gz').read_bytes()).splitlines()):
 row=byname[x['path']];audit=row['audit'];assert x['keys']==audit['Keys'] and x['records']==audit['Records'] and x['dictionary_digest']==bytes(audit['DictionaryDigest']).hex()
 span=ranges[x['fromTxNum'],x['toTxNum']];a=(span['first']['BlockNum']-1)//5000000;b=(span['last']['BlockNum']-1)//5000000
 label=('%dM-%dM'%(a*5,(a+1)*5)) if a==b else 'boundary-%dM-%dM'%(a*5,(b+1)*5)
 buckets[label].update(v8_accessor_bytes=row['source']['size'],v9_accessor_bytes=row['candidate']['size'])
for label,counts in buckets.items():
 expected=read('growth-summary.json')['frozen']['groups'][label]['counts'];assert all(counts[k]==expected[k] for k in counts)
print('Verified 19,949 accessors; saved',space['saved_bytes'],'B; 238 timed / 286 correct HTTP checks; MuHash and 22,496 actual restored rows; source files',len(native['production_source_inventory']))
