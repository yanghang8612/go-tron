import gzip,json,pathlib,collections,sys
P=pathlib.Path(sys.argv[1]);E=pathlib.Path('docs/dev/evidence/20261024-cold-growth')
with gzip.open(E/'growth-spans.jsonl.gz','rt') as f:spans={r['ref']['path'].removesuffix('.seg')+'.kv':(r['first']['BlockNum'],r['last']['BlockNum']) for r in map(json.loads,f) if "first" in r and "last" in r}
with gzip.open(E/'frozen-kv-domains.jsonl.gz','rt') as f:old={r['path']:{'size':r['size'],'keys':r['keys'],'records':r['records'],'digest':r['dictionary_digest']} for r in map(json.loads,f)}
bins=[(0,5000000),(5000000,10000000),(10000000,15000000),(15000000,20000000),(20000000,25000000),(25000000,30000000),(30000000,35000000)]
counts={str(a)+'-'+str(b):collections.Counter() for a,b in bins};boundary=collections.Counter();seen=set()
for r in map(json.loads,(P/'corpus-files.jsonl').open()):
 n=r['source']['path'];a=r['audit'];lo,hi=spans[n];assert n not in seen;seen.add(n)
 assert a['Keys']==old[n]['keys'] and a['Records']==old[n]['records'] and bytes(a['DictionaryDigest']).hex()==old[n]['digest']
 group=next((counts[str(x)+'-'+str(y)] for x,y in bins if lo>x and hi<=y),boundary)
 for k,v in a.items():
  if isinstance(v,int) and k!='Version':group[k]+=v
 group['files']+=1;group['smaller']+=a['CandidateBytes']<a['SourceBytes'];group['original_v7_bytes']+=old[n]['size']
 if not group['first_block'] or lo<group['first_block']:group['first_block']=lo
 group['last_block']=max(hi,group['last_block'])
assert len(seen)==19949
out={'all_files':len(seen),'scope':'current immutable cohort, maximum height 33,248,191; complete files only per interval; boundary files separate; not historical disk snapshots','bins':{k:dict(v) for k,v in counts.items()},'boundary':dict(boundary)}
(P/'by-height.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
