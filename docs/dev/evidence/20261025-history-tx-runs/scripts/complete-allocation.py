import pathlib,json,os,datetime,hashlib; p=pathlib.Path('/data/gtron/history-tx-runs-20261025-01'); rows=list(map(json.loads,(p/'complete-input-files.jsonl').open())); checks=[]
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4<<20),b''):h.update(b)
 return h.hexdigest()
for r in rows:
 a=p/'baseline-input/state-snapshots'/r['source']['path']; b=p/'candidate-input/state-snapshots'/r['candidate']['path']
 with b.open('rb') as f:os.fsync(f.fileno())
 sa=a.stat(); sb=b.stat(); assert sa.st_size==r['source']['size'] and sb.st_size==r['candidate']['size']; assert sa.st_ino==r['source_inode'] and sb.st_ino==r['candidate_inode'] and sa.st_ino!=sb.st_ino
 ah=sha(a);bh=sha(b);assert 'sha256:'+ah==r['source']['checksum'] and 'sha256:'+bh==r['candidate']['checksum']
 checks.append({'source_sha256':ah,'candidate_sha256':bh,'path':r['source']['path'],'source_bytes':sa.st_size,'candidate_bytes':sb.st_size,'source_allocated_bytes':sa.st_blocks*512,'candidate_allocated_bytes':sb.st_blocks*512})
x={'complete':True,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':len(checks),'scope':'same complete fixed capture accessor files only; after candidate fsync; content hashes in complete-input-files receipts','per_file':checks}; x.update({k:sum(v[k] for v in checks) for k in ['source_bytes','candidate_bytes','source_allocated_bytes','candidate_allocated_bytes']}); x['saved_allocated_bytes']=x['source_allocated_bytes']-x['candidate_allocated_bytes']; (p/'complete-input-space-final.json').write_text(json.dumps(x)); print(json.dumps({k:v for k,v in x.items() if k!='per_file'}))
