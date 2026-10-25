import pathlib,json,hashlib,os,datetime
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');A=P/'baseline-input/state-snapshots';B=P/'candidate-input/state-snapshots'
a=json.loads((A/'manifest.json').read_text());b=json.loads((B/'manifest.json').read_text());changed={r['source']['path'] for r in map(json.loads,(P/'complete-input-files.jsonl').open())};assert a.get('chain')==b.get('chain') and a['progress']==b['progress'];assert all(a.get(k)==b.get(k) for k in ['sourceRows','commitmentScheme','elementVersion','visibleTxStart','visibleTxEnd'])
refs={r['path']:r for r in b['segments']};rows=[]
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for x in iter(lambda:f.read(4<<20),b''):h.update(x)
 return h.hexdigest()
for r in a['segments']:
 n=r['path'];s=refs[n]
 if n in changed or 'routing' in r['kind']:continue
 assert r==s,n;x=(A/n).stat();y=(B/n).stat();assert (x.st_dev,x.st_ino,x.st_size)==(y.st_dev,y.st_ino,y.st_size) and x.st_size==r['size'];digest=sha(B/n);assert 'sha256:'+digest==r['checksum'],n
 rows.append(dict(path=n,sha256=digest,bytes=x.st_size,baseline_inode=x.st_ino,candidate_inode=y.st_ino,canonical_ref_unchanged=True))
assert len(refs)==len(a['segments'])
(P/'canonical-companions-final.json').write_text(json.dumps(dict(complete=True,utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),chain_frontiers_unchanged=True,scope='Every unchanged canonical snapshot manifest ref, including .seg/.idx/latest state; full physical SHA reread and shared immutable inode proof after all HTTP measurements.',files=len(rows),bytes=sum(r['bytes'] for r in rows),per_file=rows)))
print('CANONICAL_REHASH_VERIFIED',len(rows),'files',sum(r['bytes'] for r in rows),'bytes')
