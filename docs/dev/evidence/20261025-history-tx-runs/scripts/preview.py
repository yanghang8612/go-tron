import pathlib,json,zipfile,hashlib,os
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');B=pathlib.Path('/data/gtron/history-key-owner-20261025-01');rows=[json.loads(l) for l in (P/'corpus-files.jsonl.2workers-interrupted').open()];chosen=[]
predicates=[lambda a:a['Version']==11 and a['OwnerDataBytes']>0,lambda a:a['Version']==11 and a['OwnerDataBytes']==0,lambda a:a['Version']!=11,lambda a:a['Version']==11 and a['Records']>1000000]
for pred in predicates:
 for row in rows:
  if pred(row['audit']) and row['source']['path'] not in {r['source']['path'] for r in chosen}:chosen.append(row);break
assert len(chosen)>=2
out=pathlib.Path('/tmp/gtron-history-tx-runs-preview-20261025-01.zip');assert not out.exists();checks={}
with zipfile.ZipFile(str(out),'w',compression=zipfile.ZIP_DEFLATED) as z:
 def add(n,data):z.writestr(n,data);checks[n]=hashlib.sha256(data).hexdigest()
 add('fixtures.json',json.dumps(chosen,indent=2).encode())
 for i,r in enumerate(chosen):
  for label,base,key in [('baseline',B/'corpus-v10','source'),('candidate',P/'corpus-v11','candidate')]:
   data=(base/r[key]['path']).read_bytes();assert len(data)==r[key]['size'] and 'sha256:'+hashlib.sha256(data).hexdigest()==r[key]['checksum'];add(label+'/'+str(i)+'.kv',data)
 for n in ['preflight.json','native-status.json','prepare-status.json','census-status.json','corpus-resume-authorization.json']:
  if (P/n).exists():add(n,(P/n).read_bytes())
 z.writestr('SHA256SUMS.json',json.dumps(checks,indent=2))
os.chmod(str(out),0o644);print('PREVIEW',len(chosen),'fixtures',out.stat().st_size,hashlib.sha256(out.read_bytes()).hexdigest())
