import pathlib,json,zipfile,hashlib,datetime,subprocess
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');files={}
for x in P.iterdir():
 if x.is_file() and (x.name.endswith(('.json','.jsonl','.log','.py','.go','.txt','.v1','.v2','.v3')) and x.name!='SHA256SUMS.json'):files[x.name]=x.read_bytes()
m=json.loads((P/'baseline-input/state-snapshots/manifest.json').read_text());h=[r for r in m['segments']if r['kind']=='history' and r.get('dataset')=='state-domain-change'];files['checkpoint-summary.json']=json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_history_files=len(h),source_history_bytes=sum(r['size']for r in h),source_progress=m['progress'],production_identity=subprocess.check_output(['systemctl','show','gtron','-p','ActiveState','-p','MainPID']).decode()),indent=2).encode()
files.pop('evidence-sha256.json',None);files['evidence-sha256.json']=json.dumps({n:hashlib.sha256(b).hexdigest()for n,b in files.items()},indent=2).encode();out=pathlib.Path('/tmp/gtron-account-delta-checkpoint-20261025-01.zip')
with zipfile.ZipFile(str(out),'w',zipfile.ZIP_DEFLATED)as z:
 for n,b in files.items():z.writestr(n,b)
receipt=dict(bytes=out.stat().st_size,sha256=hashlib.sha256(out.read_bytes()).hexdigest(),members=len(files));pathlib.Path('/tmp/gtron-account-delta-checkpoint-20261025-01-receipt.json').write_text(json.dumps(receipt));print(receipt)
