import pathlib,json,hashlib,zipfile,subprocess,os,datetime
R=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
for root in [R,R/'cold',R/'random',R/'window32']:
 s=json.loads((root/'candidate-status.json').read_text());assert s['complete'] and s['all_returns_match'];assert s['production_before']==s['production_after']
identity=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','ExecMainStartTimestamp']).decode().splitlines());identity['exe']=os.readlink('/proc/'+identity['MainPID']+'/exe');assert identity==s['production_after']
(R/'final-identity.json').write_text(json.dumps(identity,indent=2))
paths=[]
for root in [R,R/'cold',R/'random',R/'window32']+[p for p in R.iterdir() if p.is_dir() and p.name.startswith('random-failed')]:
 if not root.exists():continue
 paths.extend(p for p in root.iterdir() if p.is_file() and p.suffix in ['.json','.jsonl','.log','.py'])
 for folder in root.iterdir():
  if folder.is_dir() and (folder/'ready.json').is_file():paths.extend(p for p in folder.iterdir() if p.is_file() and p.suffix in ['.json','.jsonl','.log'])
files={};packet={}
for p in sorted(set(paths)):
 assert p.stat().st_size<20<<20,str(p)
 name='docs/dev/evidence/20261028-tail-read-ahead/native/'+str(p.relative_to(R));raw=p.read_bytes();packet[name]=raw;files[name]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
seal={'utc':datetime.datetime.utcnow().isoformat()+'Z','lookahead_source':'80293e10cbc9ff983fe112dad2b9dafa08a29ed0','random_io_source':'9365e06710d5cdc7ac836e0b40cbde09e8977906','window_source':json.loads((R/'window32/candidate-status.json').read_text())['source'],'files':files}
p=pathlib.Path('/tmp/AAA-gtron-tail-read-ahead-final-20261028-01.zip')
with zipfile.ZipFile(str(p),'w',zipfile.ZIP_DEFLATED) as z:
 for name,raw in packet.items():z.writestr(name,raw)
 z.writestr('docs/dev/evidence/20261028-tail-read-ahead/native/member-seal.json',json.dumps(seal,indent=2))
os.chmod(str(p),420);h=hashlib.sha256(p.read_bytes()).hexdigest();print('FINAL_PACKET',p.stat().st_size,len(files));print([h[i:i+16] for i in range(0,64,16)])
