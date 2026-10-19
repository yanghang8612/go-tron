from pathlib import Path
import json,os,stat,hashlib,subprocess,datetime,base64,zlib,urllib.request,sys
p=Path('/data/gtron/compact-v3-rollout-20261019-01');stamp=sys.argv[1];assert len(stamp)==13 and stamp.startswith('2026') and stamp[8]=='T' and (stamp[:8]+stamp[9:]).isdigit();e=p/('heartbeat-'+stamp);e.mkdir(exist_ok=False)
f=p/'runtime-samples.jsonl'
with f.open('rb') as stream:
 size=f.stat().st_size;offset=max(0,size-(2<<20));stream.seek(offset)
 if offset:stream.readline()
 data=stream.read().decode()
lines=data.splitlines()
if data and not data.endswith(chr(10)):lines=lines[:-1]
rows=[json.loads(x) for x in lines[-45:]];raw=''.join(json.dumps(x,separators=(',',':'))+chr(10) for x in rows)
show=subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode();fields=dict(x.split('=',1) for x in show.splitlines() if '=' in x);pid=int(fields['MainPID']);assert pid>0
sha=hashlib.sha256(Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest();v={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'binary_sha256':sha,'expected_identity':sha=='859f0b6b44dcad8b74905a7d889d93ab1317d91a2365aa8aab1c30c455313505','pid':pid,'service':fields,'exe':os.path.realpath('/proc/'+str(pid)+'/exe'),'samples':len(rows),'sample_window_note':'Last at most 45 complete minute samples; tail bounded to 2 MiB.','runtime_log_bytes':size,'latest':rows[-1],'sample_errors':[{'utc':r['utc'],'errors':r['errors'],'log_errors':r.get('new_error_lines')} for r in rows if r['errors'] or r.get('new_error_lines')],'sampler':subprocess.check_output(['systemctl','show','gtron-compact-v3-observe-20261019.service','-p','ActiveState','-p','MainPID']).decode(),'timer':subprocess.check_output(['systemctl','show','gtron-deploy.timer','-p','ActiveState','-p','SubState']).decode()}
ret=json.loads((p/'retention.json').read_text());old=Path(ret['retained']);inv=[]
for parent,dirs,names in os.walk(str(old),followlinks=False):
 dirs.sort()
 for name in sorted(names):
  f=Path(parent)/name;s=f.lstat();inv.append({'path':str(f.relative_to(old)),'dev':s.st_dev,'inode':s.st_ino,'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'mode':stat.S_IMODE(s.st_mode)})
v['old_inventory_sha256']=hashlib.sha256(json.dumps(inv,separators=(',',':')).encode()).hexdigest();v['old_inventory_unchanged']=v['old_inventory_sha256']==ret['inventory_sha256'];v['old_files']=len(inv)
with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=8) as f:v['wallet']=json.load(f)
with urllib.request.urlopen('http://127.0.0.1:6062/debug/metrics',timeout=10) as f:m=json.load(f)['metrics']
v['cold_metrics']={k:r for k,r in m.items() if k.startswith(('state/snapshot/cold/last/','state/prune/last/domain_change/')) or k in ['state/snapshot/cold/errors','state/snapshot/cold/latest/deferred/sync','sync/import/window/blocks_per_second','sync/import/window/transactions_per_second','sync/import/window/energy_per_second']};v['memory']=subprocess.check_output(['free','-b']).decode();v['collector_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();(e/'status.json').write_text(json.dumps(v,indent=2));(e/'runtime-samples.jsonl').write_text(raw);packet=json.dumps({'status.json':json.dumps(v,indent=2),'runtime-samples.jsonl':raw},separators=(',',':')).encode();(e/'packet.json').write_bytes(packet);print(chr(27)+'[2J'+chr(27)+'[H',end='');print('GTRROL:'+base64.b64encode(zlib.compress(packet,9)).decode());print('PACKET_SHA256',hashlib.sha256(packet).hexdigest(),'BYTES',len(packet))
