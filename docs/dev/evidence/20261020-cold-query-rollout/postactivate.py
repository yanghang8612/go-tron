from pathlib import Path
import subprocess,os,json,time,datetime,hashlib,urllib.request,stat
p=Path('/data/gtron/cold-query-rollout-20261020-9030ce7a');rev='9030ce7a31016491ece014ac76e5656ec149417e'
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def status(phase,**extra):
 v={'utc':utc(),'phase':phase};v.update(extra);(p/'postactivate-status.json').write_text(json.dumps(v,indent=2));print(json.dumps(v),flush=True)
def run(args,timeout=30):
 r=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=timeout);assert r.returncode==0,(args,r.returncode,r.stdout.decode()[-2000:]);return r.stdout.decode(errors='replace')
def show(unit):return dict(x.split('=',1) for x in run(['systemctl','show',unit,'-p','MainPID','-p','ActiveState','-p','Result']).splitlines() if '=' in x)
def wallet():
 with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=8) as f:return json.load(f)
try:
 status('waiting-for-original-deployment')
 for i in range(360):
  d=show('gtron-deploy.service');s=show('gtron.service');pid=int(s.get('MainPID','0'));exe=os.path.realpath('/proc/'+str(pid)+'/exe') if pid else ''
  if '/auto-9030ce7a3101-' in exe and d.get('ActiveState')=='inactive' and d.get('Result')=='success':break
  if d.get('ActiveState')=='failed':raise RuntimeError('original deploy failed; inspect start.log')
  time.sleep(5)
 else:raise RuntimeError('new release not ready within 30 minutes')
 status('verify-release',pid=pid,exe=exe)
 proof=json.loads(run(['/usr/bin/python3','/usr/local/libexec/gtron-mainnet-release.py','verify','--source',rev],timeout=60).strip().splitlines()[-1]);sha=hashlib.sha256(Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest();assert proof['verified'] and proof['binary_sha256']==sha and proof['source_commit']==rev
 assert Path('/data/gtron/main/datadir').stat().st_ino==94642617
 assert Path('/data/gtron/main/datadir/gtron/chaindata/CURRENT').is_file()
 before=json.loads((p/'before-deploy-wallet.json').read_text());w=wallet();assert w['currentBlock']>=before['currentBlock'] and not w['syncInfo']['paused']
 old=Path('/data/gtron/main/datadir-pre-compact-v3-20261019-659ab078');inv=[]
 for parent,dirs,names in os.walk(str(old),followlinks=False):
  dirs.sort()
  for name in sorted(names):
   f=Path(parent)/name;s=f.lstat();inv.append({'path':str(f.relative_to(old)),'dev':s.st_dev,'inode':s.st_ino,'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'mode':stat.S_IMODE(s.st_mode)})
 invsha=hashlib.sha256(json.dumps(inv,separators=(',',':')).encode()).hexdigest();assert invsha=='318de55a385e3a506bf37463a228fe2a91803b26a7e2eecd0d92af2b2a5165fb'
 identity={'source':rev,'binary_sha256':sha,'pid':pid,'exe':exe,'release_proof':proof,'wallet':w,'old_inventory_unchanged':True,'old_inventory_sha256':invsha,'datadir_inode':94642617,'buildinfo':run(['/data/go/bin/go','version','-m',exe]),'service':run(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','ExecStart','-p','MemoryLimit']),'utc':utc()};(p/'identity.json').write_text(json.dumps(identity,indent=2))
 stopped=subprocess.run(['systemctl','stop','gtron-compact-v3-observe-20261019.service'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 assert stopped.returncode==0 or (stopped.returncode==5 and b'not loaded' in stopped.stdout),(stopped.returncode,stopped.stdout)
 run(['systemctl','start','gtron-deploy.timer'])
 status('new-release-verified-probing',pid=pid,binary_sha256=sha,head=w['currentBlock'])
 (p/'after-local.log').write_text(run(['/usr/bin/python3',str(p/'probe-local.py'),'after-local','3'],timeout=400))
 status('profiling-bounded-history-load',pid=pid,binary_sha256=sha)
 (p/'after-profile.log').write_text(run(['/usr/bin/python3',str(p/'profile-load.py'),'after'],timeout=240))
 run(['systemd-run','--unit=gtron-cold-query-observe-20261020','--property=MemoryLimit=268435456','/usr/bin/python3',str(p/'monitor.py')])
 status('complete-observer-running',pid=pid,binary_sha256=sha,head=wallet()['currentBlock'])
except BaseException as e:
 status('failed',error=repr(e));raise
