from pathlib import Path
import os,json,subprocess,time,datetime,urllib.request,hashlib,traceback
p=Path('/data/gtron/compact-v3-rollout-20261019-01');root=Path('/data/gtron/main/datadir');dest=p/'checkpoint-1';build=json.loads((p/'build-status.json').read_text());status={'complete':False,'phase':'precheck'}
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save():
 status['utc']=utc();(p/'audit-status.json').write_text(json.dumps(status,indent=2))
def run(args,timeout=90):
 r=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=timeout);s=r.stdout.decode(errors='replace')
 if r.returncode:raise RuntimeError(str(args)+': '+s[-2000:])
 return s
def wallet():
 with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=6) as f:return json.load(f)
def identity():
 pid=int(run(['systemctl','show','gtron','-p','MainPID']).split('=',1)[1]);assert pid>0
 sha=hashlib.sha256(Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest();assert sha==build['sha256'];return pid
stopped=False
try:
 assert not dest.exists();assert (p/'generation-census').is_file();assert root.stat().st_ino==94642617
 status['before_pid']=identity();status['before_wallet']=wallet();status['stop_requested_utc']=utc();save();t=time.monotonic()
 run(['systemctl','stop','gtron'],660);stopped=True
 assert 'MainPID=0' in run(['systemctl','show','gtron','-p','MainPID']);status['stopped_utc']=utc()
 dest.mkdir(mode=0o700);src=root/'gtron'/'chaindata';assert (src/'CURRENT').is_file();target=dest/'chaindata';target.mkdir(mode=0o700)
 entries=[];linked=0;copied=0
 for f in sorted(src.iterdir()):
  assert f.is_file() and not f.is_symlink(),str(f)
  st=f.stat();out=target/f.name
  if f.suffix=='.sst':os.link(str(f),str(out));linked+=st.st_size;sha=None
  else:
   with f.open('rb') as i,out.open('xb') as o:
    h=hashlib.sha256()
    while True:
     b=i.read(1<<20)
     if not b:break
     o.write(b);h.update(b)
    o.flush();os.fsync(o.fileno())
   sha=h.hexdigest();copied+=st.st_size
  entries.append({'name':f.name,'bytes':st.st_size,'inode':st.st_ino,'dev':st.st_dev,'hardlinked':sha is None,'sha256':sha})
 manifest=json.dumps(entries,separators=(',',':')).encode();(p/'checkpoint-1-files.json').write_bytes(manifest)
 status.update({'checkpoint_files':len(entries),'sst_hardlink_bytes':linked,'copied_bytes':copied,'inventory_sha256':hashlib.sha256(manifest).hexdigest(),'copy_complete_utc':utc(),'phase':'restarting'});save()
 run(['systemctl','start','gtron'],180);stopped=False
 deadline=time.monotonic()+180
 while True:
  try:
   w=wallet();assert w['currentBlock']>=status['before_wallet']['currentBlock'];break
  except BaseException:
   if time.monotonic()>deadline:raise
   time.sleep(2)
 status.update({'after_pid':identity(),'after_wallet':w,'service_restored_utc':utc(),'stop_to_api_seconds':time.monotonic()-t,'phase':'offline-census'});save()
 verify=run(['/usr/bin/python3','/usr/local/libexec/gtron-mainnet-release.py','verify','--source',build['source']],180);(p/'restart-release-verify.txt').write_text(verify)
 with (p/'census.log').open('wb') as log:
  r=subprocess.run(['nice','-n','15','ionice','-c3',str(p/'generation-census'),'--db-dir',str(target),'--out',str(p/'census.json'),'--groups',str(p/'census-groups.jsonl'),'--max-duration','15m'],stdout=log,stderr=subprocess.STDOUT,timeout=930)
  assert r.returncode==0,'census rc '+str(r.returncode)
 report=json.loads((p/'census.json').read_text());assert report['complete'] and report['muhash_verified'],report.get('error')
 status.update({'complete':True,'phase':'verified','block_num':report['block_num'],'muhash_root':report['muhash_root'],'muhash_verified':True,'after_audit_wallet':wallet()});save()
except BaseException as e:
 status.update({'phase':'failed','error':repr(e),'traceback':traceback.format_exc()});save()
 if stopped:
  try:status['recovery_start']=run(['systemctl','start','gtron'],180);status['recovery_pid']=identity();save()
  except BaseException as ex:status['recovery_error']=repr(ex);save()
 raise
