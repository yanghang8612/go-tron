"""Create a logical fixed clone; do not evict any file pages in this phase."""
import pathlib,os,json,shutil,subprocess,time,traceback
R=pathlib.Path('/data/gtron/maintenance/whole-query-cold-20261029-01');R.mkdir()
source=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-runtime/input-runtime')
dest=R/'input';dest.mkdir()
status={'complete':False,'stage':'clone fixed input with private mutable files','production_mutations':False,'evictions':False};proof=[];links=[]
def identity():
 d=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','ExecMainStartTimestamp']).decode().splitlines());d['exe']=os.readlink('/proc/'+d['MainPID']+'/exe');return d
def save():status['utc']=time.time();(R/'prepare-status.json').write_text(json.dumps(status,indent=2))
try:
 status['production_before']=identity();status['free_bytes_before']=shutil.disk_usage(str(R)).free;save()
 assert status['free_bytes_before']>400_000_000_000
 counts={'files':0,'linked_bytes':0,'private_mutable_bytes':0}
 for kind in ['chaindata','history','ancient','state-snapshots']:
  base=(source/kind).resolve();target=dest/kind;target.mkdir();status['stage']='clone '+kind;save()
  for directory,dirs,files in os.walk(str(base),followlinks=False):
   relative=pathlib.Path(directory).relative_to(base);out=target/relative;out.mkdir(exist_ok=True)
   for name in list(dirs):
    p=pathlib.Path(directory)/name
    if p.is_symlink():os.symlink(os.readlink(str(p)),str(out/name));links.append({'path':str((out/name).relative_to(dest)),'target':os.readlink(str(p))});dirs.remove(name)
   for name in files:
    p=pathlib.Path(directory)/name;q=out/name
    if p.is_symlink():os.symlink(os.readlink(str(p)),str(q));links.append({'path':str(q.relative_to(dest)),'target':os.readlink(str(p))});continue
    st=p.stat();immutable=(p.suffix.lower() in ['.sst','.ldb']) if kind in ['chaindata','history'] else p.suffix.lower() not in ['.json','.tmp','.log']
    if immutable:os.link(str(p),str(q));counts['linked_bytes']+=st.st_size
    else:
     assert counts['private_mutable_bytes']+st.st_size<16<<30,'unexpected mutable copy size'
     shutil.copy2(str(p),str(q));actual=q.stat();assert (actual.st_dev,actual.st_ino)!=(st.st_dev,st.st_ino)
     proof.append({'path':str(q.relative_to(dest)),'source':str(p),'bytes':st.st_size,'source_dev':st.st_dev,'source_ino':st.st_ino,'private_dev':actual.st_dev,'private_ino':actual.st_ino});counts['private_mutable_bytes']+=st.st_size
    counts['files']+=1
    if counts['files']%1000==0:status.update(counts);save()
 for p in source.iterdir():
  if p.name not in ['chaindata','history','ancient','state-snapshots']:os.symlink(str(p.resolve()),str(dest/p.name),target_is_directory=p.is_dir())
 (R/'mutable-copy-proof.json').write_text(json.dumps(proof,indent=2));(R/'nested-symlinks.json').write_text(json.dumps(links,indent=2))
 status.update(counts);status['production_after']=identity();assert status['production_before']==status['production_after'];status.update(complete=True,stage='private mutable clone ready; immutable files still shared and NOT eligible for eviction');save()
except BaseException:status.update(stage='failed',error=traceback.format_exc());save();raise
