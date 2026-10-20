import datetime,json,pathlib,subprocess,time,traceback,os
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
UNIT='gtron-cold-account-gc-final-20261020.service'
state={'complete':False,'production_mutations':False,'unit':UNIT}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'account-gc-resource-status.json').write_text(json.dumps(state,indent=2))
def kv(path):
 result={}
 try:
  for line in path.read_text().splitlines():
   k,_,v=line.partition(':');result[k]=v.strip()
 except OSError:pass
 return result
try:
 save();begin=time.monotonic();samples=0
 while time.monotonic()-begin<10800:
  active=subprocess.check_output(['systemctl','show',UNIT,'-p','ActiveState','-p','MainPID']).decode().splitlines();unit=dict(s.split('=',1) for s in active)
  if unit.get('ActiveState') not in ('active','activating'):
   state.update(complete=True,phase='unit-finished');break
  parent=int(unit.get('MainPID') or 0);pids=[parent] if parent else []
  if parent:
   try:pids.extend(int(p) for p in pathlib.Path('/proc/%d/task/%d/children'%(parent,parent)).read_text().split())
   except OSError:pass
  row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'unit':unit,'processes':[]}
  for pid in pids:
   base=pathlib.Path('/proc/%d'%pid)
   try:
    stat=base.joinpath('stat').read_text().rsplit(')',1)[1].split();status=kv(base/'status');io=kv(base/'io')
    row['processes'].append({'pid':pid,'name':status.get('Name'),'utime_ticks':int(stat[11]),'stime_ticks':int(stat[12]),'start_ticks':int(stat[19]),'rss':status.get('VmRSS'),'peak_rss':status.get('VmHWM'),'io':io})
   except OSError:pass
  cg=pathlib.Path('/sys/fs/cgroup/memory/system.slice')/UNIT
  for name in ('memory.usage_in_bytes','memory.max_usage_in_bytes','memory.limit_in_bytes'):
   try:row[name]=int((cg/name).read_text())
   except OSError:pass
  for cpu in ('cpu,cpuacct','cpu'):
   cp=pathlib.Path('/sys/fs/cgroup')/cpu/'system.slice'/UNIT/'cpu.stat'
   try:row['cpu_stat']=cp.read_text();break
   except OSError:pass
  with (ROOT/'account-gc-resources.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
  samples+=1;state.update(phase='sampling-build-only',samples=samples);save();time.sleep(5)
 else:state.update(complete=False,phase='bounded-expiry')
 save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
