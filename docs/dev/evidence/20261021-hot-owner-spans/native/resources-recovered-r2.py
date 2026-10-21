import pathlib,json,datetime,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-03');units=['gtron-owner-blake-r2-'+s+'-20261021.service' for s in ['prepare','layout','compare','audit','diagnostic']];state={'complete':False,'production_mutations':False}
def save():state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=ROOT/'resource-status.json';tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2));tmp.replace(p)
def keyvals(p):
 try:return dict(line.split(':',1) for line in p.read_text().splitlines() if ':' in line)
 except OSError:return {}
try:
 start=time.monotonic();samples=0
 while time.monotonic()-start<9000:
  row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'units':{},'meminfo':{k:v.strip() for k,v in keyvals(pathlib.Path('/proc/meminfo')).items() if k in ['MemTotal','MemAvailable']}}
  active=False
  for unit in units:
   fields=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','MainPID','-p','MemoryLimit','-p','CPUQuotaPerSecUSec']).decode().splitlines());row['units'][unit]=fields
   if fields.get('ActiveState') not in ['active','activating']:continue
   active=True;pid=int(fields.get('MainPID') or 0);pids=[pid] if pid else []
   if pid:
    try:pids.extend(int(n) for n in pathlib.Path('/proc/%d/task/%d/children'%(pid,pid)).read_text().split())
    except OSError:pass
   fields['processes']=[]
   for pid in pids:
    p=pathlib.Path('/proc/%d'%pid);status=keyvals(p/'status');io=keyvals(p/'io');fields['processes'].append({'pid':pid,'name':status.get('Name'),'rss':status.get('VmRSS'),'peak_rss':status.get('VmHWM'),'io':io})
   cg=pathlib.Path('/sys/fs/cgroup/memory/system.slice')/unit
   for n in ['memory.usage_in_bytes','memory.max_usage_in_bytes']:
    try:fields[n]=int((cg/n).read_text())
    except OSError:pass
  with (ROOT/'resources.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
  samples+=1;state.update(phase='sampling',samples=samples);save()
  if not active:state.update(complete=True,phase='all-units-finished');save();break
  time.sleep(10)
 else:state.update(phase='bounded-expiry');save()
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
