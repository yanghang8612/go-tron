# Bounded loopback sampling. No cache eviction, DB writes, or service control.
import pathlib,sys,json,os,time,datetime,subprocess,hashlib,urllib.request,traceback,re,gzip
R=pathlib.Path('/data/gtron/query-plan-rollout-20261028-01')
mode=sys.argv[1];assert mode in ['before','after','followup']
out=R/('live-'+mode);out.mkdir(exist_ok=True)
s={'complete':False,'mode':mode,'cache':'uncontrolled production caches; no eviction','requests':[]}
def save():
 (out/'status.json').write_text(json.dumps(s,indent=2))
def get(url):
 with urllib.request.urlopen(url,timeout=20) as f:return f.read()
def snapshot(label):
 props=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','gtron']).decode().splitlines() if '=' in x)
 pid=props['MainPID'];assert pid!='0' and props['ActiveState']=='active'
 argv=[x.decode() for x in pathlib.Path('/proc/'+pid+'/cmdline').read_bytes().split(b'\0') if x]
 env=dict(x.split(b'=',1) for x in pathlib.Path('/proc/'+pid+'/environ').read_bytes().split(b'\0') if b'=' in x)
 roots={}
 for name in ['.','chaindata','history','state-snapshots','ancient']:
  p=pathlib.Path('/data/gtron/main/datadir/gtron')/name
  if p.exists():st=p.stat();roots[name]={'path':str(p.resolve()),'dev':st.st_dev,'ino':st.st_ino}
 snap=pathlib.Path(roots['state-snapshots']['path']);manifest=json.loads((snap/'manifest.json').read_text());refs=manifest['segments'];routes=[]
 for r in refs:
  if r['kind'] in ['system-history-routing','small-system-history-routing']:
   p=snap/r['path']
   try:
    with p.open('rb') as f:header=f.read(16)
    routes.append(dict(r,sources=int.from_bytes(header[12:16],'big')))
   except FileNotFoundError:routes.append(dict(r,concurrently_retired=True))
 metrics_port=argv[argv.index('--metrics.port')+1] if '--metrics.port' in argv else None
 if metrics_port:
  try:
   b=get('http://127.0.0.1:'+metrics_port+'/metrics');gzip.open(str(out/(label+'-metrics.txt.gz')),'wb').write(b)
   (out/(label+'-metrics-selected.txt')).write_text('\n'.join(x for x in b.decode().splitlines() if not x.startswith('#') and re.search('snapshot|posting|muhash|history.*(rout|bootstrap|publish|prune)|cold',x,re.I)))
  except Exception as e:s[label+'_metrics_error']=str(e)
 wallet=json.loads(get('http://127.0.0.1:8090/wallet/getnowblock'))
 result={'utc':datetime.datetime.utcnow().isoformat()+'Z','pid':pid,'exe':os.readlink('/proc/'+pid+'/exe'),'sha256':hashlib.sha256(pathlib.Path('/proc/'+pid+'/exe').read_bytes()).hexdigest(),'started':props.get('ExecMainStartTimestamp'),'argv':argv,'environment':{k:env[k.encode()].decode() for k in ['GOMAXPROCS','GOMEMLIMIT','GOGC','GTRON_HISTORY_ROLLOUT_READERS_ONLY','GTRON_HISTORY_COMPRESSION_FORMAT','GTRON_PEBBLE_BOUNDED_POINT_READ'] if k.encode() in env},'memory_current':props.get('MemoryCurrent'),'memory_limit':props.get('MemoryLimit'),'roots':roots,'height':wallet['block_header']['raw_data']['number'],'routes':routes,'cold_history_sources':sum(r['kind']=='history' and r.get('dataset')=='state-domain-change' for r in refs),'metadata_routes':sum(r['kind']=='metadata-history-routing' for r in refs)}
 p=pathlib.Path('/data/gtron/main/gtron.log')
 with p.open('rb') as f:
  f.seek(max(0,p.stat().st_size-(8<<20)));tail=f.read().decode(errors='replace')
 (out/(label+'-recent-errors.txt')).write_text('\n'.join(x for x in tail.splitlines() if re.search(r'(?i)muhash.*(error|fail|mismatch)|commitment.*(error|fail|mismatch)|\b(ERROR|FATAL|CRIT)\b|corrupt',x))[-128000:])
 (out/(label+'-snapshot.json')).write_text(json.dumps(result,indent=2));return result
def rpc(method,params,case,expected=None):
 q={'jsonrpc':'2.0','id':len(s['requests'])+1,'method':method,'params':params};start=time.perf_counter();row={'case':case,'utc':datetime.datetime.utcnow().isoformat()+'Z','method':method,'params':params}
 try:
  req=urllib.request.Request('http://127.0.0.1:8545',data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
  with urllib.request.urlopen(req,timeout=120) as f:body=json.loads(f.read())
  row.update(seconds_loopback_http=time.perf_counter()-start,response=body);s['requests'].append(row);save();assert 'error' not in body,row
  if expected is not None:assert int(body['result'],16)==expected,row
  return body['result']
 except Exception:
  if row not in s['requests']:row.update(seconds_loopback_http=time.perf_counter()-start,error=traceback.format_exc());s['requests'].append(row);save()
  raise
try:
 s['initial']=snapshot('initial');save()
 for height,value in [(9000000,207),(35000000,249)]:
  for repeat in range(1 if mode=='before' else 2):rpc('eth_call',[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)],str(height)+'-'+str(repeat),value)
 if mode!='before':
  recv='0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1'
  rpc('eth_getBalance',[recv,hex(341)],'balance-341',0)
  rpc('eth_getBalance',[recv,hex(342)],'balance-342',33251807524000000*10**12)
  rpc('eth_getBalance',['0x71b0af54e0a1182a5e0947d6a64f3b22740ef318','0x0'],'genesis-balance',99000000000000000*10**12)
  tx=rpc('eth_getTransactionByHash',['0x5281eb99b3cab6b3cb4777e6b070df936f2cb068163fb8f3959a1cd88733a4c2'],'transaction-342');assert int(tx['blockNumber'],16)==342,tx
 s['final']=snapshot('final');assert s['initial']['pid']==s['final']['pid'];s['complete']=True;save()
except BaseException:s['error']=traceback.format_exc();save();raise
