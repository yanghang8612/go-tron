# Prepare the exact merged source without switching production or any DB files.
import pathlib,sys,json,subprocess,os,hashlib,datetime,traceback,urllib.request,collections
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
R=pathlib.Path('/data/gtron/query-plan-rollout-20261028-01');R.mkdir(exist_ok=True)
s={'complete':False,'stage':'production preflight','source':SOURCE,'production_mutations':False};requests=[]
def props(unit,*fields):return dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show',unit]+sum((['-p',f] for f in fields),[])).decode().splitlines())
def get_json(url):
 with urllib.request.urlopen(url,timeout=15) as f:return json.loads(f.read())
try:
 s['production_before']=identity();save();pid=s['production_before']['MainPID'];assert pid!='0'
 argv=pathlib.Path('/proc/'+pid+'/cmdline').read_bytes().split(b'\0');argv=[x.decode() for x in argv if x]
 environ=dict(x.split(b'=',1) for x in pathlib.Path('/proc/'+pid+'/environ').read_bytes().split(b'\0') if b'=' in x)
 safe_env={k:environ[k.encode()].decode() for k in ['GOMAXPROCS','GOMEMLIMIT','GOGC','GTRON_HISTORY_ROLLOUT_READERS_ONLY','GTRON_HISTORY_COMPRESSION_FORMAT','GTRON_PEBBLE_BOUNDED_POINT_READ'] if k.encode() in environ}
 allprops=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','gtron']).decode().splitlines() if '=' in x)
 installed=pathlib.Path('/data/gtron/start.sh').read_bytes();repo_start=pathlib.Path('/data/gtron/go-tron/scripts/start.sh').read_bytes();assert installed==repo_start,'review changed installed start.sh before publication'
 snap=pathlib.Path('/data/gtron/main/datadir/gtron/state-snapshots');manifest=json.loads((snap/'manifest.json').read_text());refs=manifest['segments']
 routes=[dict(r,sources=int.from_bytes((snap/r['path']).open('rb').read(16)[12:16],'big')) for r in refs if r['kind'] in ['system-history-routing','small-system-history-routing']]
 roots={}
 for name in ['.','chaindata','history','state-snapshots','ancient']:
  path=pathlib.Path('/data/gtron/main/datadir/gtron')/name
  if path.exists():st=path.stat();roots[name]={'realpath':str(path.resolve()),'dev':st.st_dev,'ino':st.st_ino}
 ports=subprocess.check_output(['ss','-lntup']).decode();(R/'listeners-before.txt').write_text(ports)
 pre={'utc':datetime.datetime.utcnow().isoformat()+'Z','identity':identity(),'binary_sha256':hashlib.sha256(pathlib.Path('/proc/'+pid+'/exe').read_bytes()).hexdigest(),'argv':argv,'runtime_environment':safe_env,'limits':{k:v for k,v in allprops.items() if k in ['MemoryLimit','MemorySoftLimit','CPUQuotaPerSecUSec','TasksMax']},'deploy':props('gtron-deploy','ActiveState','Result'),'timer':props('gtron-deploy.timer','ActiveState','SubState'),'start_script_sha256':hashlib.sha256(installed).hexdigest(),'start_matches_repo':True,'roots':roots,'wallet':get_json('http://127.0.0.1:8090/wallet/getnowblock'),'routes':routes,'cold_history_sources':sum(r['kind']=='history' and r.get('dataset')=='state-domain-change' for r in refs),'metadata_routes':sum(r['kind']=='metadata-history-routing' for r in refs)}
 (R/'preflight.json').write_text(json.dumps(pre,indent=2));s['stage']='native merged source checks';save();src=archive_source('source',SOURCE)
 command(['/data/go/bin/go','test','-tags=sapling','./core/rawdb','./core/state','./core/state/snapshots','./cmd/gtron','-run','Test(HistoryCandidate|HistoryRoutingReadScope|HistoryRequest|HistoryExecutionPrefetch|IndexedKV|StateChangePostingScratch|HistoryRoutingBootstrap|HistoryTxMetadata|V11)','-count=1','-timeout=180s'],src,R/'native-focused.log',240)
 command(['/data/go/bin/go','build','-tags=sapling','-o',str(R/'merged-gtron'),'./cmd/gtron'],src,R/'native-gtron-build.log',300)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'merged-http.test')],src,R/'native-http-build.log',300)
 s['stage']='merged fixed-cut full HTTP smoke';save();http_case('merged-fixed-http',R/'merged-http.test',A/'http-performance-priority/candidate-input')
 # Repeat the exact corrected auxiliary condition, without new index writes.
 http_case('merged-indexed-http',R/'merged-http.test',pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01/final/indexed-input'))
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='merged native build and full HTTP passed',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
