from pathlib import Path
import os,json,subprocess,time,datetime,urllib.request,hashlib,re
p=Path('/data/gtron/cold-query-rollout-20261020-9030ce7a');out=p/'runtime-samples.jsonl';root=Path('/data/gtron/main/datadir/gtron');expected=json.loads((p/'identity.json').read_text())['binary_sha256']
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def request(url,payload=None):
 data=None if payload is None else json.dumps(payload).encode();r=urllib.request.Request(url,data=data,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(r,timeout=8) as f:return json.load(f)
def system():
 r=subprocess.run(['systemctl','show','gtron','-p','ActiveState','-p','MainPID'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 return dict(x.split('=',1) for x in r.stdout.decode().splitlines() if '=' in x)
previous=None;identity={};log_inode=None;log_offset=0;last_disk=0
initial_log=Path('/data/gtron/main/gtron.log')
if initial_log.exists():log_inode=initial_log.stat().st_ino;log_offset=initial_log.stat().st_size
for iteration in range(10080):
 start=time.monotonic();row={'utc':utc(),'iteration':iteration,'source':'9030ce7a31016491ece014ac76e5656ec149417e','errors':[]};active=system();row['service']=active
 try:
  pid=int(active['MainPID']);assert pid>0
  if identity.get('pid')!=pid:
   binary=Path('/proc/'+str(pid)+'/exe');identity={'pid':pid,'exe':os.path.realpath(str(binary)),'sha256':hashlib.sha256(binary.read_bytes()).hexdigest()}
  row['identity']=identity
  if identity['sha256']!=expected:row['errors'].append('binary identity changed');row['stop_monitor']=True
  row['wallet']=request('http://127.0.0.1:8090/wallet/getnodeinfo')
  status=Path('/proc/'+str(pid)+'/status').read_text();row['rss_kib']=int(re.search(r'^VmRSS:\s+(\d+)',status,re.M).group(1))
  metrics=request('http://127.0.0.1:6062/debug/metrics')['metrics'];selected={}
  exact={'sync/import/window/blocks_per_second','sync/import/window/transactions_per_second','sync/import/window/energy_per_second','sync/import/window/exec_busy_ratio','sync/import/window/buffer_wait_ratio','sync/import/window/updated_unix','state/prune/last/domain_change/pruned_through_block','state/prune/last/domain_change/pruned_through_tx'}
  for k,v in metrics.items():
   if k in exact or k.startswith('state/snapshot/cold/last/') or '/storage/keyspace/' in k or k.startswith('state/commitment/'):
    selected[k]=v.get('value',v.get('count'))
  row['metrics']=selected
  current={'pid':pid,'time':time.monotonic(),'head':row['wallet']['currentBlock'],'transactions':row['wallet'].get('syncInfo',{}).get('sessionTransactions')}
  if previous and previous['pid']==pid and current['head']>=previous['head']:
   dt=current['time']-previous['time'];row['window_seconds']=dt;row['observed_blocks_per_second']=(current['head']-previous['head'])/dt
   if current['transactions'] is not None and previous['transactions'] is not None:row['observed_transactions_per_second']=(current['transactions']-previous['transactions'])/dt
  previous=current
  if time.monotonic()-last_disk>300:
   disk={}
   for directory in sorted(root.iterdir()):
    if not directory.is_dir() or directory.is_symlink():continue
    d=subprocess.run(['du','-s','-B1',str(directory)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25)
    disk[directory.name]={'rc':d.returncode,'raw':d.stdout.decode().strip(),'error':d.stderr.decode()[-300:]}
   row['disk_live_sample']=disk;last_disk=time.monotonic()
   rpc={'jsonrpc':'2.0','id':1,'method':'eth_getBalance','params':['0x71b0af54e0a1182a5e0947d6a64f3b22740ef318','0x0']}
   begin=time.monotonic();historical=request('http://127.0.0.1:8545',rpc);row['genesis_balance_milliseconds']=(time.monotonic()-begin)*1000;row['genesis_balance_rpc']=historical
   row['genesis_balance_correct']=historical.get('result')==hex(99000000000000000*1000000000000)
   if not row['genesis_balance_correct']:row['errors'].append('genesis historical balance mismatch')
   probes=[]
   for height,expected_sun in [(341,0),(342,33251807524000000)]:
    q={'jsonrpc':'2.0','id':height,'method':'eth_getBalance','params':['0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1',hex(height)]}
    begin=time.monotonic();ans=request('http://127.0.0.1:8545',q);ok=ans.get('result')==hex(expected_sun*10**12);probes.append({'height':height,'response':ans,'correct':ok,'milliseconds':(time.monotonic()-begin)*1000})
    if not ok:row['errors'].append('first-transfer historical balance mismatch '+str(height))
   row['transfer_history_probes']=probes
   tx='0x5281eb99b3cab6b3cb4777e6b070df936f2cb068163fb8f3959a1cd88733a4c2';ans=request('http://127.0.0.1:8545',{'jsonrpc':'2.0','id':1,'method':'eth_getTransactionByHash','params':[tx]});row['first_transaction_lookup_correct']=bool(ans.get('result')) and ans['result'].get('hash')==tx and ans['result'].get('blockNumber')==hex(342)
   if not row['first_transaction_lookup_correct']:row['errors'].append('first transaction index lookup mismatch')
   counter_probes=[]
   for height,counter in [(3144020,6),(3144021,7)]:
    begin=time.monotonic();ans=request('http://127.0.0.1:8545',{'jsonrpc':'2.0','id':height,'method':'eth_getStorageAt','params':['0xa4997124a328a4615eab437cede31da47a31b375','0x0',hex(height)]});ok=ans.get('result')=='0x'+format(counter,'064x');counter_probes.append({'height':height,'slot':0,'expected_counter':counter,'response':ans,'correct':ok,'milliseconds':(time.monotonic()-begin)*1000})
    if not ok:row['errors'].append('cold TVM counter mismatch '+str(height))
   row['tvm_counter_history_probes']=counter_probes
   manifest=root/'state-snapshots/manifest.json'
   if manifest.is_file():
    m=json.loads(manifest.read_text());row['cold_manifest']={k:m.get(k) for k in ('version','commitmentScheme','elementVersion','generation','visibleTxStart','visibleTxEnd','progress')};row['cold_manifest']['active_segments']=len(m.get('segments',[]))
  v=os.statvfs('/data');row['filesystem_free_bytes']=v.f_bavail*v.f_frsize
 except BaseException as e:row['errors'].append(repr(e))
 try:
  f=Path('/data/gtron/main/gtron.log');inode=f.stat().st_ino
  if inode!=log_inode:log_inode=inode;log_offset=0
  with f.open('rb') as stream:
   stream.seek(log_offset);data=stream.read(2<<20);log_offset=stream.tell()
  lines=[x for x in data.decode(errors='replace').splitlines() if re.search(r'\b(ERROR|FATAL)\b|panic:|Sync paused|MuHash.*mismatch|corruption',x,re.I)]
  if lines:row['new_error_lines']=lines[-30:]
 except BaseException as e:row['log_error']=repr(e)
 with out.open('a') as stream:stream.write(json.dumps(row,separators=(',',':'))+chr(10))
 (p/'latest-observation.json').write_text(json.dumps(row,indent=2))
 if row.get('stop_monitor') or out.stat().st_size>128<<20:break
 time.sleep(max(0,60-(time.monotonic()-start)))
(p/'monitor-finished.json').write_text(json.dumps({'utc':utc(),'iterations':iteration+1}))
