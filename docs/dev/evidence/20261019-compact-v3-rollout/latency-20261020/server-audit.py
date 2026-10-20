from pathlib import Path
import os,json,datetime,hashlib,re,subprocess,time,urllib.request,collections,base64,zlib
root=Path('/data/gtron/compact-v3-rollout-20261019-01');out=root/'latency-followup-20261020'
out.mkdir(exist_ok=True)
def parse_utc(s):return datetime.datetime.strptime(s[:-3]+s[-2:],'%Y-%m-%dT%H:%M:%S.%f%z')
now=datetime.datetime.now(datetime.timezone.utc);since=datetime.datetime(2026,10,19,10,tzinfo=datetime.timezone.utc)
r={'utc':now.isoformat(),'requested_since_utc':since.isoformat(),'requested_since_beijing':'2026-10-19T18:00:00+08:00'}
pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);exe=Path('/proc/'+str(pid)+'/exe')
r['identity']={'pid':pid,'exe':os.path.realpath(str(exe)),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest()}
r['timezone_env']=[x.decode() for x in Path('/proc/'+str(pid)+'/environ').read_bytes().split(b'\0') if x.startswith(b'TZ=')]
r['source_head']=subprocess.check_output(['git','rev-parse','HEAD'],cwd='/data/gtron/go-tron').decode().strip()
r['source_hashes']={n:hashlib.sha256((Path('/data/gtron/go-tron')/n).read_bytes()).hexdigest() for n in ['core/state/history.go','core/rawdb/accessors_state_changeset.go','core/state/snapshots/history_segment.go','core/state/snapshots/history_binary.go','core/state/snapshots/compressed_block.go']}
raw=(root/'runtime-samples.jsonl').read_bytes();(out/'runtime-prefix.jsonl').write_bytes(raw)
rows=[json.loads(x) for x in raw.splitlines() if x.strip()];window=[x for x in rows if since<=parse_utc(x['utc'])<=now]
periodic=[x for x in window if 'disk_live_sample' in x]
errors=[{'utc':x['utc'],'errors':x.get('errors',[]),'new_error_lines':x.get('new_error_lines',[]),'genesis_correct':x.get('genesis_balance_correct'),'tx_correct':x.get('first_transaction_lookup_correct'),'tvm_complete':len(x.get('tvm_counter_history_probes',[]))==2} for x in window if x.get('errors') or x.get('new_error_lines')]
small=[]
for x in window:
 y={k:x[k] for k in ('utc','errors','new_error_lines','genesis_balance_correct','first_transaction_lookup_correct','transfer_history_probes','tvm_counter_history_probes') if k in x}
 y['head']=x.get('wallet',{}).get('currentBlock');y['identity']=x.get('identity');small.append(y)
(out/'window-observations.jsonl').write_text(''.join(json.dumps(x,separators=(',',':'))+'\n' for x in small))
r['observer']={'source_bytes':len(raw),'source_sha256':hashlib.sha256(raw).hexdigest(),'snapshot_path':str(out/'runtime-prefix.jsonl'),'all_samples':len(rows),'window_samples':len(window),'first':window[0]['utc'],'last':window[-1]['utc'],'first_head':window[0]['wallet']['currentBlock'],'last_head':window[-1]['wallet']['currentBlock'],'max_gap_seconds':max((parse_utc(b['utc'])-parse_utc(a['utc'])).total_seconds() for a,b in zip(window,window[1:])),'pid_counts':dict(collections.Counter(str(x.get('identity',{}).get('pid')) for x in window)),'periodic_batches':len(periodic),'complete_tvm_batches':sum(len(x.get('tvm_counter_history_probes',[]))==2 for x in periodic),'errors':errors,'commitment_metric_names':sorted(set(k for x in window for k in x.get('metrics',{}) if 'commitment' in k.lower() or 'muhash' in k.lower()))}
logpat=re.compile(r'\[(\d\d-\d\d)\|(\d\d:\d\d:\d\d\.\d{3})\]');r['logs']=[];allbad=[]
for f in sorted(Path('/data/gtron/main').glob('gtron.log*')):
 if not f.is_file() or f.suffix=='.gz':continue
 st=f.stat();data=f.open('rb').read(st.st_size);(out/(f.name+'.prefix')).write_bytes(data)
 lines=data.decode(errors='replace').splitlines();timed=[];bad=[];mu=[];unknown=0
 for line in lines:
  m=logpat.search(line)
  if not m:unknown+=1;continue
  dt=datetime.datetime.strptime('2026-'+m.group(1)+' '+m.group(2),'%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
  if since<=dt.astimezone(datetime.timezone.utc)<=now:
   timed.append(line)
   if re.search(r'muhash|commitment',line,re.I):mu.append(line)
   if re.search(r'^\s*(ERROR|CRIT|FATAL)|panic:|MuHash.*mismatch|commitment.*(?:mismatch|fail|invalid|corrupt)|Sync paused',line,re.I):bad.append(line)
 r['logs'].append({'path':str(f),'inode':st.st_ino,'captured_bytes':len(data),'prefix_sha256':hashlib.sha256(data).hexdigest(),'first_line':lines[0] if lines else None,'last_line':lines[-1] if lines else None,'window_lines':len(timed),'window_first':timed[0] if timed else None,'window_last':timed[-1] if timed else None,'unparsed_lines_all':unknown,'muhash_or_commitment_lines':len(mu),'muhash_or_commitment_examples':mu[:2]+mu[-2:],'error_lines':bad[:100],'error_count':len(bad)})
 allbad+=bad
r['journal']={}
try:
 j=subprocess.check_output(['journalctl','-u','gtron','--since','2026-10-19 10:00:00 UTC','--until',now.strftime('%Y-%m-%d %H:%M:%S UTC'),'--no-pager','-o','short-iso'],stderr=subprocess.STDOUT,timeout=20)
 (out/'journal.txt').write_bytes(j);jl=j.decode(errors='replace').splitlines();matches=[x for x in jl if re.search(r'error|panic|muhash|commitment|corrupt|Sync paused',x,re.I)]
 r['journal']={'bytes':len(j),'sha256':hashlib.sha256(j).hexdigest(),'lines':len(jl),'first':jl[:2],'last':jl[-2:],'matches':matches[:100],'match_count':len(matches),'muhash_commitment_matches':[x for x in matches if re.search(r'muhash|commitment',x,re.I)]}
except BaseException as e:r['journal']={'error':repr(e)}
manifest=Path('/data/gtron/main/datadir/gtron/state-snapshots/manifest.json').read_bytes();m=json.loads(manifest);h=[x for x in m['segments'] if x.get('dataset')=='state-domain-change' and x.get('kind')=='history'];over=[x for x in h if x['toTxNum']>20587119]
r['manifest']={'sha256':hashlib.sha256(manifest).hexdigest(),'generation':m['generation'],'segments':len(m['segments']),'history_segments':len(h),'overlap_after_target_tx':len(over),'target_tx':20587119,'visibleTxEnd':m['visibleTxEnd'],'progress':m['progress'],'first_overlap':over[0],'last_overlap':over[-1]}
(out/'manifest.json').write_bytes(manifest)
r['resources']={'loadavg':Path('/proc/loadavg').read_text().strip(),'cpus':os.cpu_count(),'memory':subprocess.check_output(['free','-b']).decode(),'top_rss':subprocess.check_output(['ps','-eo','pid,ppid,comm,pcpu,pmem,rss,etime','--sort=-rss']).decode().splitlines()[:12]}
def request(method,params):
 req=urllib.request.Request('http://127.0.0.1:8545',data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=15) as f:return json.load(f)
r['probes']=[]
for rep in range(2):
 for name,method,params,expected in [('genesis','eth_getBalance',['0x71b0af54e0a1182a5e0947d6a64f3b22740ef318','0x0'],hex(99000000000000000*10**12)),('latest','eth_getStorageAt',['0xa4997124a328a4615eab437cede31da47a31b375','0x0','latest'],None),('cold6','eth_getStorageAt',['0xa4997124a328a4615eab437cede31da47a31b375','0x0',hex(3144020)],'0x'+format(6,'064x')),('cold7','eth_getStorageAt',['0xa4997124a328a4615eab437cede31da47a31b375','0x0',hex(3144021)],'0x'+format(7,'064x'))]:
  q={'round':rep+1,'name':name,'method':method,'params':params};t=time.monotonic()
  try:q['response']=request(method,params);q['correct']=None if expected is None else q['response'].get('result')==expected
  except BaseException as e:q['error']=repr(e)
  q['milliseconds']=(time.monotonic()-t)*1000;r['probes'].append(q)
  print('PROBE',rep+1,name,round(q['milliseconds'],3),q.get('correct'),q.get('error',''),flush=True)
r['end_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(out/'audit.json').write_text(json.dumps(r,indent=2)+'\n')
packet={n:(out/n).read_text() for n in ('inventory.json','audit.json','window-observations.jsonl')};b=json.dumps(packet,separators=(',',':')).encode();(out/'packet.json').write_bytes(b)
print('\x1b[2J\x1b[H',end='');print('GTRAUD:'+base64.b64encode(zlib.compress(b,9)).decode());print('PACKET_SHA256',hashlib.sha256(b).hexdigest(),'BYTES',len(b),flush=True)
