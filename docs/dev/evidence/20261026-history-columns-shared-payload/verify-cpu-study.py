"""Verify complete profile evidence and regenerate native top tables locally."""
import gzip,hashlib,io,json,os,pathlib,re,statistics,subprocess,sys,tarfile,zipfile,tempfile

def digest(b):return hashlib.sha256(b).hexdigest()
def read_archive(p):
 with tarfile.open(p) as t:
  names=t.getnames();assert len(names)==len(set(names)) and all(pathlib.PurePosixPath(n).name==n for n in names)
  b={n:t.extractfile(n).read() for n in names}
 m=json.loads(b['archive-members.json']);assert set(b)==set(m)|{'archive-members.json'}
 assert all(digest(b[n])==h for n,h in m.items());return b
def top_rows(s):
 out={}
 for line in s.splitlines():
  m=re.match(r'^\s*((?:[0-9.]+ms)|0)\s+(\S+%)\s+(\S+%)\s+((?:[0-9.]+ms)|0)\s+(\S+%)\s+(.+)$',line)
  if m:out[m[6]]=list(m.groups()[:5])
 assert out;return out
def verify(archive,parent,local):
 assert pathlib.Path(archive).stat().st_size==500088 and digest(pathlib.Path(archive).read_bytes())=='903c1ae670c5ca4de0833fd755a2b5f5a052f15bf0717c01d87fce1940cf0ac0'
 local=pathlib.Path(local);local.mkdir(parents=True,exist_ok=True)
 assert digest(pathlib.Path(parent).read_bytes())=='cc18b1830e0016afc364c56e74e8aff2fd81f1e2408c3147461863ac0e08ed27'
 old=read_archive(parent);b=read_archive(archive);s=json.loads(b['profile-status.json'])
 assert s['complete'] and s['phase']=='complete' and not s['production_mutations']
 assert s['prior_archive_sha256']==digest(pathlib.Path(parent).read_bytes()) and s['go_version']=='go version go1.25.5 linux/amd64'
 assert s['production_before']==s['production_after']==json.loads(old['study-status.json'])['production_after']
 assert s['production_binary_sha256']==json.loads(old['study-status.json'])['production_binary_sha256']
 assert b['source-before.json']==b['source-after.json']==old['source-after.json']
 assert b['inputs-before.json']==b['inputs-after.json']==old['inputs-after.json']
 assert b['selected-inputs.json']==old['selected-inputs.json'] and b['reference-pages.jsonl']==old['reference-pages.jsonl']
 for n,h in s['helpers'].items():assert digest(b[n])==h
 for n in ['offline_shared_payload_test.go','offline_shared_scratch_test.go','offline_shared_decoder_test.go']:assert b[n]==old[n]
 expected=['native-build','cpu-study','v5-flat','v5-cum','v5-decode','scratch-flat','scratch-cum','scratch-decode']
 assert [x['label'] for x in s['checks']]==expected
 for x in s['checks']:assert x['returncode']==0 and digest(b[x['label']+'.log'])==x['log_sha256']
 assert b'PASS' in b['cpu-study.log'] and b'SKIP' not in b['cpu-study.log'] and b'FAIL' not in b['cpu-study.log']
 start=json.loads(b['start-receipt.json']);assert start['production_before']==start['production_after']
 u=start['observed_unit'];assert u['MemoryLimit']=='4294967296' and u['CPUQuotaPerSecUSec']=='2s' and u['Nice']=='15'
 dispatch=json.loads(b['dispatch.json']);assert not dispatch['production_mutations']
 assert len(b['profile-payload.zip'])==dispatch['payload_bytes'] and digest(b['profile-payload.zip'])==dispatch['payload_sha256']
 with zipfile.ZipFile(io.BytesIO(b['profile-payload.zip'])) as z:
  assert set(z.namelist())==set(dispatch['manifest']) and len(z.namelist())==len(dispatch['manifest'])
  for n,m in dispatch['manifest'].items():assert z.read(n)==b[n] and len(b[n])==m['bytes'] and digest(b[n])==m['sha256']
 assert json.loads(b['local-status.json'])['go_version']=='go version go1.25.5 darwin/arm64'
 assert b'PASS' in b['local-smoke.log'] and b'FAIL' not in b['local-smoke.log']
 rows=[json.loads(x) for x in b['cpu-windows.jsonl'].splitlines()]
 assert len(rows)==6 and [x['Arm'] for x in rows]==['v5','scratch','scratch','v5','v5','scratch']
 assert digest(b['cpu-windows.jsonl'])==s['windows_sha256']
 for row in rows:
  assert row['Pages']==1817 and isinstance(row['Passes'],int) and 0<row['Passes']<500 and 20<=row['WallSeconds']<40 and 0<row['CPUSeconds']<2.2*row['WallSeconds'] and 0<row['ScratchCap']<=131108
  assert row['Profile']==row['Arm']+'-r'+str(row['Round'])+'.cpu.pprof'
  assert len(gzip.decompress(b[row['Profile']]))>0
 for arm in ('v5','scratch'):assert [x['Round'] for x in rows if x['Arm']==arm]==[1,2,3]
 profiles={x['Profile'] for x in rows};assert {n for n in b if n.endswith('.pprof')}==profiles
 for n in profiles:(local/n).write_bytes(b[n])
 env=dict(os.environ,GOTOOLCHAIN='go1.25.5',TZ='UTC');version=subprocess.check_output(['go','version'],env=env,text=True).strip();assert version.startswith('go version go1.25.5 ')
 result={'scope':s['scope'],'pages':1817,'ordinary_pages':11,'production_identity':s['production_before'],'native_binary_sha256':s['binary_sha256'],'archive_sha256':digest(pathlib.Path(archive).read_bytes()),'local_go_version':version,'windows':rows,'arms':{}}
 for arm in ('v5','scratch'):
  part=[x for x in rows if x['Arm']==arm];paths=[str(local/x['Profile']) for x in part];tables={}
  for kind,flags in [('flat',['-top']),('cum',['-top','-cum'])]:
   args=['go','tool','pprof']+flags+['-nodecount=45','-unit=ms']+paths
   run=subprocess.run(args,env=env,text=True,capture_output=True,timeout=60);assert run.returncode==0,run.stderr
   (local/(arm+'-'+kind+'.log')).write_text(run.stdout);(local/(arm+'-'+kind+'.stderr')).write_text(run.stderr)
   tables[kind]=top_rows(run.stdout);assert tables[kind]==top_rows(b[arm+'-'+kind+'.log'].decode()),arm+' '+kind+' top differs'
  all_tables={}
  for kind,flags in [('flat',['-top']),('cum',['-top','-cum'])]:
   run=subprocess.run(['go','tool','pprof']+flags+['-nodefraction=0','-nodecount=0','-unit=ms']+paths,env=env,text=True,capture_output=True,timeout=60);assert run.returncode==0,run.stderr
   (local/(arm+'-all-'+kind+'.log')).write_text(run.stdout);all_tables[kind]=top_rows(run.stdout)
  selected={}
  targets={'zstd_decode':('github.com/klauspost/compress/zstd.(*Decoder).DecodeAll',3),'canonical_restore':('github.com/tronprotocol/go-tron/core/state/snapshots.decodeHistoryRecordColumns',3),'literal_decode':('github.com/klauspost/compress/zstd.(*blockDec).decodeLiterals',3),'huffman_table':('github.com/klauspost/compress/huff0.ReadTable',3),'huffman_decode':('github.com/klauspost/compress/huff0.(*Decoder).Decompress4X',3),'frame_reset':('github.com/klauspost/compress/zstd.(*frameDec).reset',3),'memmove':('runtime.memmove',0),'sequence_decode':('github.com/klauspost/compress/zstd.sequenceDecs_decodeSync_safe_bmi2',0)}
  passes=sum(x['Passes'] for x in part)
  for label,(function,column) in targets.items():
   values=all_tables['flat'].get(function);value=float(values[column].removesuffix('ms')) if values else 0
   selected[label]={'sampled_ms_per_pass':value/passes,'function_sampled':values is not None,'kind':'flat' if column==0 else 'cumulative'}
  result['arms'][arm]={'sampled_metrics':selected,'passes':sum(x['Passes'] for x in part),'cpu_seconds_per_pass_median':statistics.median(x['CPUSeconds']/x['Passes'] for x in part),'wall_seconds_per_pass_median':statistics.median(x['WallSeconds']/x['Passes'] for x in part),'cpu_seconds_per_pass_total':sum(x['CPUSeconds'] for x in part)/sum(x['Passes'] for x in part),'scratch_cap_max':max(x['ScratchCap'] for x in part),'tables':tables}
 paths=[str(local/x['Profile']) for x in rows if x['Arm']=='scratch']
 raw_run=subprocess.run(['go','tool','pprof','-raw']+paths,env=env,text=True,capture_output=True,timeout=60);assert raw_run.returncode==0
 raw=raw_run.stdout;(local/'scratch-raw.log').write_text(raw);locations={};current=None
 for line in raw.splitlines():
  match=re.match(r'^\s*(\d+): 0x',line)
  if match:current=int(match[1]);locations[current]=line
  elif current is not None:locations[current]+='\n'+line
 source=b['offline_shared_scratch_test.go'].decode().splitlines()
 lines={label:next(i for i,line in enumerate(source,1) if text in line) for label,text in [('keys','keys, err := dec.DecodeAll'),('common','transformed, err := dec.DecodeAll'),('canonical','out, err = decodeHistoryRecordColumns')]}
 ids={label:{n for n,text in locations.items() if 'decodeSharedStudyScratch' in text and re.search(':'+str(number)+r':\d+',text)} for label,number in lines.items()};assert all(len(v)==1 for v in ids.values())
 huffman={n for n,text in locations.items() if 'github.com/klauspost/compress/huff0.' in text};literals={n for n,text in locations.items() if '(*blockDec).decodeLiterals' in text};counts={k:{'cpu_ns':0,'huffman_ns':0,'literal_ns':0} for k in ids}
 for line in raw.splitlines():
  match=re.match(r'^\s*\d+\s+(\d+):\s+([0-9 ]+)\s*$',line)
  if not match:continue
  ns=int(match[1]);stack=set(map(int,match[2].split()))
  for label,points in ids.items():
   if stack&points:
    counts[label]['cpu_ns']+=ns
    if stack&huffman:counts[label]['huffman_ns']+=ns
    if stack&literals:counts[label]['literal_ns']+=ns
 result['scratch_callsites']={'source_lines':lines,'sampled_nanoseconds':counts,'sampled_ms_per_pass':{k:{n:v/1e6/result['arms']['scratch']['passes'] for n,v in row.items()} for k,row in counts.items()}}
 assert counts['keys']['literal_ns']>0 and counts['common']['huffman_ns']==0
 reference=[json.loads(line) for line in old['reference-pages.jsonl'].splitlines()]
 original=sum(x['OriginalBytes'] for x in reference);chosen=sum(x['PrototypeBytes'] for x in reference);keybytes=sum(x['Costs']['keys'][x['Costs']['selected']] for x in reference if x['Costs']['selected']>=0)
 result['key_size_tradeoff']={'original_page_bytes':original,'chosen_page_bytes':chosen,'selected_key_frame_bytes':keybytes,'total_additional_key_bytes_before_losing_savings':original-chosen,'additional_key_percent_before_losing_savings':100*(original-chosen)/keybytes,'uniform_mode_page_bytes':{str(mode):sum(min(x['Costs']['ordinary'],x['Costs']['candidates'][mode]) if x['AnnotationsRecovered'] else x['OriginalBytes'] for x in reference) for mode in range(3)}}
 a=result['arms'];result['scratch_vs_v5_cpu_percent']=(a['scratch']['cpu_seconds_per_pass_median']/a['v5']['cpu_seconds_per_pass_median']-1)*100;result['scratch_vs_v5_wall_percent']=(a['scratch']['wall_seconds_per_pass_median']/a['v5']['wall_seconds_per_pass_median']-1)*100
 return result
if __name__=='__main__':
 if len(sys.argv)==1:
  here=pathlib.Path(__file__).resolve().parent
  with tempfile.TemporaryDirectory(prefix='gtron-decoder-cpu-verify-') as tmp:result=verify(here/'cpu-study-core.tar.xz',here/'decoder-study-core.tar.xz',tmp)
 elif len(sys.argv)==4:result=verify(*sys.argv[1:])
 else:raise SystemExit('usage: verify-cpu-study.py [cpu-study-core.tar.xz decoder-study-core.tar.xz local-output-dir]')
 print(json.dumps(result,indent=2,sort_keys=True))
