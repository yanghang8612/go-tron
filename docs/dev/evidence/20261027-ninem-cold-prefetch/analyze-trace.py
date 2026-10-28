import re,pathlib,json,collections,sys,gzip,struct,hashlib
r=pathlib.Path(sys.argv[1]);pending={};allstats=collections.defaultdict(lambda:collections.Counter());qstats=collections.defaultdict(lambda:collections.Counter());opened=collections.defaultdict(set);qopened=collections.defaultdict(set);iomarkers=[];inquery=False;bad=0;durations=collections.defaultdict(list)
for line in (r/'strace.log').read_text().splitlines():
 m=re.match(r'^(\d+) (\d+\.\d+) (.*)$',line)
 if not m:continue
 pid,stamp,body=m.groups()
 if body.endswith('<unfinished ...>'):pending[pid]=(stamp,body[:-len('<unfinished ...>')]);continue
 if body.startswith('<... '):
  tail=re.match(r'<\.\.\. \w+ resumed>(.*)',body)
  if not tail:continue
  if pid not in pending:bad+=1;continue
  stamp,first=pending.pop(pid);body=first+tail.group(1)
 match=re.search(r'\)\s+=\s+(-?\d+)',body);dur=re.search(r'<([0-9.]+)>$',body)
 if not match or not dur:continue
 rc=int(match.group(1));secs=float(dur.group(1));op=body.split('(',1)[0]
 f=re.match(r'(?:read|pread64)\(\d+<([^>]+)>,',body)
 if f:
  name=f.group(1)
  if re.match(r'/proc/\d+/io$',name) and rc>0:
   iomarkers.append(float(stamp));inquery=len(iomarkers)==1
  if '/private-candidate/' not in name:continue
  rel=name.split('/private-candidate/',1)[1];kind=pathlib.Path(rel).suffix or rel
  allstats[kind]['read_calls']+=1;allstats[kind]['logical_bytes']+=max(rc,0);allstats[kind]['traced_syscall_seconds']+=secs
  if inquery:qstats[kind]['read_calls']+=1;qstats[kind]['logical_bytes']+=max(rc,0);qstats[kind]['traced_syscall_seconds']+=secs;durations[kind].append(secs)
 if op=='openat' and rc>=0:
  f=re.match(r'openat\([^,]+, "([^"]+)"',body)
  if not f or '/private-candidate/' not in f.group(1):continue
  name=f.group(1);rel=name.split('/private-candidate/',1)[1];kind=pathlib.Path(rel).suffix or rel;opened[kind].add(rel);allstats[kind]['open_calls']+=1
  if inquery:qopened[kind].add(rel);qstats[kind]['open_calls']+=1
assert len(iomarkers)==2 and not pending and bad==0,(iomarkers,pending,bad)
for k,files in opened.items():allstats[k]['unique_opened_files']=len(files)
for k,files in qopened.items():qstats[k]['unique_opened_files']=len(files)
result={'complete':True,'io_window_markers':iomarkers,'io_window_seconds':iomarkers[1]-iomarkers[0],'query_window':dict(qstats),'whole_process':dict(allstats),'query_unique_opened_paths':{k:sorted(v) for k,v in qopened.items()},'limits':'Single request with strace and CPU profiling; syscall durations include tracing effects, are not untraced latency or disjoint wall-clock phases.'}
if len(sys.argv)>2:
 catalog=pathlib.Path(sys.argv[2]);manifest=json.loads(gzip.decompress((catalog/'source-proof/before-manifest.json.gz').read_bytes()));refs={v['path']:v for v in manifest['segments']};old=(catalog/'source-proof/legacy-source-prefix.bin').read_bytes();n=struct.unpack_from('>I',old,12)[0];ids={old[40+32*i:72+32*i] for i in range(n)};expected={'.kv':set(),'.seg':set()}
 def pair(h,a):
  s=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
  for v in [h,a]:
   s.update(struct.pack('>QQQQQH',v['fromTxNum'],v['toTxNum'],v.get('aggregationSteps',0) or 1,v['size'],v.get('rows',0),v.get('domain',0)))
   for k in ['dataset','kind','path','checksum']:
    b=v.get(k,'').encode();s.update(struct.pack('>Q',len(b))+b)
  return s.digest()
 for h in manifest['segments']:
  if h.get('dataset')=='state-domain-change' and h['kind']=='history':
   a=refs[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))]
   if pair(h,a) not in ids:expected['.kv'].add(a['path']);expected['.seg'].add(h['path'])
 assert all(set(qopened[k])==v for k,v in expected.items())
 result.update(query_opened_exactly_uncovered_sources=True,uncovered_sources=len(expected['.kv']))
(r/'trace-analysis.json').write_text(json.dumps(result,indent=2));print(json.dumps({k:v for k,v in result.items() if k!='query_unique_opened_paths'},indent=2))
