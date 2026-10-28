"""Verify small receipts only; never reopen a database or repeat a state scan."""
import pathlib,json,hashlib,statistics,collections,sys
base=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else 'docs/dev/evidence/20261028-tail-read-ahead');r=base/'native'
seal=json.loads((r/'member-seal.json').read_text())
for name,m in seal['files'].items():
 p=r/pathlib.Path(name).relative_to('docs/dev/evidence/20261028-tail-read-ahead/native');b=p.read_bytes();assert len(b)==m['bytes'] and hashlib.sha256(b).hexdigest()==m['sha256'],name
statuses=[json.loads((x/'candidate-status.json').read_text()) for x in [r,r/'cold',r/'random']]
assert all(x['complete'] and x['all_returns_match'] and x['production_before']==x['production_after'] for x in statuses)
assert len({json.dumps(x['production_after'],sort_keys=True) for x in statuses})==1
assert statuses[-1]['production_after']==json.loads((r/'final-identity.json').read_text())
heads=set();rows=[];expected={}
def usage(x):return sum(x[k]['Sec']+x[k]['Usec']/1e6 for k in ['Utime','Stime'])
def io(x):return {k:int(v) for k,v in (line.split(':') for line in x.splitlines())}
for phase,root,filename in [('warm',r,'candidate-requests.json'),('cold',r/'cold','requests.json'),('random',r/'random','requests.json')]:
 requests=json.loads((root/filename).read_text())
 for q in requests:
  case=q['case'];host=root/case;ready=json.loads((host/'ready.json').read_text());assert ready['read_only'];heads.add(tuple(ready[k] for k in ['head','head_hash','head_tx','root']))
  matches=[json.loads(line) for line in (host/'server-requests.jsonl').read_text().splitlines()];matches=[s for s in matches if s['id']==q['response']['id']];assert len(matches)==1,(phase,case,q)
  s=matches[0];height=int(s['params'][1],16);assert height==q.get('height',9000000);assert s['method']=='eth_call';assert s['params'][0]['to']=='0xa4997124a328a4615eab437cede31da47a31b375' and s['params'][0]['data']=='0x9fa6a6e3'
  result=q['response']['result'];assert result==expected.setdefault(height,result)
  if height==9000000:assert int(result,16)==207
  a,b=io(s['io_before']),io(s['io_after']);row=dict(q,phase=phase,height=height,cpu_seconds=usage(s['usage_after'])-usage(s['usage_before']),alloc_bytes=s['alloc_bytes'],num_gc=s['num_gc'],read_bytes=b['read_bytes']-a['read_bytes'],rchar=b['rchar']-a['rchar'],read_syscalls=b['syscr']-a['syscr'],server_elapsed_ns=s['elapsed_ns'])
  rows.append(row)
assert len(heads)==1
proof=json.loads((r/'cold/private-proof.json').read_text());paths={x['path'] for x in proof};assert len(paths)==len(proof)
assert all((p['source_dev'],p['source_ino'])!=(p['private_dev'],p['private_ino']) for p in proof)
controlled=[]
for phase,root in [('cold',r/'cold'),('random',r/'random')]:
 for folder in root.iterdir():
  p=folder/'dispatch-residency.json'
  if not p.is_file():continue
  resident=json.loads(p.read_text());assert {x['path'] for x in resident}==paths;assert all(x['resident_pages']==0 for x in resident)
  coverage=json.loads((folder/'actual-read-coverage.json').read_text());assert not coverage['missing'] and not coverage['overflow'];assert set(coverage['paths'])<=paths
  controlled.append(str(folder.relative_to(r)))
assert len(controlled)==8,len(controlled)
formal=[x for x in rows if x['cache']!='discovery-excluded'];assert len(formal)==65,len(formal)
summary={'head_identity':list(heads)[0],'sealed_members_verified':len(seal['files']),'formal_requests':len(formal),'discovery_requests':len(rows)-len(formal),'all_returns_match':True,'controlled_cold_cases':controlled,'private_files':len(proof),'private_bytes':sum(x['bytes'] for x in proof),'production_unchanged':True,'rows':rows}
groups=collections.defaultdict(list)
for x in formal:
 variant=('random' if 'random' in x['case'] else 'lookahead' if 'candidate' in x['case'] or 'lookahead' in x['case'] else 'baseline')
 tier='controlled-cold' if x['cache'].startswith('private-cold') else 'second-call' if x['cache'].startswith('second-call') else 'new-process' if x.get('repeat')==0 else 'later-call'
 groups[(x['phase'],variant,tier,x['height'])].append(x)
summary['groups']=[]
for key,rs in sorted(groups.items()):
 g=dict(zip(['phase','variant','cache','height'],key));g.update(n=len(rs),wall_values=[x['seconds_loopback_http'] for x in rs])
 for field in ['seconds_loopback_http','cpu_seconds','alloc_bytes','read_bytes','rchar','read_syscalls']:g['median_'+field]=statistics.median(x[field] for x in rs)
 summary['groups'].append(g)
(base/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for x in summary['groups']:print(json.dumps(x))
print('VERIFIED',len(formal),'formal requests;',len(controlled),'controlled cold cases;',len(seal['files']),'sealed files')
