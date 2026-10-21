#!/usr/bin/env python3
"""Recheck transferred bytes, code identity, HTTP medians and fixed-state receipts."""
import argparse, collections, gzip, hashlib, json, math, pathlib, re, statistics, subprocess, zipfile
ROOT=pathlib.Path(__file__).resolve().parent
EXPECTED_ROOT='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
EXPECTED_HASH='0000000000b454d09a46886a40d09d8cbec73812d013f57de1fc803784acfdf6'
HTTP_SHA='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf'
def raw(name):
 p=ROOT/name
 if p.exists():return p.read_bytes()
 p=pathlib.Path(str(p)+'.gz')
 return gzip.decompress(p.read_bytes())
def read(name):return json.loads(raw(name))
def rows(name):return [json.loads(x) for x in raw(name).splitlines() if x]
def sha(data):return hashlib.sha256(data).hexdigest()
def norm(s):
 if len(s)==128:s=bytes.fromhex(s).decode()
 return s.lower()
def close(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-9),(a,b)
def verify_sources(prefix):
 s=read(prefix+'/source-identity.json');commit=s['commit'];names=list(s['production_sources'])
 # One git batch avoids a subprocess per source. Contents come from Git, not
 # the mutable current checkout or downloaded diagnostic overlays.
 p=subprocess.run(['git','cat-file','--batch'],input=''.join(commit+':'+n+'\n' for n in names).encode(),stdout=subprocess.PIPE,check=True)
 data=p.stdout;off=0
 for name in names:
  end=data.index(b'\n',off);header=data[off:end].split();assert header[1]==b'blob',name
  size=int(header[2]);off=end+1;assert sha(data[off:off+size])==s['production_sources'][name],name;off+=size+1
 assert off==len(data)
 assert s['http_harness_sha256']==HTTP_SHA
 return {'commit':commit,'production_files':len(names),'binaries':s['binaries']}
def verify_comparison(prefix):
 d=prefix+'/tail-read-comparison/';status=read(d+'comparison-status.json');assert status['complete'] and status['requests']==154 and status['all_responses_correct']
 head=status['head'];assert head['head']==11818192 and head['head_tx']==601386177 and norm(head['head_hash'])==EXPECTED_HASH and norm(head['root'])==EXPECTED_ROOT
 rs=rows(d+'requests.jsonl');assert len(rs)==154 and len({r['id'] for r in rs})==154
 specs=read(d+'specs.json');assert [s['height'] for s in specs]==[3144020,3144021,5000000,9000000,11817936,11818191,11818192]
 expected={s['tag']:s['expected'] for s in specs}
 assert [int(s['expected'],16) for s in specs]==[6,7,63,207,225,225,225]
 groups=collections.defaultdict(list)
 for r in rs:
  assert r['correct'] and r['expected']==expected[r['tag']] and r['response'].get('result')==r['expected'] and 'error' not in r['response']
  if r['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):groups[r['mode'],r['cache'],r['tag']].append(r['milliseconds'])
 assert len(groups)==42
 summaries=read(d+'summary.json');assert len(summaries)==42
 for r in summaries:
  v=groups[r['mode'],r['cache'],r['tag']];assert r['n']==len(v)==(4 if r['cache']=='warm' else 2)
  close(r['median_ms'],statistics.median(v));close(r['min_ms'],min(v));close(r['max_ms'],max(v))
 manifests={mode:read(d+mode+'-private-files.json') for mode in ['baseline','candidate']}
 for m in manifests.values():
  assert m and len({r['path'] for r in m})==len(m)
  for r in m:assert r['source_inode']!=r['private_inode'] and r['bytes']>=0
 for repeat in range(2):
  for index in range(7):
   for mode in ['baseline','candidate']:
    p=read(d+f'os-cold-{repeat}-{index}-{mode}-residency.json');assert p['fraction']<0.01 and p['pages']>0;close(p['fraction'],p['resident_pages']/p['pages'])
 return summaries

def verify_aggregate(inventory):
 d='final/tail-read-comparison/';server=collections.defaultdict(list)
 for name in inventory:
  if name.startswith(d) and name.endswith('/server-requests.jsonl'):
   for r in rows(name):server[r['id']].append(r)
 groups=collections.defaultdict(list)
 def io(s):return {k:int(v) for k,v in (line.split(':') for line in s.splitlines())}
 def us(t):return t['Sec']*1000000+t['Usec']
 for r in rows(d+'requests.jsonl'):
  if r['cache'] not in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):continue
  matches=server[r['id']];assert len(matches)==1,(r['id'],len(matches))
  s=matches[0];before,after=io(s['io_before']),io(s['io_after'])
  values={'client_ms':r['milliseconds'],'handler_ms':s['elapsed_ns']/1e6,
   'cpu_ms':sum(us(s['usage_after'][k])-us(s['usage_before'][k]) for k in ('Utime','Stime'))/1000,
   'alloc_bytes':s['alloc_bytes'],'num_gc':s['num_gc'],
   **{k:after[k]-before[k] for k in ('syscr','rchar','read_bytes')}}
  groups[r['mode'],r['cache'],r['tag']].append(values)
 aggregate=read('final/aggregate.json');assert len(aggregate)==len(groups)==42
 for r in aggregate:
  values=groups[r['mode'],r['cache'],r['tag']];assert r['n']==len(values)
  for key in values[0]:close(r[key],statistics.median(v[key] for v in values))
 diagnostic=read('final/nine-diagnostic/response.json')
 assert diagnostic['correct'] and diagnostic['excluded_from_primary_performance'] and int(diagnostic['response']['result'],16)==207
 profile=read('final/nine-diagnostic/profile/profile-receipt.json')
 assert profile['all_results_correct'] and profile['excluded_from_primary_performance'] and profile['calls']>0
 return [r for r in aggregate if r['tag']=='0x895440']

def verify_trace():
 d='final/nine-diagnostic/';response=read(d+'response.json');breakdown=read(d+'read-breakdown.json')
 grouped=collections.defaultdict(lambda:{'calls':0,'returned_bytes':0,'syscall_elapsed_seconds':0.0})
 attributed,unparsed=0,0
 for line in raw(d+'read.trace').decode().splitlines():
  t=re.match(r'\s*\d+\s+(\d+\.\d+)\s+',line)
  if not t or not response['start_unix']<=float(t[1])<=response['end_unix']:continue
  m=re.search(r'\b(?:read|pread64)\(\d+<([^>]+)>.*=\s+(\d+)\s+<([0-9.]+)>$',line)
  if not m:unparsed+=1;continue
  path,size,elapsed=m.groups();marker='/candidate-cache-input/'
  if marker not in path:continue
  rel=path.split(marker,1)[1]
  family='cold-history' if rel.startswith('state-snapshots/history/') else ('hot-history-pebble' if rel.startswith('history/') else ('state-pebble' if rel.startswith('chaindata/') else 'other-input'))
  row=grouped[family,pathlib.PurePosixPath(rel).suffix];row['calls']+=1;row['returned_bytes']+=int(size);row['syscall_elapsed_seconds']+=float(elapsed);attributed+=1
 assert attributed==breakdown['attributed_calls'] and unparsed==breakdown['unparsed_window_lines']
 reported=breakdown['groups']['family+suffix'];assert len(reported)==len(grouped)
 for row in reported:
  got=grouped[row['family'],row['suffix']]
  for k,v in got.items():close(row[k],v)
 return {'attributed_calls':attributed,'unparsed_window_lines':unparsed}

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--archive',type=pathlib.Path);args=parser.parse_args()
 inv=read('evidence-inventory.json');assert inv
 for name,entry in inv.items():
  data=raw(name);assert len(data)==entry['bytes'] and sha(data)==entry['sha256'],name
 if args.archive:
  transfer=read('transfer.json');data=args.archive.read_bytes();assert len(data)==transfer['bytes'] and sha(data)==transfer['sha256']
  with zipfile.ZipFile(args.archive) as z:
   assert z.testzip() is None and json.loads(z.read('evidence-inventory.json'))==inv
   assert set(z.namelist())==set(inv)|{'evidence-inventory.json'}
   for name,entry in inv.items():assert sha(z.read(name))==entry['sha256'],name
 local=read('local-validation/manifest.json')
 assert local['source_commit']=='4d34e751f0d37add20451ca9aae2d68e0c930b6e'
 for name,entry in local['logs'].items():
  data=raw('local-validation/'+name);assert len(data)==entry['bytes'] and sha(data)==entry['sha256'],name
 assert read('local-validation/final-local-status.json')['complete']
 identities={p:verify_sources(p) for p in ['first-trial','final']}
 comparisons={p:verify_comparison(p) for p in ['first-trial','final']}
 aggregate=verify_aggregate(inv)
 trace=verify_trace()
 assert identities['first-trial']['commit'].startswith('d2e41fc5') and identities['final']['commit'].startswith('4d34e751')
 for p in ['first-trial','final']:
  status=read(p+'/tail-read-comparison/comparison-status.json');assert status['candidate_binary_sha256']==identities[p]['binaries']['candidate.test']
  assert status['baseline_binary_sha256']=='98473c753fd5327418048a1176986e773ece53447f2c9a624adee0438a38245e'
 build=read('final/storage-route-build.json');assert build['complete'] and build['all_pairs_covered'] and build['covered_pairs']==4609 and build['canonical_references_unchanged'] and build['progress_unchanged']
 proof=read('final/canonical-inodes.json');layout=read('final/layout-status.json');assert layout['complete'] and layout['canonical_files_unchanged']==len(proof)
 for r in proof:assert r['inode']>0 and r['bytes']>=0
 census=read('final/muhash-census.json');assert census['complete'] and census['muhash_verified'] and bytes(census['muhash_root']).hex()==EXPECTED_ROOT
 assert census['block_num']==11818192 and census['tx_num']==601386177 and bytes(census['block_hash']).hex()==EXPECTED_HASH and census['source_rows']==[3484117,481,141418493]
 prior=ROOT.parent/'20261021-cold-query-layout/account-gc/account-gc-muhash-census.json'
 if prior.exists():assert census['source_sha256']==json.loads(prior.read_text())['source_sha256']
 restored=read('final/history-restore.json');assert restored['complete'] and len(restored['windows'])==3
 assert sum(w['rows'] for w in restored['windows'])==22496 and sum(w['range_rows_verified'] for w in restored['windows'])==60
 for w in restored['windows']:assert w['candidate_row_sha256']==w['full_row_sha256']==w['restored_row_sha256'] and w['result']['ChangesRestored']==w['rows'] and w['result']['TxRangesRestored']==20
 for name in ['preparation-status.json','layout-status.json','audit-status.json','nine-diagnostic/status.json','resource-status.json']:assert read('final/'+name)['complete'],name
 live=read('final/final-production-identity.json');assert live['pid']==28737 and live['sha256']=='5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4' and live['old_database_exists']
 output={'complete':True,'inventory_files':len(inv),'source_identities':identities,'http_requests_per_comparison':154,'timed_requests_per_comparison':112,'final_9m':[r for r in comparisons['final'] if r['tag']=='0x895440'],'final_9m_aggregate':aggregate,'final_9m_trace':trace,'muhash_verified':True,'restored_rows':22496,'storage_route_bytes':build['active_route_bytes']}
 print(json.dumps(output,indent=2))
if __name__=='__main__':main()
