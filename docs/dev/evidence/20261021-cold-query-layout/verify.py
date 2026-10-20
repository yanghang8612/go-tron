#!/usr/bin/env python3
"""Recompute recorded acceptance evidence; never opens a production database."""
import argparse, collections, gzip, hashlib, json, pathlib, statistics, subprocess, zipfile
ROOT=pathlib.Path(__file__).resolve().parent
parser=argparse.ArgumentParser();parser.add_argument('--source',action='store_true',help='also compare frozen production sources to Git revisions');parser.add_argument('--archives',action='store_true',help='also check retained raw ZIPs from transfers.json');args=parser.parse_args()
def data(path):
 if path.exists():return path.read_bytes()
 return gzip.decompress(path.with_name(path.name+'.gz').read_bytes())
def read(path):return json.loads(data(path))
def sha(raw):return hashlib.sha256(raw).hexdigest()
def inventory(stage):
 path=ROOT/stage/('prefix-evidence-inventory.json' if stage=='prefix' else 'evidence-inventory.json');inv=read(path)
 if 'files' in inv:inv={r['path']:r for r in inv['files']}
 verified=omitted=0
 for name,row in inv.items():
  p=ROOT/stage/name
  if not p.exists() and not p.with_name(p.name+'.gz').exists():
   assert name.endswith(('.trace','.zlib')),name;omitted+=1;continue
  raw=data(p);assert len(raw)==row['bytes'] and sha(raw)==row['sha256'],p;verified+=1
 return {'files_verified':verified,'raw_trace_or_delivery_files_in_archive_only':omitted}
def compare(stage,dirname):
 root=ROOT/stage/dirname;rows=[json.loads(s) for s in data(root/'requests.jsonl').decode().splitlines()]
 assert len(rows)==154 and all(r['correct'] and r['response'].get('result')==r['expected'] and 'error' not in r['response'] for r in rows)
 counts=collections.Counter((r['mode'],r['cache'],r['tag']) for r in rows);group=collections.defaultdict(list)
 timed=('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process')
 for r in rows:
  if r['cache'] in timed:group[r['mode'],r['cache'],r['tag']].append(r['milliseconds'])
 summary=read(root/'summary.json');assert len(summary)==42
 for r in summary:
  values=group[r['mode'],r['cache'],r['tag']];assert len(values)==r['n']==(4 if r['cache']=='warm' else 2)
  assert statistics.median(values)==r['median_ms'] and min(values)==r['min_ms'] and max(values)==r['max_ms']
 proofs=list(root.glob('*-residency.json'));assert len(proofs)==28
 assert max(read(p)['fraction'] for p in proofs)<.01
 for mode in ['baseline','candidate']:
  rows_private=read(root/(mode+'-private-files.json'));assert all(row['source_inode']!=row['private_inode'] for row in rows_private)
  paths={row['path'] for row in rows_private};assert set(read(root/(mode+'-read-files.json')))<=paths
 status=read(root/'comparison-status.json');assert status['complete'] and status['all_responses_correct'];assert status['before_identity']==status['after_identity']
 # The handler host stores Hash.String() as %x; normalize both permitted forms.
 head=status['head'];assert head['head']==11818192 and head['head_tx']==601386177
 for name,expected in [('head_hash','0000000000b454d09a46886a40d09d8cbec73812d013f57de1fc803784acfdf6'),('root','8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5')]:
  value=head[name]
  if len(value)==128:value=bytes.fromhex(value).decode()
  assert value.removeprefix('0x')==expected,(stage,name,value)
 return {'requests':len(rows),'timed':sum(map(len,group.values())),'groups':len(summary),'private_cold_residency_checks':len(proofs)}
result={}
for stage in ['prefix','routing','account2','account-gc','lazy-final','lazy-nine']:
 if (ROOT/stage).exists():result[stage]=inventory(stage)
for stage,folder in [('prefix','prefix-comparison'),('routing','routing-comparison'),('account2','account2-comparison'),('lazy-final','lazy-final-comparison')]:
 if (ROOT/stage/folder).exists():result[stage]['http']=compare(stage,folder)
audit=ROOT/'account-gc';c=read(audit/'account-gc-muhash-census.json');assert c['complete'] and c['muhash_verified'];assert bytes(c['muhash_root']).hex()=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5';assert c['source_rows']==[3484117,481,141418493]
r=read(audit/'account-gc-reclamation.json');assert r['complete'] and r['manifest_bytes_unchanged'] and r['copy_originals_preserved'];assert r['files_deleted']==144 and r['canonical_paths_unchanged']==23691 and r['exclusive_link_count']==1;assert sum(x['FilesDeleted'] for x in r['passes'])==144;assert all(x['FilesDeleted']<=16 and x['Scanned']<=256 for x in r['passes']);assert r['logical_bytes_deleted']==3914238960 and r['exclusive_allocated_bytes_before_unlink']==3914711040
restore=read(audit/'account-gc-history-restore.json');assert restore['complete'];windows=restore['windows'];assert len(windows)==3 and sum(w['rows'] for w in windows)==22496
for w in windows:
 assert w['full_row_sha256']==w['candidate_row_sha256']==w['restored_row_sha256'];assert w['range_rows_verified']==w['result']['TxRangesRestored']==20 and w['rows']==w['result']['ChangesRestored']
result['canonical_correctness']={'muhash_verified':True,'restored_rows':22496,'restored_block_ranges':60,'retired_auxiliary_files':144,'exclusive_allocated_bytes':3914711040}
nine=ROOT/'lazy-nine'
if nine.exists():
 status=read(nine/'status.json');assert status['complete'] and status['excluded_from_primary_performance']
 assert status['candidate_sha256']==read(ROOT/'lazy-final/lazy-final-source-identity.json')['candidate_sha256']
 assert read(nine/'residency.json')['fraction']<.01
 response=read(nine/'response.json');assert response['correct'] and response['response']['result']=='0x'+format(207,'064x')
 breakdown=read(nine/'read-breakdown.json');assert breakdown['attributed_calls']==5647 and sum(r['calls'] for r in breakdown['groups']['family'])==5647
 groups={(r['family'],r['suffix']):r for r in breakdown['groups']['family+suffix']}
 assert groups['cold-history','.kv']['calls']==2874 and groups['cold-history','.seg']['calls']==1062 and groups['hot-history-pebble','.sst']['calls']==1666 and groups['cold-history','.route']['calls']==8
 result['lazy-nine']['file_read_calls_verified']=5647
if args.source:
 for stage,name,commit in [('prefix','source-final-identity.json','e4805af5'),('routing','routing-source-identity.json','0b57081f'),('account2','account2-source-identity.json','2e403c3d'),('account-gc','account-gc-source-identity.json','c7ef42cf'),('lazy-final','lazy-final-source-identity.json','0b370448')]:
  p=ROOT/stage/name
  if not p.exists():continue
  files=read(p)['production_sources']
  names=list(files);queries=''.join(commit+':'+name+'\n' for name in names).encode()
  process=subprocess.run(['git','cat-file','--batch'],cwd=ROOT,input=queries,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
  position=0
  for name in names:
   end=process.stdout.index(b'\n',position);header=process.stdout[position:end].split();assert len(header)==3 and header[1]==b'blob',(commit,name,header)
   size=int(header[2]);position=end+1;raw=process.stdout[position:position+size];assert len(raw)==size and sha(raw)==files[name],(commit,name)
   position+=size;assert process.stdout[position:position+1]==b'\n';position+=1
  assert position==len(process.stdout)
  result[stage]['git_source_files_verified']=len(files)
if args.archives:
 for x in read(ROOT/'transfers.json'):
  p=pathlib.Path(x['local_retained']);raw=p.read_bytes();assert len(raw)==x['bytes'] and sha(raw)==x['sha256'],p
  with zipfile.ZipFile(p) as archive:
   assert archive.testzip() is None
   inventory_name='prefix-evidence-inventory.json' if x['stage']=='prefix' else 'evidence-inventory.json'
   inv=json.loads(archive.read(inventory_name))
   if 'files' in inv:inv={row['path']:row for row in inv['files']}
   for name,row in inv.items():
    raw=archive.read(name);assert len(raw)==row['bytes'] and sha(raw)==row['sha256'],name
 result['retained_archives_verified']=True
print(json.dumps(result,indent=2,ensure_ascii=False))
