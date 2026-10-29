import pathlib,sys,json,hashlib,zipfile,collections,re
archive=pathlib.Path(sys.argv[1]);expected_bytes=int(sys.argv[2]);expected_sha=sys.argv[3];out=pathlib.Path(sys.argv[4]);out.mkdir(parents=True,exist_ok=True)
b=archive.read_bytes();assert len(b)==expected_bytes and hashlib.sha256(b).hexdigest()==expected_sha
with zipfile.ZipFile(archive) as z:
 assert len(set(z.namelist()))==len(z.namelist())
 for name in z.namelist():
  p=pathlib.PurePosixPath(name);assert not p.is_absolute() and '..' not in p.parts and p.suffix in ['.json','.jsonl','.log'];target=out/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(z.read(name))
def read(name):return json.loads((out/name).read_text())
status=read('whole-query-status.json');tail=status.get('ablation',False);expected_first=8 if tail else 16
assert status['complete'] and status['accepted']==expected_first and status['all_returns_match'] and status['production_mutations'] is False
wants={9000000:207,10000000:213,15000000:238,20000000:249};versions=['span-only','candidate'] if tail else ['baseline','candidate']
proof={p['path']:p for p in read('whole-file-private-proof.json')};assert proof
kinds={'chaindata','history','ancient','state-snapshots'};assert all(p.split('/')[0] in kinds for p in proof)
for item in proof.values():
 if item['method']=='full independent whole-file copy':assert (item['source_dev'],item['source_ino'])!=(item['private_dev'],item['private_ino']) and len(item['copy_sha256'])==64
startup_only=set();query_union=set();prefix='/data/gtron/maintenance/whole-query-cold-20261029-01/input/'
for version in versions:
 base='traced-discovery-'+version;p=read(base+'/read-proof.json');assert p['db_file_mmaps']==[] and not p['inotify_overflow']
 read_files=set();mapped=set()
 for line in (out/base/'syscalls.log').read_text().splitlines():
  for path in re.findall(r'<([^>]+)>',line):
   if not path.startswith(prefix):continue
   rel=path[len(prefix):]
   if rel.split('/')[0] not in kinds:continue
   if 'mmap(' in line:mapped.add(rel)
   elif re.search(r'\b(?:read|pread64|readv|preadv|preadv2)\(',line):read_files.add(rel)
 assert not mapped and read_files==set(p['all_traced_read_files']) and set(p['startup_reads'])|set(p['http_reads'])<=read_files
 query_union.update(p['http_reads']);startup_only.update(set(p['startup_reads'])-set(p['http_reads']))
assert query_union<=set(proof)
requests=read('requests.json');by_case=collections.defaultdict(list)
for q in requests:
 assert q['request']['method']=='eth_call' and q['height'] in wants and q['response']['result']=='0x'+format(wants[q['height']],'064x') and q['response']['id']==q['request']['id'] and q['response']['jsonrpc']=='2.0';by_case[q['case']].append(q)
accepted=read('accepted.json');assert len(accepted)==expected_first;groups=collections.defaultdict(list)
for row in accepted:
 case=row['case'];a,b=by_case[case];assert [a['repeat'],b['repeat']]==[0,1] and a['seconds_loopback_http']==row['first_seconds'] and b['seconds_loopback_http']==row['repeat_seconds']
 for name in ['pre-start-residency.json','dispatch-residency.json']:
  pages=read(case+'/'+name);assert sum(x['resident_pages'] for x in pages)==0
 isolated={x['path'] for x in pages};assert isolated<=set(proof)
 actual=read(case+'/actual-read-proof.json');assert not actual['missing'] and not actual['overflow'] and actual['post_run_inodes_stable'] and set(actual['paths'])<=isolated
 ready=read(case+'/ready.json');assert ready['head']==45836827 and ready['head_tx']==4240407293 and ready['root']=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
 server=[json.loads(x) for x in (out/case/'server-requests.jsonl').read_text().splitlines() if x];assert len(server)==2
 groups[row['version'],row['height']].append(row)
assert set(groups)=={(v,h) for v in versions for h in wants} and all(len(x)==(1 if tail else 2) for x in groups.values())
if tail:
 a=read('tail-ablation-result.json');assert a['complete'] and a['canonical_head_unchanged'] and a['tail_blocks']==status['tail_blocks'] and a['original_indexed']-a['indexed']==status['tail_blocks']
 retained=a['retained_packs'];assert [x['block'] for x in retained]==list(range(a['indexed']+1,a['head']+1)) and all(x['pack_present'] and x['encoded_bytes']>0 and len(x['encoded_sha256'])==64 for x in retained)
 control=read('control-source-patch.json');assert control['V3_owner_spans_retained'] and 'iterateFirstStateHistoryTail' in control['inserted_branch']
summary={'complete':True,'archive_bytes':expected_bytes,'archive_sha256':expected_sha,'ablation':tail,'source':status['source'],'binary_sha256':status['binary_sha256'],'accepted_first_requests':expected_first,'total_http_requests':len(requests),'all_returns_match':True,'all_dispatch_file_pages_zero':True,'private_file_bytes':sum(x['bytes'] for x in proof.values()),'files_by_database':dict(collections.Counter(p.split('/')[0] for p in proof)),'constructor_only_file_count':len(startup_only-query_union),'scope':'full HTTP actual read paths over four databases; new process; file pages zero at dispatch; normal startup/application caches retained; no cold-device-cache claim','groups':[]}
if tail:summary['tail_blocks']=status['tail_blocks']
if (out/'traced-discovery-candidate/query-filter-counts.json').exists():summary['diagnostic_filter_counts']=read('traced-discovery-candidate/query-filter-counts.json')
for (version,height),rows in sorted(groups.items()):summary['groups'].append({'version':version,'height':height,'first_seconds':[x['first_seconds'] for x in rows],'repeat_seconds':[x['repeat_seconds'] for x in rows]})
(out.parent/(out.name+'-verification.json')).write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
