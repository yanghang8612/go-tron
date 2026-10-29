"""Independent local verification of sealed fixed-cut HTTP/cold-page evidence."""
import pathlib,sys,json,hashlib,zipfile,collections
archive=pathlib.Path(sys.argv[1]);expected_size=int(sys.argv[2]);expected_sha=sys.argv[3];out=pathlib.Path(sys.argv[4]);out.mkdir(parents=True,exist_ok=True)
raw=archive.read_bytes();assert len(raw)==expected_size and hashlib.sha256(raw).hexdigest()==expected_sha
with zipfile.ZipFile(archive) as z:
 names=set(z.namelist());assert len(names)==len(z.namelist())
 for name in names:
  p=pathlib.PurePosixPath(name);assert not p.is_absolute() and '..' not in p.parts and p.suffix in ['.json','.jsonl','.log'];target=out/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(z.read(name))
def read(name):return json.loads((out/name).read_text())
status=read('whole-query-status.json');assert status['complete'] and status['accepted']==16 and status['all_returns_match'] and status['production_mutations'] is False
build=read('candidate-status.json');assert build['complete'] and build['production_mutations'] is False
account=read('account-result.json');assert account['complete'] and account['after']==account['total'] and account['total']-account['before']==1497 and account['canonical_refs_unchanged'] and account['frontiers_unchanged']
tail=read('tail-result.json');assert tail['complete'] and tail['commitment_head_unchanged'] and tail['frontier_unchanged'] and tail['private_memtables_flushed']
assert tail['head']==45836827 and tail['head_tx']==4240407293 and tail['root']=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
proof={x['path']:x for x in read('whole-file-private-proof.json')};assert proof and set(p.split('/')[0] for p in proof)=={'chaindata','history','ancient','state-snapshots'}
for p in proof.values():
 if p['method']=='full independent whole-file copy':assert (p['source_dev'],p['source_ino'])!=(p['private_dev'],p['private_ino']) and len(p['copy_sha256'])==64
reqs=read('requests.json');by_case=collections.defaultdict(list);wants={9000000:207,10000000:213,15000000:238,20000000:249}
for q in reqs:
 assert q['height'] in wants and int(q['response']['result'],16)==wants[q['height']] and q['request']['method']=='eth_call'
 by_case[q['case']].append(q)
accepted=read('accepted.json');groups=collections.defaultdict(list);dispatch_resident=0
for row in accepted:
 case=row['case'];first,repeat=by_case[case];assert [first['repeat'],repeat['repeat']]==[0,1]
 assert first['height']==repeat['height']==row['height'];assert first['seconds_loopback_http']==row['first_seconds'] and repeat['seconds_loopback_http']==row['repeat_seconds']
 ready=read(case+'/ready.json');assert ready['head']==tail['head'] and ready['head_tx']==tail['head_tx'] and ready['root']==tail['root']
 pre=read(case+'/pre-start-residency.json');assert pre and sum(p['resident_pages'] for p in pre)==0;isolated={p['path'] for p in pre};assert isolated<=set(proof)
 actual=read(case+'/all-database-opens.json');assert actual['missing']==[] and not actual['overflow'] and set(actual['paths'])<=isolated
 dispatch=read(case+'/dispatch-residency.json');assert {p['path'] for p in dispatch}==isolated;dispatch_resident+=sum(p['resident_pages'] for p in dispatch)
 server=[json.loads(x) for x in (out/case/'server-requests.jsonl').read_text().splitlines() if x];assert len(server)==2
 groups[(row['version'],row['height'])].append(row)
assert set(groups)=={(v,h) for v in ['baseline','candidate'] for h in wants} and all(len(x)==2 for x in groups.values())
summary={'complete':True,'archive_bytes':expected_size,'archive_sha256':expected_sha,'head':tail['head'],'account_before':account['before'],'account_after':account['after'],'hot_tail':tail,'private_files':len(proof),'private_file_bytes':sum(x['bytes'] for x in proof.values()),'opened_file_cold_closure_verified':True,'dispatch_resident_pages_all_runs':dispatch_resident,'http_returns_all_match':True,'accepted_first_requests':16,'total_http_requests':len(reqs),'groups':[],'limitation':'One fixed contract/calldata at four heights, two fresh-process cold trials per version; mandatory engine startup warming recorded. Not a universal latency guarantee.'}
for (version,height),rows in sorted(groups.items()):summary['groups'].append({'version':version,'height':height,'first_seconds':[x['first_seconds'] for x in rows],'repeat_seconds':[x['repeat_seconds'] for x in rows]})
(out.parent/'verification.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
