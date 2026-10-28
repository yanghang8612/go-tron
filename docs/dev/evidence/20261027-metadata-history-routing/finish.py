import pathlib,subprocess,os,shutil,json,traceback,datetime,hashlib,base64,zlib,time,gzip,re,zipfile,pwd
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');M=A/'metadata-v3';Q=A/'metadata-cold-v3';F=A/'metadata-final';F.mkdir(mode=0o700,exist_ok=True)
s={'complete':False,'stage':'wait independent and OS-cold checks','production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(F/'status.json').write_text(json.dumps(s,indent=2))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def put(p,b):p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.final-tmp');tmp.write_bytes(b);tmp.replace(p)
env=dict(os.environ,GOCACHE='/data/gtron/deploy-cache/go-build',GOPATH='/root/go',GOMODCACHE='/root/go/pkg/mod',GOTOOLCHAIN='local',CGO_ENABLED='1',CC='/opt/rh/devtoolset-11/root/usr/bin/gcc',CXX='/opt/rh/devtoolset-11/root/usr/bin/g++',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def run(cmd,log,ev=env,cwd=None):
 with (F/log).open('wb') as f:subprocess.check_call(cmd,env=ev,cwd=cwd,stdout=f,stderr=subprocess.STDOUT)
try:
 save()
 for i in range(360):
  states=[json.loads((p/'status.json').read_text()) for p in [M,Q]]
  if any(x.get('error') for x in states):raise RuntimeError(states)
  if all(x.get('complete') for x in states):break
  time.sleep(10)
 else:raise RuntimeError('offline acceptance did not finish within one hour')
 assert json.loads((M/'strict.json').read_text())['passed']
 s.update(stage='native final state bridge correctness');save()
 packet=json.loads(zlib.decompress((A/'metadata-state-final.zlib').read_bytes()));src=F/'source';shutil.copytree(str(M/'source'),str(src),copy_function=os.link)
 assert {n:sha(src/n) for n in packet['product_before']}==packet['product_before']
 for n,b in packet['files'].items():put(src/n,base64.b64decode(b))
 actual={n:sha(src/n) for n in packet['product_after']};assert actual==packet['product_after'];(F/'product-sha256.json').write_text(json.dumps(actual,indent=2))
 run(['/data/go/bin/go','test','./core/state','-count=1','-timeout=10m'],'state-tests.log',cwd=str(src))
 run(['/data/go/bin/go','test','./core','-run=History|Reorg|MuHash|Snapshot|Versioned|ParallelTransfer','-count=1','-timeout=15m'],'core-focused-tests.log',cwd=str(src))
 s.update(stage='trace both private read sets, excluded from latency samples');save()
 proof=json.loads((Q/'private-files-proof.json').read_text())+json.loads((Q/'private-extra-proof.json').read_text());private={r['path']:r for r in proof};receipts={}
 for mode in ['baseline','candidate']:
  out=F/('trace-'+mode);out.mkdir();cat=Q/mode;binary=A/'final/baseline.test' if mode=='baseline' else M/'candidate.test'
  e=dict(env,GTRON_NINEM_CATALOG=str(cat),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1')
  run(['strace','-f','-e','trace=openat','-o',str(out/'openat.log'),str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],str(out.relative_to(F)/'run.log'),e)
  assert json.loads((out/'rows.json').read_text())==json.loads((A/'probe/baseline-first/rows.json').read_text())
  refs={r['path']:r for r in json.loads((cat/'manifest.json').read_text())['segments']};paths=set()
  for line in (out/'openat.log').read_text().splitlines():
   m=re.search(r'openat\([^,]+, "([^"]+)"',line)
   if m and m.group(1).startswith(str(cat)+'/'):
    rel=m.group(1)[len(str(cat))+1:]
    if rel in refs:paths.add(rel)
  assert paths and paths<=private.keys(),sorted(paths-private.keys())
  for n in paths:
   st=(cat/n).stat();r=private[n];assert (st.st_dev,st.st_ino)==(r['private_dev'],r['private_ino'])
  receipts[mode]={'paths':sorted(paths),'opened_files':len(paths),'covered_by_independent_inode_proof':True,'includes_unfinished_attempts_conservatively':True,'timing_excluded':True}
 (F/'trace-private-coverage.json').write_text(json.dumps(receipts,indent=2))
 s.update(stage='canonical manifest and production identity checks');save()
 before=json.loads((A/'baseline-catalog/manifest.json').read_text());previous=json.loads((A/'full-route/catalog/manifest.json').read_text());after=json.loads((M/'catalog/manifest.json').read_text());canon=lambda m:[r for r in m['segments'] if 'routing' not in r['kind']]
 assert canon(before)==canon(after)
 assert canon(previous)==canon(after)
 assert {k:v for k,v in previous.items() if k!='segments'}=={k:v for k,v in after.items() if k!='segments'}
 for name,m in [('before',before),('previous',previous),('after',after)]: (F/(name+'-manifest.json.gz')).write_bytes(gzip.compress(json.dumps(m,separators=(',',':')).encode()))
 routes=[r for r in after['segments'] if r['kind']=='metadata-history-routing'];assert len(routes)==116 and sum(r['rows'] for r in routes)==29658
 (F/'coverage.json').write_text(json.dumps({'canonical_refs_unchanged':True,'top_level_manifest_equal_previous_candidate':True,'sources':29658,'pages':len(routes),'metadata_page_bytes':sum(r['size'] for r in routes),'refs':routes},indent=2))
 pid=subprocess.check_output(['systemctl','show','gtron','--property=MainPID']).decode().strip().split('=')[1];p=pathlib.Path('/proc')/pid
 latest={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':pid,'start_ticks':p.joinpath('stat').read_text().split(')')[-1].split()[19],'exe':os.readlink(str(p/'exe')),'sha256':sha(p/'exe'),'service':subprocess.check_output(['systemctl','show','gtron','--property=MainPID','--property=ActiveState']).decode()}
 initial=json.loads((M/'runtime-identity.json').read_text());latest['identity_unchanged']=all(latest[k]==initial[k] for k in ['pid','start_ticks','exe','sha256']);assert latest['identity_unchanged'];(F/'production-identity.json').write_text(json.dumps(latest,indent=2))
 (F/'acceptance.json').write_text(json.dumps({'complete':True,'native_state_checks_passed':True,'private_trace_coverage_passed':True,'canonical_manifest_unchanged':True,'production_identity_unchanged':True},indent=2))
 s.update(stage='sealed evidence pack');save();files=set()
 for root in [M,Q,F]:
  for pattern in ['*.json','*.jsonl','*.log','repeat-*/*','metadata-*/*','os-cold-*/*','restoration/*','trace-*/*']:
   files.update(p for p in root.glob(pattern) if p.is_file() and p.stat().st_size<100_000_000)
 for r in routes:files.add(M/'catalog'/r['path'])
 for n in packet['product_after']:files.add(src/n)
 for p in src.rglob('*metadata*routing*test.go'):files.add(p)
 for n in packet['files']:files.add(src/n)
 for n in ['core/state/contract_history_test.go','core/state/snapshots/history_metadata_routing_test.go']:files.add(src/n)
 for n in ['metadata-packet-v3.zlib','run-metadata-v3.py','run-metadata-cold-v3.py','run-metadata-finish.py','metadata-state-final.zlib','probe/queries.json','probe/baseline-first/rows.json']:files.add(A/n)
 dest=pathlib.Path('/tmp/AAA-gtron-metadata-routing-final-20261027.zip');tmp=dest.with_suffix('.partial');members={}
 with zipfile.ZipFile(str(tmp),'w',zipfile.ZIP_DEFLATED,allowZip64=True) as z:
  for p in sorted(files):
   name=str(p.relative_to(A));b=p.read_bytes();members[name]={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()};z.writestr(name,b)
  z.writestr('files-sha256.json',json.dumps(members,indent=2))
 tmp.replace(dest);u=pwd.getpwnam('java-tron');os.chown(str(dest),u.pw_uid,u.pw_gid);os.chmod(str(dest),0o644)
 seal={'file':str(dest),'bytes':dest.stat().st_size,'sha256':sha(dest),'members':len(members)};(A/'metadata-evidence-seal.json').write_text(json.dumps(seal,indent=2));s.update(complete=True,stage='done',seal=seal);save();print(json.dumps(s))
except Exception:s['error']=traceback.format_exc();save();raise
