# Same fixed state and real old/new maintenance, followed by full HTTP queries.
import pathlib,sys,json,os,shutil,struct,hashlib,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/metadata-repair-ablation');R.mkdir()
s={'complete':False,'stage':'native builds','source':SOURCE,'base':'52aea62ef56799b7e6c1365293123dd029286935','production_mutations':False,'canonical_data_mutations':False,'cache':'fresh HTTP processes; shared OS caches uncontrolled; no eviction'};requests=[]
base=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01/final/indexed-input')
try:
 s['production_before']=identity();save();paircode=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/coverage_census.py').decode();exec(compile(paircode[paircode.index('def pair('):paircode.index('\nsources=[]')],'exact-pair','exec'))
 fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_metadata_repair_test.go.txt')
 sources={}
 for version,sha in [('base',s['base']),('candidate',SOURCE)]:
  src=archive_source('source-'+version,sha);sources[version]=src;(src/'core/state/snapshots/offline_metadata_repair_test.go').write_bytes(fixture)
  command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/(version+'-snapshots.test'))],src,R/(version+'-build.log'),300)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','TestMetadataHistoryRouting|TestHistoryRoutingBootstrapMaintenance','-count=1','-timeout=150s'],sources['candidate'],R/'candidate-focused.log',210)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'candidate-http.test')],sources['candidate'],R/'candidate-http-build.log',300)
 original=json.loads((base/'state-snapshots/manifest.json').read_text());by_path={r['path']:r for r in original['segments']};current=set(pair(h,by_path[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))]) for h in original['segments'] if h['kind']=='history' and h.get('dataset')=='state-domain-change')
 eligible=sorted([r for r in original['segments'] if r['kind']=='metadata-history-routing' and r.get('rows',0)>=32 and r['fromTxNum']>3006279261],key=lambda x:x['fromTxNum'])[:48];assert len(eligible)>=8,len(eligible);s['hole_pages']=len(eligible);save()
 for version in ['base','candidate']:
  dest=R/('input-'+version);dest.mkdir()
  for p in base.iterdir():
   if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
   else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
  m=json.loads(json.dumps(original));snap=dest/'state-snapshots'
  for index,old in enumerate(eligible):
   b=(snap/old['path']).read_bytes();assert 'sha256:'+hashlib.sha256(b).hexdigest()==old['checksum'];pos=32;items=[]
   for i in range(old['rows']):
    start=pos;count=int.from_bytes(b[pos+32:pos+40],'big');pos+=40+(1024 if count else 0);items.append(b[start:pos])
   assert pos==len(b);drop=next(i for i,x in enumerate(items) if x[:32] in current);items.pop(drop);header=bytearray(b[:32]);header[12:16]=struct.pack('>I',len(items));raw=bytes(header)+b''.join(items);digest=hashlib.sha256(raw).hexdigest();new=dict(old,path='history/metadata-routing-hole-%d-%s.route'%(index,digest),rows=len(items),size=len(raw),checksum='sha256:'+digest);(snap/new['path']).write_bytes(raw);m['segments'][m['segments'].index(old)]=new
  m['generation']+=1;nextfile=snap/'manifest.ablation-next.json';nextfile.write_text(json.dumps(m));os.replace(str(nextfile),str(snap/'manifest.json'))
  s['stage']=version+' real cursor maintenance';save();out=R/(version+'-maintenance');out.mkdir();env=dict(E,GTRON_META_ABLATION_INPUT=str(snap),GTRON_META_ABLATION_OUTPUT=str(out),GTRON_META_ABLATION_PASSES=str(len(eligible)),GTRON_META_ABLATION_START=str(eligible[0]['fromTxNum']))
  with (out/'host.log').open('wb') as log:subprocess.check_call([str(R/(version+'-snapshots.test')),'-test.run=^TestOfflineMetadataProofRepair$','-test.v','-test.timeout=5m'],cwd=str(R),env=env,stdout=log,stderr=subprocess.STDOUT,timeout=330)
 for name,version in [('base-1','base'),('candidate-1','candidate'),('candidate-2','candidate'),('base-2','base')]:
  s['stage']=name+' full HTTP';save();exe=pathlib.Path('/data/gtron/query-plan-rollout-20261028-01/merged-http.test') if version=='base' else R/'candidate-http.test'
  with host(name,exe,R/('input-'+version)) as ready:
   for height,want in [(9000000,207),(10000000,213),(11000000,220),(15000000,238),(20000000,249)]:
    for repeat in range(2):
     q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};start=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
     with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
     row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-start,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want,row;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='metadata coverage repair maintenance and HTTP complete',all_returns_match=True,requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
