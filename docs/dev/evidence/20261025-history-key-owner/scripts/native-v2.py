import pathlib,json,subprocess,os,hashlib,datetime,traceback,shutil
P=pathlib.Path('/data/gtron/history-key-owner-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False,'read_only_decoder_refinement':True}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'native-status.json').write_text(json.dumps(state,indent=2))
env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def run(label,args,timeout=1500):
 state['phase']=label;save()
 with (P/(label+'.log')).open('wb') as out:subprocess.run(args,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
try:
 old=json.loads((P/'native-status.json').read_text());assert old['complete'];assert not (P/'native-v1-status.json').exists();shutil.copy2(str(P/'native-status.json'),str(P/'native-v1-status.json'))
 assert not (P/'tail-read-comparison').exists()
 pipeline=json.loads((P/'pipeline-status.json').read_text());assert pipeline['phase'] not in ('full-http-comparison','complete'),pipeline
 subprocess.check_call(['systemctl','stop','gtron-key-owner-pipeline-20261025'])
 shutil.copy2(str(P/'pipeline-status.json'),str(P/'native-v1-pipeline-status.json'))
 for n in ['muhash-census.json','muhash.log','history-restore.json','history-restore.log']:
  if (P/n).exists():os.rename(str(P/n),str(P/('native-v1-'+n)))
 for n,d in old['production_source_inventory'].items():assert sha(S/n)==d,n
 patch=json.loads((P/'native-v2-delta.json').read_text())
 for n,v in patch['files'].items():
  f=S/n;assert sha(f)==v['before_sha256'],n;f.write_text(v['text']);assert sha(f)==v['after_sha256'],n
 for n in ['candidate.test','snapshots.test','gtron-candidate']:os.rename(str(P/n),str(P/('native-v1-'+n)))
 inv={n:sha(S/n) for n in old['production_source_inventory']};state['production_source_inventory']=inv;state['http_harness_sha256']=sha(S/'core/offline_fixed_http_test.go');state['format_encoding_unchanged']=True;state['original_space_converter_binary']=old['binaries']['snapshots.test']
 state['phase']='native2-preparing';save()
 subprocess.check_call(['systemd-run','--unit=gtron-key-owner-pipeline-20261025-v2','-p','MemoryLimit=4G','-p','CPUQuota=200%','/usr/bin/nice','-n','15','/usr/bin/ionice','-c','3','/usr/bin/python3',str(P/'pipeline.py')])
 run('native2-reader-tests',['/data/go/bin/go','test','./core/state/snapshots','-run','Test(HistoryOwner|HistoryOffsetPages|StateDomainChangeV[789]|HistoryPostingWriter)','-count=1','-timeout=180s'],240)
 run('native2-snapshots-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'snapshots.test')],300)
 run('native2-candidate-build',['/data/go/bin/go','test','-c','./core','-o',str(P/'candidate.test')],300)
 run('native2-packages',['/data/go/bin/go','test','./core/rawdb','./core/state','./core/state/snapshots','./core','./cmd/gtron','-count=1','-timeout=1200s'],1400)
 run('native2-race',['/data/go/bin/go','test','-race','./core/state/snapshots','-run','Test(HistoryOwner|HistoryOffsetPages|StateDomainChangeV[789]|HistoryPostingWriter)','-count=1','-timeout=240s'],300)
 run('native2-vet',['/data/go/bin/go','vet','./core/state/snapshots','./core/state','./core/rawdb','./core','./cmd/gtron'],300)
 run('native2-gtron-build',['/data/go/bin/go','build','-o',str(P/'gtron-candidate'),'./cmd/gtron'],300)
 for n,d in inv.items():assert sha(S/n)==d,n
 state['binaries']={n:sha(P/n) for n in ['baseline.test','candidate.test','snapshots.test','gtron-candidate']};state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
