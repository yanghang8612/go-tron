import pathlib,json,hashlib,shutil,os,subprocess,time,urllib.request,signal,datetime,traceback
LAB=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/tail-read-20261021-01');ROOT=LAB/'cold-cause-debug';ROOT.mkdir(exist_ok=False);SOURCE=ROOT/'source';state={'complete':False,'excluded_from_performance':True,'production_mutations':False}
def save():state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'status.json').write_text(json.dumps(state,indent=2))
try:
 state['phase']='copy-debug-overlay';save();shutil.copytree(LAB/'source',SOURCE,ignore=shutil.ignore_patterns('build','target','.git'));(SOURCE/'third_party/librustzcash/target').symlink_to((LAB/'source/third_party/librustzcash/target').resolve(),target_is_directory=True)
 p=SOURCE/'core/rawdb/state_history_query_first.go';s=p.read_text().replace('"context"','"context"\n"fmt"\n"os"');s=s.replace('if !ok {\n\t\treturn iterateStateHistoryOwnerTail', 'if !ok {\nfmt.Fprintf(os.Stderr,"TAIL_UNWRAPPED %T\\n",db)\n\t\treturn iterateStateHistoryOwnerTail');s=s.replace('old := v.first[owner]', 'old := v.first[owner]\nfmt.Fprintf(os.Stderr,"TAIL_START view=%p owner=%x bounds=%d/%d tx=%d/%d old=%+v rejected=%v\\n",v,owner,from,to,targetTx,headTx,old,v.rejected[owner])');s=s.replace('if stopped || overflow || !v.rejected[owner] {', 'fmt.Fprintf(os.Stderr,"TAIL_END view=%p owner=%x stopped=%v overflow=%v rejected=%v rows=%d used=%d\\n",v,owner,stopped,overflow,v.rejected[owner],len(next.rows),v.used)\nif stopped || overflow || !v.rejected[owner] {');p.write_text(s)
 p=SOURCE/'core/rawdb/state_history_query_tail.go';s=p.read_text().replace('"context"','"context"\n"fmt"\n"os"');needle='func iterateStateHistoryOwnerTail(ctx context.Context, db StateKVHistoryReader, from, to uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {';s=s.replace(needle,needle+'\nfmt.Fprintf(os.Stderr,"TAIL_PHYSICAL owner=%x blocks=%d/%d\\n",owner,from,to)');p.write_text(s)
 p=SOURCE/'core/state/snapshots/history_binary.go';s=p.read_text();needle='func iterateStateDomainChangeBinarySegmentByAccessorKeysFile(ctx context.Context, dir string, ref SegmentRef, accessorRef SegmentRef, lookupKeys [][]byte, fromTxNum, toTxNum uint64, fn func([]byte, *rawdb.StateDomainChange) (bool, error)) error {';assert needle in s;s=s.replace(needle,needle+'\nfmt.Fprintf(os.Stderr,"COLD_OPEN path=%s keys=%x from=%d to=%d\\n",ref.Path,lookupKeys,fromTxNum,toTxNum)');p.write_text(s)
 (ROOT/'cold-overlay-sha256.txt').write_text(hashlib.sha256(p.read_bytes()).hexdigest())
 (ROOT/'overlay-hashes.json').write_text(json.dumps({str(p.relative_to(SOURCE)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [SOURCE/'core/rawdb/state_history_query_first.go',SOURCE/'core/rawdb/state_history_query_tail.go']},indent=2));env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB');binary=ROOT/'debug.test';state['phase']='build';save()
 with (ROOT/'build.log').open('wb') as out:subprocess.run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(binary)],cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=600)
 host=ROOT/'host';host.mkdir();env.update(GTRON_FIXED_INPUT='/data/gtron/cold-query-layout-20261020-01/account-gc-input',GTRON_FIXED_OUTPUT=str(host));state['phase']='single-excluded-http';save()
 with (host/'host.log').open('wb') as out:
  process=subprocess.Popen([str(binary),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=4m'],env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
  try:
   start=time.monotonic()
   while not (host/'ready.json').exists():
    assert process.poll() is None
    if time.monotonic()-start>120:raise TimeoutError('debug host')
    time.sleep(.1)
   ready=json.loads((host/'ready.json').read_text());body=json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},'0x895440']}).encode()
   with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=150) as r:result=json.loads(r.read())
   assert result.get('result')=='0x'+format(207,'064x'),result;(ROOT/'response.json').write_text(json.dumps(result))
  finally:
   if process.poll() is None:os.kill(ready['pid'],signal.SIGTERM);process.wait(timeout=20)
   assert process.returncode==0
 log=(host/'host.log').read_text();(ROOT/'tail-events.txt').write_text('\n'.join(l for l in log.splitlines() if l.startswith(('TAIL_','COLD_')))+'\n');state.update(complete=True,phase='complete',all_results_correct=True);save()
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
