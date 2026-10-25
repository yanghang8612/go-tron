import os,json,pathlib,shutil,hashlib,subprocess,datetime,traceback
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');OLD=pathlib.Path('/data/gtron/history-key-owner-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'prepare-status.json').write_text(json.dumps(state,indent=2))
try:
 assert shutil.disk_usage(str(P)).free>250*(1<<30)
 expected=json.loads((OLD/'native-status.json').read_text())['production_source_inventory']
 for n,v in expected.items():assert sha(OLD/'candidate-source'/n)==v,n
 def ignore(path,names):return ['target'] if path==str(OLD/'candidate-source/third_party/librustzcash') else (['build','.git'] if path==str(OLD/'candidate-source') else [])
 shutil.copytree(str(OLD/'candidate-source'),str(S),symlinks=True,ignore=ignore)
 lib=S/'third_party/librustzcash/target/release/librustzcash.a';lib.parent.mkdir(parents=True,exist_ok=True);os.link(str(OLD/'candidate-source/third_party/librustzcash/target/release/librustzcash.a'),str(lib))
 delta=json.loads((P/'source-delta.json').read_text())
 for n,v in delta['files'].items():
  path=S/n
  if v['before'] is None:assert not path.exists(),n
  else:assert sha(path)==v['before'],n
  path.parent.mkdir(parents=True,exist_ok=True);path.write_text(v['text']);assert sha(path)==v['sha256']
 for n in ['offline_v11_corpus_test.go']:shutil.copy2(str(P/n),str(S/'core/state/snapshots'/n))
 os.link(str(OLD/'candidate.test'),str(P/'baseline.test'))
 fields=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','ExecMainStartTimestamp']).decode().splitlines());pid=int(fields['MainPID']);fields['sha256']=sha(pathlib.Path('/proc/%d/exe'%pid)) if pid else None
 state.update(complete=True,production_identity=fields,baseline_inventory=expected,production_source_inventory={n:sha(S/n) for n in set(expected)|{n for n in delta['files'] if not n.endswith('_test.go')}},baseline_binary_sha256=sha(P/'baseline.test'),http_harness_sha256=sha(S/'core/offline_fixed_http_test.go'));assert state['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf';save();print('PREPARED',len(expected),fields)
except BaseException as e:state.update(error=repr(e),traceback=traceback.format_exc(),phase='failed');save();raise
