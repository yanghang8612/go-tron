import pathlib,json,hashlib,subprocess,datetime
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-03')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def identity():
 fields=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode().splitlines())
 pid=int(fields['MainPID']);return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest() if pid else None,'active_state':fields['ActiveState']}
before=identity();assert before==json.loads((ROOT/'production-baseline.json').read_text())
sid=json.loads((ROOT/'source-identity.json').read_text());assert sid['commit']=='43c3d47bc1229223af677ff4c56462744cf54631'
for name,digest in sid['production_sources'].items():assert sha(ROOT/'source'/name)==digest,name
for name,digest in sid['binaries'].items():assert sha(ROOT/name)==digest,name
# The original native driver reached its final identity guard only after all
# check=True test, race, vet and build commands. Preserve its failure verbatim.
original=json.loads((ROOT/'preparation-recovery/preparation-status.json').read_text());assert '/proc/0/exe' in original['traceback'] and 'after_identity' in original['traceback']
logs={n:sha(ROOT/n) for n in ['native-packages.log','native-race.log','native-core-correctness.log','native-vet.log','native-build.log','http-build.log','rawdb-build.log','snapshots-build.log']}
assert 'FAIL' not in (ROOT/'native-packages.log').read_text() and 'FAIL' not in (ROOT/'native-race.log').read_text()
after=identity();assert before==after
receipt={'complete':True,'source_commit':sid['commit'],'production_files_checked':len(sid['production_sources']),'binaries_checked':sid['binaries'],'native_log_sha256':logs,'reason':'Native tests/build completed; final production identity guard encountered an independently observed clean service stop. Zero HTTP samples had run. Resume offline validation with the service remaining inactive.','before_identity':before,'after_identity':after,'production_mutations':False,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(ROOT/'preparation-recovery/receipt.json').write_text(json.dumps(receipt,indent=2))
state={'complete':True,'phase':'complete','original_before_identity':original['before_identity'],'external_identity_change_record':'production-external-stop.json','before_identity':before,'after_identity':after,'recovery':'preparation-recovery/receipt.json','production_mutations':False,'utc':receipt['utc']};p=ROOT/'preparation-status.json';q=p.with_suffix('.tmp');q.write_text(json.dumps(state,indent=2));q.replace(p)
