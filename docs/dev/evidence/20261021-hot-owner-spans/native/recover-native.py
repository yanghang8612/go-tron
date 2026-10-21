import pathlib,json,subprocess,shutil,re,hashlib,datetime
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-03')
REC=ROOT/'preparation-recovery';REC.mkdir(exist_ok=False)
for name in ['prepare-native.py','preparation-status.json','layout-status.json','audit-status.json','resource-status.json','seal-status.json']:
 p=ROOT/name
 if p.exists():shutil.copy2(p,REC/name)
for name in ['tail-read-comparison','nine-diagnostic']:
 p=ROOT/name
 if p.exists():p.rename(REC/('failed-'+name))
assert not (ROOT/'owner-index-input').exists()
# Only offline units were launched. Preserve a separately observed production
# stop; do not start, stop, restart, reload or reset the production service.
identity_fn='''def identity():
 fields=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode().splitlines())
 pid=int(fields['MainPID']);return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest() if pid else None,'active_state':fields['ActiveState']}
'''
exec(identity_fn,globals())
baseline=identity();assert baseline=={'pid':0,'sha256':None,'active_state':'inactive'},baseline
(ROOT/'production-baseline.json').write_text(json.dumps(baseline,indent=2))
prepare='''import pathlib,json,hashlib,subprocess,datetime
ROOT=pathlib.Path(ROOT_PATH_LITERAL)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
'''+identity_fn+'''before=identity();assert before==json.loads((ROOT/'production-baseline.json').read_text())
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
'''
prepare=prepare.replace('ROOT_PATH_LITERAL',repr(str(ROOT)));compile(prepare,str(ROOT/'prepare-recovered.py'),'exec')
(ROOT/'prepare-recovered.py').write_text(prepare)
files={'layout':'layout-native.py','compare':'compare-native.py','audit':'audit-native.py','diagnostic':'diagnose-nine.py','resources':'resources.py','seal':'seal-native.py'}
for phase,filename in files.items():
 s=(ROOT/filename).read_text().replace('gtron-owner-blake-','gtron-owner-blake-r-')
 if 'def identity():' in s:s,n=re.subn(r'def identity\(\):\n(?:[ \t].*\n)+',identity_fn,s,count=1);assert n==1
 if phase=='seal':
  s=s.replace('try:\n',identity_fn+'try:\n',1)
  old="pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);live={'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid)),"
  assert old in s;s=s.replace(old,"live={**identity(),")
  old="assert live['pid']==28737 and live['sha256']=='5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4' and live['old_database_exists']"
  assert old in s;s=s.replace(old,"expected=json.loads((ROOT/'production-baseline.json').read_text());assert all(live[k]==expected[k] for k in ['pid','sha256','active_state']) and live['old_database_exists']")
 p=ROOT/(phase+'-recovered.py');compile(s,str(p),'exec');p.write_text(s)
subprocess.run(['systemd-run','--unit=gtron-owner-blake-r-prepare-20261021','-p','CPUQuota=100%','-p','MemoryLimit=536870912','/usr/bin/python3',str(ROOT/'prepare-recovered.py')],check=True)
for phase in ['layout','compare','audit','diagnostic','resources','seal']:
 extra=['tail-read'] if phase=='compare' else [];memory='268435456' if phase=='resources' else ('536870912' if phase=='seal' else '5368709120');cpu='25%' if phase=='resources' else ('100%' if phase=='seal' else '200%')
 subprocess.run(['systemd-run','--unit=gtron-owner-blake-r-'+phase+'-20261021','-p','CPUQuota='+cpu,'-p','MemoryLimit='+memory,'/usr/bin/nice','-n','15','/usr/bin/ionice','-c','3','/usr/bin/python3',str(ROOT/(phase+'-recovered.py')),*extra],check=True)
print('ONLY OFFLINE VALIDATION RESUMED; PRODUCTION REMAINS INACTIVE')
