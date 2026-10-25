import pathlib,json,subprocess,hashlib,shutil,os,datetime
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');assert not json.loads((P/'postprocess-status.json').read_text())['complete']
assert json.loads((P/'postprocess-status.json').read_text())['phase']=='account-controls'
subprocess.run(['systemctl','stop','gtron-account-delta-postprocess-v2-20261025-01'],check=True)
def sha(f):
 h=hashlib.sha256()
 with f.open('rb')as r:
  for b in iter(lambda:r.read(4<<20),b''):h.update(b)
 return h.hexdigest()
archive=[]
for n in ('account-controls-files.jsonl','account-controls-status.json','account-controls.log','postprocess-status.json','controls-fixture.log'):
 a=P/n;b=a.with_name(a.stem+'.serial-interrupted'+a.suffix);assert not b.exists();shutil.copy2(a,b);archive.append(dict(path=str(b),bytes=b.stat().st_size,sha256=sha(b)))
src=P/'candidate-source/core/state/snapshots/offline_account_delta_controls_test.go';to=P/'offline_account_delta_controls_test.serial.go';assert not to.exists();shutil.copy2(src,to)
raw=(P/'account-controls-files.jsonl').read_bytes();parts=raw.splitlines(keepends=True);valid=[];rows=[];dropped=0
for i,line in enumerate(parts):
 try:r=json.loads(line)
 except ValueError:
  assert i==len(parts)-1 and not line.endswith(b'\n');dropped=len(line);break
 assert r['Source']['path'] not in {x['Source']['path']for x in rows};rows.append(r);valid.append(line if line.endswith(b'\n')else line+b'\n')
assert rows
clean=P/'account-controls-files.clean';clean.write_bytes(b''.join(valid));clean.replace(P/'account-controls-files.jsonl')
live={r['ControlPath']for r in rows if r['ControlPath']};orphans=P/'control-serial-orphans';assert not orphans.exists();orphans.mkdir();moved=[]
for f in (P/'plain-controls').iterdir():
 if str(f) not in live:
  assert f.is_file();q=orphans/f.name;f.rename(q);moved.append(dict(path=str(q),bytes=q.stat().st_size,sha256=sha(q)))
note=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),production_mutations=False,resume_rows=len(rows),dropped_incomplete_last_line_bytes=dropped,
 archived=archive,preserved_unreferenced_files=moved,source_harness_sha256=sha(to),resume_harness_sha256=sha(P/src.name),
 candidate_binary_sha256=sha(P/'candidate.test'),candidate_manifest_sha256=sha(P/'candidate-input/state-snapshots/manifest.json'),
 proof='Only the owned serial postprocess unit stopped; all completed control rows are rechecked against source refs and full control SHA before four-worker continuation. Previous rows and orphan files are retained.')
(P/'controls-resume-receipt.json').write_text(json.dumps(note,indent=2))
(P/'postprocess-status.json').write_text(json.dumps(dict(complete=False,phase='resume-verified-controls-four-workers',production_mutations=False)))
subprocess.run(['systemd-run','--unit=gtron-account-delta-postprocess-v3-20261025-01','-p','MemoryLimit=4G','-p','CPUQuota=400%','-p','Nice=15','/usr/bin/ionice','-c3','/usr/bin/python3',str(P/'postprocess.py')],check=True)
print(json.dumps(note,indent=2),flush=True)
