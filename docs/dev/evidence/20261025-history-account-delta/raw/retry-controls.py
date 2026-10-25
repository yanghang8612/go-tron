import pathlib,json,hashlib,shutil,subprocess,datetime
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01')
assert json.loads((P/'acceptance-status.json').read_text())['complete']
assert json.loads((P/'postprocess-status.json').read_text())['phase']=='failed'
assert not (P/'final-space-and-physical-closure.json').exists()
assert not (P/'candidate-source/core/state/snapshots/offline_account_delta_decoder_test.go').exists()
assert not (P/'account-controls-files.jsonl').read_text().strip()
old=P/'candidate-source/core/state/snapshots/offline_account_delta_controls_test.go'
preserve=P/'offline_account_delta_controls_test.first-failed.go';assert not preserve.exists();shutil.copy2(old,preserve)
archived=[]
for n in ('postprocess-status.json','post-snapshots-build.log','account-controls.log','account-controls-status.json','account-controls-files.jsonl','decoder-study-status.json'):
 f=P/n;to=f.with_name(f.stem+'.first-failed'+f.suffix);assert f.exists() and not to.exists();f.rename(to);archived.append(dict(path=str(to),bytes=to.stat().st_size,sha256=hashlib.sha256(to.read_bytes()).hexdigest()))
a=P/'plain-controls';b=P/'plain-controls.first-failed';assert a.is_dir() and not b.exists();a.rename(b)
bad=[dict(path=str(f),bytes=f.stat().st_size,sha256=hashlib.sha256(f.read_bytes()).hexdigest())for f in b.iterdir()if f.is_file()]
note=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),production_mutations=False,
 reason='Correct the offline ordinary-Zstd control generator to finish its footer stream through finishWithFooterContext; add a native/local footer fixture. HTTP binaries and fixed source/candidate inputs are unchanged.',
 archived=archived,failed_control_files=bad,old_harness_sha256=hashlib.sha256(preserve.read_bytes()).hexdigest(),new_harness_sha256=hashlib.sha256((P/old.name).read_bytes()).hexdigest(),
 candidate_binary_sha256=hashlib.sha256((P/'candidate.test').read_bytes()).hexdigest(),candidate_manifest_sha256=hashlib.sha256((P/'candidate-input/state-snapshots/manifest.json').read_bytes()).hexdigest())
(P/'postprocess-retry.json').write_text(json.dumps(note,indent=2))
(P/'postprocess-status.json').write_text(json.dumps(dict(complete=False,phase='retry-controls-with-footer-fixture',production_mutations=False)))
for unit,script,memory in [('gtron-account-delta-postprocess-v2-20261025-01','postprocess.py','4G'),('gtron-account-delta-decoder-study-v2-20261025-01','decoder-study.py','2G')]:
 subprocess.run(['systemd-run','--unit='+unit,'-p','MemoryLimit='+memory,'-p','CPUQuota=200%','-p','Nice=15','/usr/bin/ionice','-c3','/usr/bin/python3',str(P/script)],check=True)
print(json.dumps(note,indent=2),flush=True)
