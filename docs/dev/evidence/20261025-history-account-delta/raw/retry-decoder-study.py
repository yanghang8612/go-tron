import pathlib,json,hashlib,subprocess,datetime
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01')
assert json.loads((P/'postprocess-status.json').read_text())['complete']
assert json.loads((P/'decoder-study-status.json').read_text())['phase']=='failed'
archive=[]
for n in ('decoder-study-status.json','decoder-study.log','decoder-study-build.log','decoder-plan.json','decoder-snapshots.test'):
 a=P/n;b=a.with_name(a.stem+'.borrowed-first'+a.suffix);assert a.exists() and not b.exists();a.rename(b);archive.append(str(b))
src=P/'candidate-source/core/state/snapshots/offline_account_delta_decoder_test.go';to=P/'offline_account_delta_decoder_test.borrowed-first.go';assert src.exists() and not to.exists();oldsha=hashlib.sha256(src.read_bytes()).hexdigest();src.rename(to)
note=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),production_mutations=False,archived=archive,old_harness_sha256=oldsha,
 new_harness_sha256=hashlib.sha256((P/src.name).read_bytes()).hexdigest(),
 reason='The diagnostic sampled a borrowed compressed reader buffer, then advanced that same reader for the source oracle before copying it. Copy immediately and add a local/native ownership fixture; core codec and HTTP inputs/binaries are unchanged.')
(P/'decoder-study-retry.json').write_text(json.dumps(note,indent=2))
subprocess.run(['systemd-run','--unit=gtron-account-delta-decoder-study-v3-20261025-01','-p','MemoryLimit=2G','-p','CPUQuota=200%','-p','Nice=15','/usr/bin/ionice','-c3','/usr/bin/python3',str(P/'decoder-study.py')],check=True)
print(json.dumps(note,indent=2),flush=True)
