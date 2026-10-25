import pathlib, json, hashlib, zipfile, subprocess, datetime, os

P = pathlib.Path('/data/gtron/history-account-delta-20261025-01')
for name in ('layout-status.json', 'native-status.json', 'muhash-driver-status.json',
             'acceptance-status.json', 'postprocess-status.json', 'decoder-study-status.json'):
    assert json.loads((P / name).read_text())['complete'], name
closure = json.loads((P / 'final-space-and-physical-closure.json').read_text())
assert closure['all_referenced_physical_sha_verified']
assert closure['canonical_chain_and_frontiers_unchanged']
assert closure['production_reclaimed_bytes'] == 0
def sha(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(4 << 20), b''):
            h.update(b)
    return h.hexdigest()
inventory = json.loads((P / 'final-source-inventory.json').read_text())
for n, d in inventory.items():
    assert sha(P / 'candidate-source' / n) == d, n
native=json.loads((P/'native-status.json').read_text())
for n,d in native['binaries'].items():
    assert sha(P/n)==d,n
study=json.loads((P/'decoder-study-status.json').read_text())
assert sha(P/'decoder-snapshots.test')==study['binary_sha256']
final_binaries=dict(native['binaries'], **{'post-snapshots.test':sha(P/'post-snapshots.test'),
    'decoder-snapshots.test':study['binary_sha256']})
(P/'final-binary-identity.json').write_text(json.dumps(dict(binaries=final_binaries,
    source_inventory_sha256=sha(P/'final-source-inventory.json'),
    http_harness_sha256=sha(P/'candidate-source/core/offline_fixed_http_test.go'),
    controls_harness_sha256=sha(P/'candidate-source/core/state/snapshots/offline_account_delta_controls_test.go')),indent=2))
units = ['gtron-account-delta-final-layout-20261025-01',
         'gtron-account-delta-bounded-native-v2-20261025-01',
         'gtron-account-delta-muhash-20261025-01',
         'gtron-account-delta-acceptance-20261025-01',
         'gtron-account-delta-postprocess-20261025-01',
         'gtron-account-delta-decoder-study-20261025-01',
         'gtron-account-delta-postprocess-v2-20261025-01',
         'gtron-account-delta-postprocess-v3-20261025-01',
         'gtron-account-delta-decoder-study-v2-20261025-01',
         'gtron-account-delta-decoder-study-v3-20261025-01']
environment = dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    production_mutations=False,
    production_identity=subprocess.check_output(['systemctl', 'show', 'gtron',
        '-p', 'MainPID', '-p', 'ActiveState']).decode(),
    loadavg=pathlib.Path('/proc/loadavg').read_text(),
    meminfo=pathlib.Path('/proc/meminfo').read_text(),
    disk=subprocess.check_output(['df', '-B1', '/data']).decode(),
    task_units={u:subprocess.run(['systemctl', 'show', u, '-p', 'MainPID',
        '-p', 'ActiveState', '-p', 'Result', '-p', 'ExecMainStatus',
        '-p', 'MemoryLimit', '-p', 'CPUQuotaPerSecUSec', '-p', 'Nice'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.decode() for u in units})
(P / 'final-environment.json').write_text(json.dumps(environment, indent=2))
files = {}
for mode in ('baseline','candidate'):
    files['manifests/'+mode+'.json']=P/(mode+'-input/state-snapshots/manifest.json')
for f in P.iterdir():
    if f.is_file() and f.suffix in ('.json', '.jsonl', '.log', '.txt', '.py', '.go', '.v1', '.v2', '.v3'):
        if f.name not in ('SHA256SUMS.json', 'evidence-sha256.json'):
            files[f.name] = f
comparison = P / 'tail-read-comparison'
for directory, dirs, names in os.walk(comparison):
    dirs[:] = [d for d in dirs if not d.endswith('input')]
    for n in names:
        f = pathlib.Path(directory) / n
        assert not f.is_symlink(), str(f)
        files[str(f.relative_to(P))] = f
source_paths = [n for n in inventory if n.startswith('core/state/snapshots/history_account_delta')
                or n in ('core/state/snapshots/compressed_block.go',
                         'core/state/snapshots/compressed_block_footer.go',
                         'core/state/snapshots/history_stream_build.go')]
for n in source_paths:
    files['verified-source/' + n] = P / 'candidate-source' / n
for f in (P / 'candidate-source').rglob('offline*.go'):
    if f.is_file():
        files['harness-source/' + str(f.relative_to(P / 'candidate-source'))] = f
for f in (P / 'candidate-source/core/state/snapshots').glob('history_account_delta*_test.go'):
    files['unit-source/' + f.name] = f
checksums = {n:sha(f) for n, f in files.items()}
target = pathlib.Path('/tmp/gtron-account-delta-final-20261025-01.zip')
assert not target.exists(), 'preserve any prior final evidence'
with zipfile.ZipFile(target, 'x', zipfile.ZIP_DEFLATED) as z:
    for n, f in sorted(files.items()):
        z.write(f, n)
        assert sha(f) == checksums[n], 'evidence changed during pack: ' + n
    z.writestr('evidence-sha256.json', json.dumps(checksums, indent=2))
receipt = dict(utc=environment['utc'], bytes=target.stat().st_size,
              sha256=sha(target), members=len(files)+1, hashed_members=len(files),
              manifest='evidence-sha256.json', production_mutations=False,
              scope='All root lab reports, logs, scripts and comparison outputs; database trees and binaries excluded.')
pathlib.Path('/tmp/gtron-account-delta-final-20261025-01-receipt.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt, indent=2), flush=True)
