# Seal only observations and small authenticated metadata; never copy DB data.
import pathlib, json, hashlib, zipfile

root = pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01')
target = pathlib.Path('/tmp/AAA-gtron-first-cold-followup-20261028-01.zip')
status = json.loads((root/'metadata-repair-ablation/candidate-status.json').read_text())
assert status['complete'], status
files = {}
for name in ['live-census', 'live-census-2', 'fixed-census', 'account-gap-ablation', 'account-build-cost', 'metadata-repair-ablation']:
    for p in (root/name).rglob('*'):
        rel = p.relative_to(root)
        if any(x.startswith(('source-', 'input-')) for x in rel.parts):
            continue
        if p.is_file() and not p.is_symlink() and p.suffix in ['.json', '.jsonl', '.log', '.txt', '.pprof']:
            files[str(rel)] = p
original = pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01/final/indexed-input/state-snapshots')
for name, snap in [('fixed-original', original), ('metadata-base', root/'metadata-repair-ablation/input-base/state-snapshots'), ('metadata-candidate', root/'metadata-repair-ablation/input-candidate/state-snapshots')]:
    files[name+'/manifest.json'] = snap/'manifest.json'
    m = json.loads((snap/'manifest.json').read_text())
    for ref in m['segments']:
        if ref['kind'] == 'metadata-history-routing':
            files[name+'/'+ref['path']] = snap/ref['path']
with zipfile.ZipFile(str(target), 'w', zipfile.ZIP_DEFLATED) as archive:
    for name, p in sorted(files.items()):
        archive.write(str(p), name)
digest = hashlib.sha256()
with target.open('rb') as f:
    for chunk in iter(lambda: f.read(4 << 20), b''):
        digest.update(chunk)
result = {'path': str(target), 'size': target.stat().st_size, 'sha256': digest.hexdigest(), 'files': len(files)}
(root/'evidence-seal.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result, indent=2))
