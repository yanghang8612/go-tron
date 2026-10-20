"""Read-only source capture for a diagnostic clone, NOT an online backup API.

Only complete recovery + independent source-fold MuHash + history gates can
admit this candidate as benchmark input. No source lock, process signal, source
write, filesystem freeze or global cache operation is performed.
"""
import datetime, hashlib, json, os, pathlib, shutil, subprocess, traceback

ROOT = pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
SOURCE = pathlib.Path('/data/gtron/main/datadir/gtron')
ROOT.mkdir(exist_ok=True)
status = {'complete': False, 'admitted': False, 'production_mutations': False}

def identity():
    pid = int(subprocess.check_output(['systemctl', 'show', 'gtron', '-p', 'MainPID']).decode().split('=')[1])
    return {'pid': pid, 'sha256': hashlib.sha256(pathlib.Path(f'/proc/{pid}/exe').read_bytes()).hexdigest()}

def save():
    status['utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (ROOT/'capture-status.json').write_text(json.dumps(status, indent=2))

def prefix_copy(src, dst):
    # A private copy, never a link to a mutable manifest/log/legacy index.
    with src.open('rb', buffering=0) as inp, dst.open('xb', buffering=0) as out:
        before = os.fstat(inp.fileno())
        left = before.st_size
        digest = hashlib.sha256()
        while left:
            data = inp.read(min(left, 4 << 20))
            if not data:
                raise RuntimeError(f'short captured prefix: {src}')
            out.write(data); digest.update(data); left -= len(data)
    return {'path': str(dst.relative_to(ROOT)), 'bytes': before.st_size,
            'source_inode': before.st_ino, 'sha256': digest.hexdigest()}

def link_tables(src, dst):
    linked = 0
    for file in sorted(src.glob('*.sst')):
        target = dst/file.name
        if target.exists():
            continue
        try:
            os.link(file, target); linked += 1
        except FileNotFoundError:
            # Recovery checks the manifest's exact live set. Obsolete races
            # are harmless; a missing referenced table rejects the attempt.
            pass
    return linked

def database(name, attempt):
    src, dst = SOURCE/name, attempt/name
    dst.mkdir()
    link_tables(src, dst)
    current = (src/'CURRENT').read_bytes()
    manifest = current.decode().strip()
    assert manifest.startswith('MANIFEST-') and '/' not in manifest
    receipts = [prefix_copy(src/manifest, dst/manifest)]
    (dst/'CURRENT').write_bytes(current)
    for file in sorted(src.glob('OPTIONS-*')):
        receipts.append(prefix_copy(file, dst/file.name))
    # Open all handles before reading. Old logs precede the newest captured
    # WAL and must replay completely; Pebble allows a torn tail only in last.
    handles = [(f.name, f.open('rb', buffering=0)) for f in sorted(src.glob('*.log'))]
    try:
        for name_, inp in reversed(handles):
            st = os.fstat(inp.fileno()); left = st.st_size; digest = hashlib.sha256()
            with (dst/name_).open('xb', buffering=0) as out:
                while left:
                    data = inp.read(min(left, 4 << 20))
                    if not data: raise RuntimeError('short WAL prefix')
                    out.write(data); digest.update(data); left -= len(data)
            receipts.append({'path': str((dst/name_).relative_to(ROOT)), 'bytes': st.st_size,
                             'source_inode': st.st_ino, 'sha256': digest.hexdigest()})
    finally:
        for _, inp in handles: inp.close()
    link_tables(src, dst)
    if (src/'CURRENT').read_bytes() != current:
        raise RuntimeError('manifest rotated during capture; reject attempt')
    (attempt/(name+'-capture.json')).write_text(json.dumps(receipts, indent=2))
    return {'tables': len(list(dst.glob('*.sst'))), 'logical_file_bytes': sum(f.stat().st_size for f in dst.iterdir())}

def immutable_cold(attempt):
    src, dst = SOURCE/'state-snapshots', attempt/'state-snapshots'
    dst.mkdir()
    data = (src/'manifest.json').read_bytes(); manifest = json.loads(data)
    # Retired refs are not part of this fixed reader's visible manifest.
    manifest['retired'] = []
    for ref in manifest['segments']:
        path = pathlib.Path(ref['path']); assert not path.is_absolute() and '..' not in path.parts
        target = dst/path; target.parent.mkdir(parents=True, exist_ok=True)
        os.link(src/path, target)
        assert target.stat().st_size == ref['size']
    (dst/'manifest.json').write_text(json.dumps(manifest))
    (attempt/'source-cold-manifest.json').write_bytes(data)
    return {'generation': manifest.get('generation'), 'visible_tx_end': manifest['visibleTxEnd'],
            'files': len(manifest['segments']), 'source_manifest_sha256': hashlib.sha256(data).hexdigest()}

def ancient(attempt):
    src, dst = SOURCE/'ancient', attempt/'ancient'; dst.mkdir()
    receipts = []
    # Legacy mutable files are independently copied. V2 segments and published
    # tx-index runs are immutable; manifests are copied, locks never shared.
    for directory, subdirs, files in os.walk(src):
        subdirs[:] = [d for d in subdirs if d not in ('etl', 'tmp')]
        rel = pathlib.Path(directory).relative_to(src)
        destdir = dst/rel; destdir.mkdir(parents=True, exist_ok=True)
        for name in sorted(files):
            if name in ('LOCK', 'FLOCK') or '.tmp' in name: continue
            file = pathlib.Path(directory)/name; target = destdir/name
            if rel == pathlib.Path('.') or name.endswith('.json'):
                receipts.append(prefix_copy(file, target))
            else:
                os.link(file, target)
    (attempt/'ancient-capture.json').write_text(json.dumps(receipts, indent=2))
    return {'files': sum(1 for f in dst.rglob('*') if f.is_file())}

try:
    status['before_identity'] = identity(); save()
    assert status['before_identity']['sha256'] == '5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4'
    attempt = ROOT/'capture-1'; attempt.mkdir()
    status['phase'] = 'chaindata'; save()
    status['chaindata'] = database('chaindata', attempt)
    status['phase'] = 'history'; save()
    status['history'] = database('history', attempt)
    status['phase'] = 'cold'; save()
    status['cold'] = immutable_cold(attempt)
    status['phase'] = 'ancient'; save()
    status['ancient'] = ancient(attempt)
    status['after_identity'] = identity(); assert status['after_identity'] == status['before_identity']
    status.update(complete=True, phase='captured-not-admitted'); save()
except BaseException as exc:
    status.update(phase='rejected', error=repr(exc), traceback=traceback.format_exc()); save()
    raise
