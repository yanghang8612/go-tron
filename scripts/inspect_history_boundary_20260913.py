#!/usr/bin/env python3
"""Reuse the verified offline inspector for 64 exact blocks across one bucket edge.

prepare only verifies existing native artifacts and records an explicit interval.
inspect reuses the pinned stop/probe/reap/finally-restore transaction. It never
changes a unit, executable, hold, checkout or business data. No automatic retry.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
import types
import urllib.request

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260913-history-boundary')
DIAGNOSTIC_RELEASE = Path('/data/gtron/releases/20260913-history-prev')
BINARY = DIAGNOSTIC_RELEASE / 'gtron-inspect'
CURRENT_RELEASE = Path('/data/gtron/releases/20260913-history-pack-gate')
CURRENT_EXE = str(CURRENT_RELEASE / 'gtron')
CURRENT_SOURCE = 'b1305f553b5639d90eb2f58c6a5a1b23b9b025b5'
CURRENT_PID, CURRENT_TICKS = 22687, 4503185044
CURRENT_START_NANO = 1789297356925847442
DIAGNOSTIC_SOURCE = '3055eded341f310bb080164cf96e4d2f3447160e'
SOURCE_BASE = 'fdfe853236445ca77c2d7a9553c1d4e172ff9a43'
BUILDER_REVISION = '8f3b8c7d48a6e8076c81b8e49d83751945715f3d'
BUILDER_PATH = 'scripts/inspect_history_prev_20260913.py'
BUILDER_SHA = '75d2e44cd78ad26149a69614e0ac6c5557db07f7491b97eb84e0c1e55b4da4ae'
SCRIPT_PATH = 'scripts/inspect_history_boundary_20260913.py'
RESULT = RELEASE / 'result'
PACKS = RESULT / 'packs'
SAMPLES, BUCKET_BLOCKS = 64, 1024
ENCODED_BYTES, DECODED_BYTES, ROWS = 256 << 20, 1 << 30, 2000000
PROBE_TIMEOUT = 120
COOPERATIVE_DURATION = '90s'  # Leave cancellation/reap margin inside the outer wait.
SMALL_METRICS = tuple('state/history/changeset/block_pack/small_chunk/' + field
                      for field in ('attempts', 'selected', 'candidate_saved_bytes', 'work_nanos'))


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_regular(path, limit=8 << 20):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, 'invalid bounded file: ' + str(path))
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'file grew beyond limit: ' + str(path))
    return data


def read_json(path, limit=8 << 20):
    return json.loads(read_regular(path, limit).decode('utf-8'))


def git_blob(revision, path):
    return subprocess.check_output(['git', 'show', revision + ':' + path], cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, timeout=30)


def load_modules(helper_path=None):
    blob = git_blob(BUILDER_REVISION, BUILDER_PATH)
    require(sha(blob) == BUILDER_SHA, 'pinned inspection helper blob differs')
    i = types.ModuleType('pinned_boundary_inspection')
    i.__file__ = __file__
    exec(compile(blob, BUILDER_PATH, 'exec'), i.__dict__)
    if helper_path is not None:  # Local test injection only; no CLI path override.
        i.HELPER = Path(helper_path)
    h = i.load_helper()  # Verifies the original a0fc... range helper before adaptation.
    h.RELEASE = RELEASE
    return i, h


def verify_native_record(h, release, binary, source, gate=False):
    path = release / ('gate-prepared.json' if gate else 'prepared.json')
    data = read_regular(path)
    record = json.loads(data.decode('utf-8'))
    require(record.get('prepared') is True and record.get('source_commit') == source and
            record.get('base_commit') == SOURCE_BASE, 'native prepare/source identity differs')
    if gate:
        require(record.get('gate_prepared') is True and record.get('builder_sha256') == BUILDER_SHA,
                'production gate prepare attestation missing')
        require(read_regular(release / 'source-commit').decode().strip() == source, 'production source marker differs')
        require(read_regular(release / 'SHA256SUMS').decode() == record.get('binary_sha256', '') + '  gtron\n',
                'production checksum marker differs')
        require(h.file_sha(release / 'prepared.json') == record.get('builder_record_sha256') and
                h.file_sha(release / 'native-history-tests.log') == record.get('history_tests_sha256'),
                'production native test/build evidence differs')
    require(record.get('helper_sha256') == 'a0fcf33a7ccf09ad45dab5d8bc5f101955d36e8193d022854bf2f0568c6ff70b',
            'native range helper attestation differs')
    expected = record.get('binary_sha256', '')
    require(re.fullmatch('[0-9a-f]{64}', expected) is not None and binary.is_file() and not binary.is_symlink(),
            'existing native binary/checksum missing')
    require(h.file_sha(binary) == expected, 'existing native binary differs from successful prepare')
    ops = record.get('script_commit', '')
    require(re.fullmatch('[0-9a-f]{40}', ops) is not None, 'native ops commit missing')
    old_script = 'scripts/deploy_history_pack_gate_20260913.py' if gate else BUILDER_PATH
    require(sha(git_blob(ops, old_script)) == record.get('script_sha256'), 'native ops blob differs from Git')
    changes, _ = h.run(['git', 'diff', '--name-status', '--no-renames', SOURCE_BASE, source, '--'], cwd=REPO)
    paths = set()
    for row in changes.splitlines():
        item = row.split('\t')
        require(len(item) == 2 and item[0] in ('A', 'M'), 'unexpected frozen native source diff')
        if item[1].startswith('docs/') and item[1].endswith('.md'):
            continue
        paths.add(item[1])
    manifest = record.get('manifest', {})
    require(paths and set(manifest) == paths and set(record.get('changed_files', [])) == paths,
            'native source manifest scope differs from frozen Git source')
    for name in sorted(paths):
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe source path')
        require(sha(git_blob(source, name)) == manifest[name], 'native source blob differs: ' + name)
        require(sha(read_regular(release / 'source' / name)) == manifest[name], 'native extracted source differs: ' + name)
    return {'source_commit': source, 'binary_sha256': expected, 'prepared_sha256': sha(data),
            'script_commit': ops, 'source_manifest': manifest}


def reuse_identity(h):
    return {'diagnostic': verify_native_record(h, DIAGNOSTIC_RELEASE, BINARY, DIAGNOSTIC_SOURCE),
            'production': verify_native_record(h, CURRENT_RELEASE, Path(CURRENT_EXE), CURRENT_SOURCE, True)}


def select_interval(head):
    require(type(head) is int and 1055 <= head < (1 << 64), 'invalid head for a complete 64-block bucket edge')
    boundary = ((head - 31) // BUCKET_BLOCKS) * BUCKET_BLOCKS
    return {'observed_head': head, 'bucket_boundary': boundary, 'from_block': boundary - 32,
            'to_block': boundary + 31, 'samples': list(range(boundary - 32, boundary + 32))}


def probe_argv(args):
    require(type(args.from_block) is int and type(args.to_block) is int and
            args.export_packs is True and args.to_block - args.from_block == SAMPLES - 1 and
            args.from_block >= 0 and (args.from_block + 32) % BUCKET_BLOCKS == 0 and args.to_block < (1 << 64),
            'exact 32+32 consecutive block export required')
    return [str(BINARY), 'db', 'inspect-history-prev', '--datadir', '/data/gtron/main/datadir',
            '--db.cache', '64', '--db.handles', '128', '--from-block', str(args.from_block),
            '--to-block', str(args.to_block), '--seed', '20260913', '--samples', str(SAMPLES),
            '--max-encoded-bytes', str(ENCODED_BYTES), '--max-decoded-bytes', str(DECODED_BYTES),
            '--max-rows', str(ROWS), '--max-duration', COOPERATIVE_DURATION, '--export-packs', str(PACKS)]


def small_metric_values(metrics):
    values = {name: metrics.get(name, {}).get('count') for name in SMALL_METRICS}
    require(all(type(value) is int and value >= 0 for value in values.values()), 'small-chunk counter contract differs')
    return values


def read_local_metrics():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:6062/debug/metrics', timeout=10) as response:
        return json.loads(response.read(8 << 20).decode('utf-8'))['metrics']


def configure_modules(i, h, pins):
    i.RELEASE, i.RESULT, i.PACKS = RELEASE, RESULT, PACKS
    i.BINARY, i.SOURCE = BINARY, DIAGNOSTIC_RELEASE / 'source'
    i.CURRENT_EXE, i.CURRENT_SOURCE = CURRENT_EXE, CURRENT_SOURCE
    i.CURRENT_PID, i.CURRENT_TICKS = CURRENT_PID, CURRENT_TICKS
    i.CURRENT_SHA = pins['production']['binary_sha256']
    i.PROBE_TIMEOUT, i.SAMPLES = PROBE_TIMEOUT, SAMPLES
    i.probe_argv = probe_argv
    h.OLD_EXE, h.OLD_SHA, h.BINARY = CURRENT_EXE, i.CURRENT_SHA, Path(CURRENT_EXE)
    h.OLD_PID, h.OLD_START_TICKS = CURRENT_PID, CURRENT_TICKS
    original_normalizer = i.normalize_exec_start
    def normalize(value):
        if isinstance(value, str):
            value = re.sub(r'(;\s*status=[0-9]+)/[A-Z][A-Z0-9_]*(\s*\})$', r'\1\2', value)
        return original_normalizer(value)
    i.normalize_exec_start = normalize
    def expected_queue(exe):
        require(exe == CURRENT_EXE, 'unexpected executable for existing five flags')
        return '1'
    h.expected_history_queue_environment = expected_queue
    original_runtime = h.verify_observer_runtime
    def runtime(pid, exe):
        result = original_runtime(pid, exe)
        metrics = read_local_metrics()
        process = metrics.get('process/start/unix_nano', {}).get('value')
        require(type(process) is int and process == result['process_start_unix_nano'], 'process changed between metric checks')
        result['small_chunk_encoding_attempts'] = small_metric_values(metrics)
        return result
    h.verify_observer_runtime = runtime
    original_snapshot = i.snapshot
    def snapshot(helper, initial=True):
        value = original_snapshot(helper, initial)
        proc = value['process']
        value['runtime'] = h.verify_observer_runtime(proc['pid'], CURRENT_EXE)
        if initial:
            require(value['runtime']['process_start_unix_nano'] == CURRENT_START_NANO, 'original process metric identity differs')
        now = h.show(i.SERVICE)
        identity = h.process_identity(int(now.get('MainPID', '0')))
        require(now.get('ActiveState') == 'active' and
                (identity['pid'], identity['start_ticks']) == (proc['pid'], proc['start_ticks']), 'process changed during snapshot')
        return value
    i.snapshot = snapshot
    i.verify_prepared = lambda helper, record: verify_prepared(i, helper, record, pins)


def validate_ops(i, h, revision):
    require(re.fullmatch('[0-9a-f]{40}', revision or '') is not None, 'exact ops commit required')
    require(i.resolve(h, revision) == revision and i.resolve(h, 'refs/remotes/origin/master') == revision,
            'fetched ops commit differs')
    require(i.resolve(h, 'HEAD') == i.CHECKOUT, 'production checkout changed')
    require(h.run(['git', 'merge-base', '--is-ancestor', CURRENT_SOURCE, revision], cwd=REPO, check=False)[1] == 0,
            'ops commit is not descended from the running source')
    dirty, _ = h.run(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=REPO)
    require(all(row[3:] == 'third_party/librustzcash' for row in dirty.splitlines()), 'unexpected tracked checkout changes')
    require(sha(git_blob(revision, SCRIPT_PATH)) == h.file_sha(__file__), 'executing ops script differs from Git')


def verify_prepared(i, h, record, pins):
    require(record.get('prepared') is True and record.get('reused') == pins and
            record.get('builder_sha256') == BUILDER_SHA and record.get('helper_sha256') == i.HELPER_SHA,
            'successful reuse prepare identity differs')
    require(record.get('script_sha256') == h.file_sha(__file__) and
            sha(git_blob(record['script_commit'], SCRIPT_PATH)) == record['script_sha256'], 'reviewed ops script changed')
    require(record.get('interval') == select_interval(record.get('interval', {}).get('observed_head')),
            'prepared block interval was changed')
    require(reuse_identity(h) == pins, 'reused binary/source evidence changed')


def prepare(i, h, args, pins):
    validate_ops(i, h, args.script_revision)
    require(not RELEASE.is_symlink(), 'release cannot be a symlink')
    RELEASE.mkdir(mode=0o755, parents=True, exist_ok=True)
    with open(str(RELEASE / '.prepare.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (RELEASE / 'prepared.json').exists():
            record = h.load_json('prepared.json')
            verify_prepared(i, h, record, pins)
            require(record['script_commit'] == args.script_revision, 'prepared ops revision differs')
            i.snapshot(h)
            return {'prepared': True, 'already_prepared': True, 'prepared_sha256': h.file_sha(RELEASE / 'prepared.json')}
        require(not RESULT.exists(), 'previous diagnostic attempt exists')
        before = i.snapshot(h)
        interval = select_interval(h.wallet_head())
        help_text, _ = h.run([str(BINARY), 'db', 'inspect-history-prev', '--help'], timeout=30, log='reused-inspect-help.txt')
        require(all(flag in help_text for flag in ('--from-block', '--to-block', '--samples', '--export-packs',
                                                   '--max-duration', '--max-encoded-bytes', '--max-decoded-bytes')),
                'reused inspection CLI contract differs')
        after = i.snapshot(h)
        i.same_configuration(h, before, after)
        record = {'prepared': True, 'prepared_at': time.time(), 'script_commit': args.script_revision,
                  'script_sha256': h.file_sha(__file__), 'builder_sha256': BUILDER_SHA,
                  'helper_sha256': i.HELPER_SHA, 'reused': pins, 'interval': interval,
                  'preflight': after, 'diagnostic_only': True, 'build_performed': False}
        verify_prepared(i, h, record, pins)
        h.save_json('prepared.json', record)
        return {'prepared': True, 'prepared_sha256': h.file_sha(RELEASE / 'prepared.json'),
                'interval': interval, 'reused': pins, 'build_performed': False}


def validate_complete_samples(value, interval):
    detail = value.get('inspection', {})
    require(detail.get('complete') is True and detail.get('stop_reason') == 'complete' and
            type(detail.get('packs_complete')) is int and detail['packs_complete'] == SAMPLES and
            type(detail.get('missing_packs')) is int and detail['missing_packs'] == 0,
            'all 64 physical seq=0 packs must be complete; preserve missing/partial evidence')
    expected_options = {'from_block': interval['from_block'], 'to_block': interval['to_block'],
                        'samples': SAMPLES, 'seed': 20260913, 'max_rows': ROWS,
                        'max_encoded_bytes': ENCODED_BYTES, 'max_decoded_bytes': DECODED_BYTES,
                        'max_duration_ns': 90000000000}
    options = detail.get('options', {})
    require(all(type(options.get(key)) is int and options[key] == value for key, value in expected_options.items()),
            'inspection options differ from the exact bounded command')
    samples = detail.get('samples', [])
    require([item.get('block') for item in samples] == interval['samples'] and
            all(type(item.get('block')) is int and item.get('status') == 'complete' and
                item.get('codec') in ('raw', 'snappy1', 'chunks2') for item in samples),
            'unexpected sampled heights, codecs or completion state')
    for field, limit in (('encoded_bytes_read', ENCODED_BYTES), ('encoded_bytes_accepted', ENCODED_BYTES),
                         ('decoded_bytes_reserved', DECODED_BYTES), ('rows', ROWS)):
        number = detail.get(field)
        require(type(number) is int and 0 <= number <= limit, 'inspection budget/type differs: ' + field)
    return {'packs_complete': SAMPLES, 'encoded_bytes': detail['encoded_bytes_accepted'],
            'decoded_bytes': detail['decoded_bytes_reserved'], 'rows': detail['rows']}


def verify_export_after_restore(interval):
    require(not PACKS.is_symlink() and PACKS.is_dir() and stat.S_IMODE(PACKS.stat().st_mode) == 0o700,
            'export directory scope/mode differs')
    manifest_data = read_regular(PACKS / 'manifest.json')
    require(stat.S_IMODE((PACKS / 'manifest.json').lstat().st_mode) == 0o600, 'manifest mode differs')
    manifest = json.loads(manifest_data.decode('utf-8'))
    require(type(manifest.get('version')) is int and manifest['version'] == 1 and manifest.get('inspection_complete') is True and
            manifest.get('inspection_stop_reason') == 'complete', 'export manifest is incomplete')
    entries = manifest.get('entries', [])
    require([entry.get('block') for entry in entries] == interval['samples'] and
            all(type(entry.get('block')) is int for entry in entries), 'export heights differ')
    expected_files = {'manifest.json'}
    total_encoded, total_decoded = 0, 0
    for entry in entries:
        filename = '{0:020d}.pack'.format(entry['block'])
        require(entry.get('file') == filename and entry.get('codec') in ('raw', 'snappy1', 'chunks2'), 'invalid export filename/codec')
        n, d = entry.get('encoded_bytes'), entry.get('decoded_bytes')
        require(type(n) is int and type(d) is int and 0 < n <= ENCODED_BYTES - total_encoded and
                0 < d <= DECODED_BYTES - total_decoded, 'export byte budget/type differs')
        data = read_regular(PACKS / filename, n)
        require(len(data) == n and sha(data) == entry.get('sha256'), 'export bytes/checksum differ')
        info = (PACKS / filename).lstat()
        require(stat.S_IMODE(info.st_mode) == 0o600, 'export file mode differs')
        total_encoded += n
        total_decoded += d
        expected_files.add(filename)
    require({item.name for item in PACKS.iterdir()} == expected_files, 'unexpected export directory contents')
    return {'verified': True, 'files': len(entries), 'encoded_bytes': total_encoded,
            'decoded_bytes': total_decoded, 'manifest_sha256': sha(manifest_data)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'inspect'))
    parser.add_argument('--script-revision')
    parser.add_argument('--prepared-sha')
    args = parser.parse_args()
    require(os.geteuid() == 0, 'explicit root execution is required')
    i, h = load_modules()
    pins = reuse_identity(h)
    configure_modules(i, h, pins)
    if args.mode == 'prepare':
        result = prepare(i, h, args, pins)
    else:
        require(re.fullmatch('[0-9a-f]{64}', args.prepared_sha or '') is not None, 'reviewed prepared SHA256 required')
        record = h.load_json('prepared.json')
        verify_prepared(i, h, record, pins)
        require(h.file_sha(RELEASE / 'prepared.json') == args.prepared_sha, 'operator-reviewed record differs')
        interval = record['interval']
        args.from_block, args.to_block, args.export_packs = interval['from_block'], interval['to_block'], True
        probe_argv(args)
        class BoundaryOps(i.LiveOps):
            def probe(self):
                outcome = super().probe()
                if outcome.get('ok'):
                    outcome['exact_samples'] = validate_complete_samples(read_json(RELEASE / 'inspection-stdout.json', 64 << 20), interval)
                return outcome
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, i.interrupted)
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = i.inspect_transaction(BoundaryOps(h, args))
        if result.get('ok') and result.get('restored'):
            try:
                result['export_verification'] = verify_export_after_restore(interval)
            except BaseException as exc:
                result['ok'], result['export_error'] = False, repr(exc)
            h.save_json('inspection-state.json', result)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result.get('prepared') or result.get('ok') else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            raise
        print(json.dumps({'ok': False, 'error': repr(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
