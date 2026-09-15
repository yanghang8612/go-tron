#!/usr/bin/env python3
"""Compare baseline/candidate binaries on the same authenticated private history.

A/B/B/A, three complete serial builds per process, then one separate profile per
binary. Only the binary varies. Neither publication nor production data is used.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import statistics
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
BASE = 'e913c72d274d3f76f07b61f1d44ae1df986cbee9'
HELPER_PATH = 'scripts/benchmark_history_pipeline_20260915.py'
HELPER_SHA = '00ba99f8a513da210698a7dc897ffb2a4dea5c7a4d4ad673321fdee3bede404b'
ORDER = ('baseline', 'candidate', 'candidate', 'baseline')


def load_helper():
    data = subprocess.check_output(['git', 'show', BASE + ':' + HELPER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != HELPER_SHA:
        raise RuntimeError('reviewed helper bytes differ')
    helper = types.ModuleType('history_cdc_reuse_native_helpers')
    helper.__file__ = str(Path(__file__).resolve())
    exec(compile(data, HELPER_PATH, 'exec'), helper.__dict__)
    return helper


def run_matrix(helper, args, source, summary):
    for index, (kind, profile) in enumerate([(kind, False) for kind in ORDER] +
                                           [('baseline', True), ('candidate', True)]):
        binary = getattr(args, kind + '_binary')
        digest = getattr(args, kind + '_sha')
        options = types.SimpleNamespace(binary=binary, binary_sha=digest,
                                        input_dir=args.input_dir, manifest_sha=args.manifest_sha,
                                        output_dir=args.output_dir)
        # The reviewed helper's serial mode explicitly passes pipeline=false.
        item = helper.run_one(options, index, 'serial', source, profile)
        item['binary_kind'], item['binary_sha256'] = kind, digest
        if summary['runs']:
            first = summary['runs'][0]
            helper.require(item['source_digest'] == first['source_digest'] and item['refs'] == first['refs'],
                           'baseline/candidate/profile output or logical contents differ')
        summary['runs'].append(item)
        print(json.dumps({'finished': kind, 'index': index, 'profile': profile, 'equivalent': True}), flush=True)
    results = {}
    for kind in ('baseline', 'candidate'):
        rows = [i for r in summary['runs'] if r['binary_kind'] == kind and not r['profile'] for i in r['iterations']]
        helper.require(len(rows) == 6, 'missing primary build samples')
        walls = [r['build_wall_nanos'] / 1e9 for r in rows]
        median = statistics.median(walls)
        results[kind] = {'wall_seconds': walls, 'median_wall_seconds': median,
                         'blocks_per_second_at_median_wall': 16 / median,
                         'output_bytes': [r['output_bytes'] for r in rows],
                         'allocated_bytes': [r['build_allocated_bytes'] for r in rows]}
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for kind in ('baseline', 'candidate'):
        p.add_argument('--' + kind + '-binary', type=Path, required=True)
        p.add_argument('--' + kind + '-sha', required=True)
    p.add_argument('--input-dir', type=Path, required=True)
    p.add_argument('--manifest-sha', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    args = p.parse_args()
    if not sys.platform.startswith('linux') or os.geteuid() == 0:
        raise RuntimeError('native Linux execution as private capture owner required')
    h = load_helper()
    for sig in h.SIGNALS:
        signal.signal(sig, h.interrupted)
    binaries = {'baseline': args.baseline_binary, 'candidate': args.candidate_binary}
    for path in tuple(binaries.values()) + (args.input_dir, args.output_dir):
        h.require(path.is_absolute() and path == path.resolve(), 'absolute non-symlink paths required')
    pins = {kind: getattr(args, kind + '_sha') for kind in binaries}
    for kind, binary in binaries.items():
        h.require(h.sha(pins[kind]) and h.file_sha(binary) == pins[kind], 'binary SHA differs: ' + kind)
    h.require(pins['baseline'] != pins['candidate'], 'distinct baseline/candidate binaries required')
    info = args.input_dir.stat()
    h.require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) & 0o077 == 0,
              'input must be private and owned by this user')
    data = h.read_file(args.input_dir / 'manifest.json', 128 << 20)
    h.require(h.sha(args.manifest_sha) and hashlib.sha256(data).hexdigest() == args.manifest_sha, 'manifest SHA differs')
    source = json.loads(data.decode('utf-8'))
    h.check_export(source)
    original = Path(source['source_chaindata'])
    h.require(original.is_absolute() and original.name == 'chaindata' and original.parent.name == 'gtron',
              'unrecognized production data path')
    for excluded in (original.parent.parent.resolve(), args.input_dir):
        h.require(args.output_dir != excluded and excluded not in args.output_dir.parents, 'output overlaps protected input')
    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    summary = {'complete': False, 'runs': [], 'started_utc': h.utc(), 'binary_pins': pins,
               'binary_paths': {kind: str(path) for kind, path in binaries.items()},
               'planned_order': list(ORDER) + ['baseline-profile', 'candidate-profile'],
               'script_sha256': h.file_sha(__file__), 'helper_sha256': HELPER_SHA,
               'input_manifest_sha256': args.manifest_sha,
               'comparison_scope': 'Two fixed binaries; owned/auto/serial GOMAXPROCS=4 GOMEMLIMIT=10GiB, same 16 original blocks. '
                                   'ABBA six samples per binary; profiles separate. Complete trio, no publication/pruning/online-rate claim.',
               'resource_scope': h.RESOURCE_SCOPE}
    before = None
    try:
        before = h.private_manifest(args.input_dir)
        h.save(args.output_dir / 'input-files-before.json', before)
        summary['results'] = run_matrix(h, args, source, summary)
        summary['complete'] = True
    except BaseException as error:
        summary['error'] = repr(error)
        raise
    finally:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, h.SIGNALS)
        try:
            try:
                after = h.private_manifest(args.input_dir)
                h.save(args.output_dir / 'input-files-after.json', after)
                summary['input_unchanged'] = before is not None and before == after
            except Exception as error:
                summary['input_unchanged'], summary['input_after_error'] = False, repr(error)
            try:
                summary['binaries_unchanged'] = all(h.file_sha(binary) == pins[kind] for kind, binary in binaries.items())
            except Exception as error:
                summary['binaries_unchanged'], summary['binary_after_error'] = False, repr(error)
            summary['complete'] = summary['complete'] and summary['input_unchanged'] and summary['binaries_unchanged']
            summary['finished_utc'] = h.utc()
            h.save(args.output_dir / 'summary.json', summary)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    h.require(summary['complete'], 'input or binary after-check failed')
    print(json.dumps(summary['results'], sort_keys=True), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete': False, 'error': repr(error)}), file=sys.stderr)
        sys.exit(1)
