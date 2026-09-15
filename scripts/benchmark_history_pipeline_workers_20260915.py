#!/usr/bin/env python3
"""Two repeats of 2/4/8 shared-read workers on one unchanged private 16-block input.

This is an offline single-trio replay: 2,4,8 then 8,4,2, one build per command.
The previously reviewed file/process/manifest helpers are loaded from pinned
local Git bytes without running their main. No service or source mutation runs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import statistics
import subprocess
import sys
import time
import types

REPO = Path('/data/gtron/go-tron')
HELPER_REVISION = 'e913c72d274d3f76f07b61f1d44ae1df986cbee9'
HELPER_PATH = 'scripts/benchmark_history_pipeline_20260915.py'
HELPER_SHA = '00ba99f8a513da210698a7dc897ffb2a4dea5c7a4d4ad673321fdee3bede404b'
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
WORKERS = (2, 4, 8)
ORDER = WORKERS + tuple(reversed(WORKERS))
COMPARISON_SCOPE = ('Same original 16 blocks, single trio, owned Get, auto compression, pipeline enabled, '
                    'GOMAXPROCS=4 and GOMEMLIMIT=10GiB throughout. Only shared_read_workers changes. '
                    'Order 2,4,8,8,4,2 gives two one-build samples per configuration; no profile runs. '
                    'Fixed decoded-materialization budget is not a process memory cap. More workers '
                    'does not promise that all slots can admit large packs. OS/read caches and order '
                    'still affect results; two samples are exploratory, not statistical proof. '
                    'No publication, pruning or sustainable online throughput claim.')


def load_helpers():
    data = subprocess.check_output(['git', 'show', HELPER_REVISION + ':' + HELPER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != HELPER_SHA:
        raise RuntimeError('reviewed pipeline helper Git bytes differ')
    module = types.ModuleType('history_pipeline_workers_helpers')
    module.__file__ = str(Path(__file__).resolve())
    exec(compile(data, HELPER_PATH, 'exec'), module.__dict__)
    return module


def check_report(h, report, args, output, workers, source):
    require, uint, check_digest = h.require, h.uint, h.check_digest
    count, enabled, profile = 1, True, False
    require(type(workers) is int and workers in WORKERS, 'unsupported worker count')
    require(uint(report.get('version'), 'report version') == 1 and report.get('complete') is True and
            report.get('physical_verified') is True and not report.get('error') and
            report.get('manifest_file_sha256') == args.manifest_sha and report.get('export') == source['export'],
            'incomplete or wrong physical input report')
    options = report.get('options', {})
    require(uint(options.get('iterations'), 'option iterations') == count and
            uint(options.get('max_duration_ns'), 'option duration') == 300000000000 and
            options.get('copy_mode') == 'owned' and options.get('compression_format') == 'auto' and
            options.get('shared_read_pipeline') is enabled and
            uint(options.get('shared_read_workers'), 'option workers') == workers and
            options.get('cpu_profile') is profile and
            options.get('input_dir') == str(args.input_dir) and options.get('output_dir') == str(output),
            'wrong benchmark options')
    require(report.get('compression_format') == 'auto' and uint(report.get('gomaxprocs'), 'gomaxprocs') == 4 and
            report.get('go_version') == 'go1.25.5' and report.get('goos') == 'linux' and report.get('goarch') == 'amd64',
            'wrong native runtime')
    source_digest = check_digest(report.get('source_digest'))
    require(source_digest['tx_ranges'] == 16, 'wrong full source tx-range count')
    uint(report.get('source_verification_wall_nanos'), 'source verification wall')
    iterations = report.get('iterations', [])
    require(len(iterations) == count, 'wrong iteration count')
    first_refs = None
    for number, item in enumerate(iterations, 1):
        require(uint(item.get('iteration'), 'iteration') == number and item.get('equivalent') is True and
                not item.get('error') and check_digest(item.get('digest')) == source_digest,
                'incomplete or unequal hot/cold iteration')
        for name in ('build_wall_nanos', 'build_allocated_bytes', 'build_allocations', 'build_gc_cycles',
                     'output_bytes', 'verification_wall_nanos'):
            uint(item.get(name), name, positive=name in ('build_wall_nanos', 'output_bytes'))
        refs = item.get('refs', [])
        require(len(refs) == 3 and {r.get('kind') for r in refs} == {'history', 'accessor', 'inverted'}, 'missing trio refs')
        seen, total = set(), 0
        for ref in refs:
            require(ref.get('dataset') == 'state-domain-change' and
                    uint(ref.get('fromTxNum'), 'ref fromTxNum') == source['export']['from_tx_num'] and
                    uint(ref.get('toTxNum'), 'ref toTxNum') == source['export']['to_tx_num'], 'wrong trio range or dataset')
            path = ref.get('path')
            require(type(path) is str and path and not Path(path).is_absolute() and '..' not in Path(path).parts and
                    str(Path(path)) == path and path not in seen, 'unsafe or duplicate trio path')
            seen.add(path)
            require(type(ref.get('checksum')) is str and re.fullmatch('sha256:[0-9a-f]{64}', ref['checksum']) is not None,
                    'invalid trio checksum')
            total += uint(ref.get('size'), 'ref size', positive=True)
        require(item['output_bytes'] == total, 'output byte total differs')
        if first_refs is None:
            first_refs = refs
        require(refs == first_refs, 'iteration trio bytes differ')
    require(not report.get('cpu_profile_sha256'), 'unexpected profiled sample')
    return iterations


def run_one(h, args, index, workers, source):
    label = '{:02d}-workers-{}'.format(index, workers)
    output = args.output_dir / label
    command = [str(args.binary), 'db', 'benchmark-history-cold', '--input-dir', str(args.input_dir),
               '--output-dir', str(output), '--copy-mode', 'owned', '--compression-format', 'auto',
               '--shared-read-pipeline=true', '--shared-read-workers', str(workers),
               '--iterations', '1', '--max-duration', '5m']
    env = {'PATH': '/data/go/bin:/usr/local/bin:/usr/bin:/bin', 'LANG': 'C',
           'HOME': os.environ['HOME'], 'GOMAXPROCS': '4', 'GOMEMLIMIT': '10GiB'}
    record = {'label': label, 'workers': workers, 'complete': False, 'started_utc': h.utc(),
              'command': command, 'environment': env, 'returncode': None,
              'resource_scope': h.RESOURCE_SCOPE, 'usage_before': h.usage()}
    started, child = time.monotonic(), None
    stdout_path = args.output_dir / (label + '.stdout.json')
    stderr_path = args.output_dir / (label + '.stderr.log')
    try:
        h.require(type(workers) is int and workers in WORKERS, 'unsupported worker count')
        h.require(h.file_sha(args.binary) == args.binary_sha, 'binary changed before run')
        with open(str(stdout_path), 'xb') as stdout, open(str(stderr_path), 'xb') as stderr:
            previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
            try:
                try:
                    child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=stdout,
                                             stderr=stderr, start_new_session=True,
                                             preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous))
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous)
                record['returncode'] = child.wait(timeout=330)
            finally:
                h.terminate(child)
                if child is not None:
                    record['returncode'] = child.returncode
        h.require(record['returncode'] == 0, 'cold workers diagnostic failed: ' + label)
        report_path = output / 'report.json'
        report = json.loads(h.read_file(report_path, 128 << 20).decode('utf-8'))
        record['report_sha256'] = h.file_sha(report_path)
        iterations = check_report(h, report, args, output, workers, source)
        record.update(source_digest=report['source_digest'], refs=iterations[0]['refs'],
                      iterations=iterations, complete=True)
    except BaseException as exc:
        record['error'] = repr(exc)
        raise
    finally:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            record['elapsed_seconds'], record['finished_utc'] = time.monotonic() - started, h.utc()
            record['usage_after'] = h.usage()
            record['whole_command_child_cpu_seconds'] = sum(record['usage_after'][k] - record['usage_before'][k]
                                                            for k in ('user_cpu_seconds', 'system_cpu_seconds'))
            for name, path in (('stdout', stdout_path), ('stderr', stderr_path)):
                if path.exists():
                    record[name + '_sha256'] = h.file_sha(path)
            h.save(args.output_dir / (label + '.attempt.json'), record)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    return record


def summarize(h, runs):
    h.require([r['workers'] for r in runs] == list(ORDER) and
              all(r['complete'] is True and len(r['iterations']) == 1 for r in runs),
              'incomplete forward/reverse worker order')
    h.require(all(r['source_digest'] == runs[0]['source_digest'] and r['refs'] == runs[0]['refs']
                  for r in runs), 'cross-worker source/trio bytes differ')
    result = {}
    for workers in WORKERS:
        groups = [r for r in runs if r['workers'] == workers]
        rows = [r['iterations'][0] for r in groups]
        h.require(len(rows) == 2, 'missing worker samples')
        walls = [r['build_wall_nanos'] / 1e9 for r in rows]
        median = statistics.median(walls)
        result[str(workers)] = {'samples': 2, 'wall_seconds': walls, 'median_wall_seconds': median,
            'min_wall_seconds': min(walls), 'max_wall_seconds': max(walls),
            'blocks_per_second_at_median_wall': 16 / median,
            'allocated_bytes': [r['build_allocated_bytes'] for r in rows],
            'allocations': [r['build_allocations'] for r in rows],
            'gc_cycles': [r['build_gc_cycles'] for r in rows],
            'output_bytes': [r['output_bytes'] for r in rows],
            'verification_wall_seconds': [r['verification_wall_nanos'] / 1e9 for r in rows],
            'whole_command_child_cpu_seconds': [g['whole_command_child_cpu_seconds'] for g in groups]}
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--binary-sha', required=True)
    p.add_argument('--input-dir', type=Path, required=True)
    p.add_argument('--manifest-sha', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    args = p.parse_args()
    if not sys.platform.startswith('linux') or os.geteuid() == 0:
        raise RuntimeError('native Linux runner requires the non-root private capture owner')
    h = load_helpers()
    for path in (args.binary, args.input_dir, args.output_dir):
        h.require(path.is_absolute() and path == path.resolve(), 'absolute non-symlink paths required')
    h.require(h.sha(args.binary_sha) and h.sha(args.manifest_sha), 'full SHA256 pins required')
    h.require(h.file_sha(args.binary) == args.binary_sha, 'binary SHA differs')
    info = args.input_dir.stat()
    h.require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) & 0o077 == 0,
              'input must be owned by this user and private')
    data = h.read_file(args.input_dir / 'manifest.json', 128 << 20)
    h.require(hashlib.sha256(data).hexdigest() == args.manifest_sha, 'manifest SHA differs')
    source = json.loads(data.decode('utf-8'))
    h.check_export(source)
    source_path = Path(source['source_chaindata'])
    h.require(source_path.is_absolute() and source_path.name == 'chaindata' and source_path.parent.name == 'gtron',
              'unrecognized original production path')
    for excluded in (source_path.parent.parent.resolve(), args.input_dir):
        h.require(args.output_dir != excluded and excluded not in args.output_dir.parents,
                  'output overlaps input or original production datadir')
    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    summary = {'complete': False, 'started_utc': h.utc(), 'binary_sha256': args.binary_sha,
               'input_manifest_sha256': args.manifest_sha, 'script_sha256': h.file_sha(__file__),
               'helper_revision': HELPER_REVISION, 'helper_path': HELPER_PATH, 'helper_sha256': HELPER_SHA,
               'input_dir': str(args.input_dir), 'output_dir': str(args.output_dir), 'runs': [],
               'comparison_scope': COMPARISON_SCOPE, 'resource_scope': h.RESOURCE_SCOPE}
    before = None
    try:
        before = h.private_manifest(args.input_dir)
        h.save(args.output_dir / 'input-files-before.json', before)
        for index, workers in enumerate(ORDER):
            item = run_one(h, args, index, workers, source)
            if summary['runs']:
                h.require(item['source_digest'] == summary['runs'][0]['source_digest'], 'source digest changed')
                h.require(item['refs'] == summary['runs'][0]['refs'], 'cross-worker trio bytes differ')
            summary['runs'].append(item)
            print(json.dumps({'finished': item['label'], 'equivalent': True}), flush=True)
        summary['results'] = summarize(h, summary['runs'])
        summary['complete'] = True
    except BaseException as exc:
        summary['error'] = repr(exc)
        raise
    finally:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            try:
                after = h.private_manifest(args.input_dir)
                h.save(args.output_dir / 'input-files-after.json', after)
                summary['input_unchanged'] = before is not None and after == before
            except Exception as exc:
                summary['input_unchanged'], summary['input_after_error'] = False, repr(exc)
            try:
                summary['binary_unchanged'] = h.file_sha(args.binary) == args.binary_sha
            except Exception as exc:
                summary['binary_unchanged'], summary['binary_after_error'] = False, repr(exc)
            summary['complete'] = summary['complete'] and summary['input_unchanged'] and summary['binary_unchanged']
            summary['finished_utc'] = h.utc()
            h.save(args.output_dir / 'summary.json', summary)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    h.require(summary['complete'], 'input/binary after-check failed')
    print(json.dumps(summary['results'], sort_keys=True), flush=True)


def interrupted(signum, unused_frame):
    raise InterruptedError('operator signal ' + str(signum))


if __name__ == '__main__':
    for sig in SIGNALS:
        signal.signal(sig, interrupted)
    try:
        main()
    except Exception as exc:
        print(json.dumps({'complete': False, 'error': repr(exc)}), file=sys.stderr)
        sys.exit(1)
