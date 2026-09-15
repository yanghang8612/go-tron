#!/usr/bin/env python3
"""A/B/B/A build-scope authenticated chunk cache on one private 16-block input.

Same binary, four read workers, three builds per command plus separate profiles.
The previously reviewed file/process/manifest helpers are loaded from pinned
local Git bytes without running their main. No service or source mutation runs.
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
import time
import types

REPO = Path('/data/gtron/go-tron')
HELPER_REVISION = 'e913c72d274d3f76f07b61f1d44ae1df986cbee9'
HELPER_PATH = 'scripts/benchmark_history_pipeline_20260915.py'
HELPER_SHA = '00ba99f8a513da210698a7dc897ffb2a4dea5c7a4d4ad673321fdee3bede404b'
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
MODES = (False, True, True, False)
ORDER = tuple((enabled, False) for enabled in MODES) + ((False, True), (True, True))
CACHE_PAYLOAD_LIMIT = 64 << 20
CACHE_ENTRY_LIMIT = 4096
CACHE_COUNTERS = ('hits', 'misses', 'inserts', 'evictions')
CACHE_UINT_FIELDS = CACHE_COUNTERS + ('payload_bytes', 'peak_payload_bytes', 'entries', 'peak_entries',
                                      'payload_budget_bytes', 'entry_limit')
COMPARISON_SCOPE = ('Same binary and original 16 blocks, one trio, owned Get, auto compression, '
                    'shared_read_pipeline=true, shared_read_workers=4, cdc_compression_workers=1, '
                    'GOMAXPROCS=4, GOMEMLIMIT=10GiB. Both sides use local CDC1 to match the proposed '
                    'enhanced build topology; this is separate from prior default-CDC binary comparisons. '
                    'Only shared_chunk_cache changes: off,on,on,off, three builds per command; '
                    'one separate profile per mode is excluded from primary timings. A fresh cache '
                    'belongs to each timed trio build and spans its two source passes. Cache payload '
                    'limit 64MiB and 4096 entries do not bound process RSS: table metadata, 256MiB '
                    'pipeline decoded output, chunk reads, codecs, ETL and runtime memory are additional. '
                    'Cache statistics never replace complete chunk/pack/output authentication. '
                    'OS/read caches remain order-dependent. No publication/pruning or sustainable '
                    'online throughput claim; build wall excludes external verification, child CPU does not.')


def load_helpers():
    data = subprocess.check_output(['git', 'show', HELPER_REVISION + ':' + HELPER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(data).hexdigest() != HELPER_SHA:
        raise RuntimeError('reviewed pipeline helper Git bytes differ')
    module = types.ModuleType('history_chunk_cache_helpers')
    module.__file__ = str(Path(__file__).resolve())
    exec(compile(data, HELPER_PATH, 'exec'), module.__dict__)
    return module


def check_cache_stats(h, item, enabled):
    names = ('shared_chunk_cache_before_close', 'shared_chunk_cache_after_close')
    if not enabled:
        h.require(all(name not in item for name in names), 'disabled cache unexpectedly reports lifecycle statistics')
        return
    before, after = (item.get(name) for name in names)
    for label, stats in (('before', before), ('after', after)):
        h.require(type(stats) is dict and set(stats) == set(CACHE_UINT_FIELDS) | {'closed'},
                  'incomplete cache statistics: ' + label)
        for name in CACHE_UINT_FIELDS:
            h.uint(stats[name], 'cache ' + label + ' ' + name)
        h.require(type(stats['closed']) is bool, 'cache closed marker must be boolean')
        h.require(stats['payload_budget_bytes'] == CACHE_PAYLOAD_LIMIT and stats['entry_limit'] == CACHE_ENTRY_LIMIT,
                  'cache fixed budget differs')
        h.require(stats['payload_bytes'] <= stats['peak_payload_bytes'] <= CACHE_PAYLOAD_LIMIT and
                  stats['entries'] <= stats['peak_entries'] <= CACHE_ENTRY_LIMIT,
                  'cache payload or entry limit exceeded')
    h.require(before['closed'] is False and after['closed'] is True and
              after['payload_bytes'] == 0 and after['entries'] == 0, 'cache cleanup incomplete')
    h.require(all(before[name] == after[name] for name in CACHE_UINT_FIELDS
                  if name not in ('payload_bytes', 'entries')), 'cache cleanup changed counters or peaks')
    h.require(before['misses'] > 0, 'fresh cache did not exercise authenticated misses')


def check_report(h, report, args, output, enabled, profile, source):
    h.require(type(enabled) is bool and type(profile) is bool, 'invalid cache/profile mode')
    options = report.get('options', {})
    h.require(h.uint(options.get('shared_read_workers'), 'read workers') == 4 and
              h.uint(options.get('cdc_compression_workers'), 'CDC workers') == 1 and
              options.get('shared_chunk_cache') is enabled, 'wrong cache experiment options')
    iterations = h.check_report(report, args, output, 'pipeline', profile, source)
    for item in iterations:
        check_cache_stats(h, item, enabled)
    return iterations


def run_one(h, args, index, enabled, source, profile=False):
    label = '{:02d}-cache-{}{}'.format(index, 'on' if enabled else 'off', '-profile' if profile else '')
    output = args.output_dir / label
    command = [str(args.binary), 'db', 'benchmark-history-cold', '--input-dir', str(args.input_dir),
               '--output-dir', str(output), '--copy-mode', 'owned', '--compression-format', 'auto',
               '--shared-read-pipeline=true', '--shared-read-workers', '4', '--cdc-compression-workers', '1',
               '--shared-chunk-cache=' + ('true' if enabled else 'false'),
               '--iterations', '1' if profile else '3', '--max-duration', '5m']
    if profile:
        command.append('--cpu-profile')
    env = {'PATH': '/data/go/bin:/usr/local/bin:/usr/bin:/bin', 'LANG': 'C',
           'HOME': os.environ['HOME'], 'GOMAXPROCS': '4', 'GOMEMLIMIT': '10GiB'}
    record = {'label': label, 'cache_enabled': enabled, 'profile': profile, 'complete': False, 'started_utc': h.utc(),
              'command': command, 'environment': env, 'returncode': None,
              'resource_scope': h.RESOURCE_SCOPE, 'usage_before': h.usage()}
    started, child = time.monotonic(), None
    stdout_path = args.output_dir / (label + '.stdout.json')
    stderr_path = args.output_dir / (label + '.stderr.log')
    try:
        h.require(type(enabled) is bool and type(profile) is bool, 'invalid cache/profile mode')
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
        h.require(record['returncode'] == 0, 'cold chunk cache diagnostic failed: ' + label)
        report_path = output / 'report.json'
        report = json.loads(h.read_file(report_path, 128 << 20).decode('utf-8'))
        record['report_sha256'] = h.file_sha(report_path)
        iterations = check_report(h, report, args, output, enabled, profile, source)
        record.update(source_digest=report['source_digest'], refs=iterations[0]['refs'],
                      iterations=iterations, complete=True)
        if profile:
            record['cpu_profile_sha256'] = report['cpu_profile_sha256']
            record['cpu_profile_scope'] = report['cpu_profile_scope']
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
    h.require([(r['cache_enabled'], r['profile']) for r in runs] == list(ORDER) and
              all(r['complete'] is True for r in runs), 'incomplete cache ABBA/profile order')
    h.require(all(r['source_digest'] == runs[0]['source_digest'] and r['refs'] == runs[0]['refs']
                  for r in runs), 'cross-cache source/trio bytes differ')
    result = {}
    for enabled in (False, True):
        groups = [r for r in runs if r['cache_enabled'] is enabled and not r['profile']]
        rows = [row for r in groups for row in r['iterations']]
        h.require(len(rows) == 6, 'missing primary cache samples')
        walls = [r['build_wall_nanos'] / 1e9 for r in rows]
        median = statistics.median(walls)
        cache_stats = [r['shared_chunk_cache_before_close'] for r in rows] if enabled else []
        totals = {key: sum(r[key] for r in cache_stats) for key in CACHE_COUNTERS}
        if enabled:
            h.require(totals['hits'] > 0, 'primary cache samples did not exercise any authenticated hit')
        result['on' if enabled else 'off'] = {'samples': 6, 'wall_seconds': walls, 'median_wall_seconds': median,
            'min_wall_seconds': min(walls), 'max_wall_seconds': max(walls),
            'blocks_per_second_at_median_wall': 16 / median,
            'allocated_bytes': [r['build_allocated_bytes'] for r in rows],
            'allocations': [r['build_allocations'] for r in rows],
            'gc_cycles': [r['build_gc_cycles'] for r in rows],
            'output_bytes': [r['output_bytes'] for r in rows],
            'verification_wall_seconds': [r['verification_wall_nanos'] / 1e9 for r in rows],
            'whole_command_child_cpu_seconds': [g['whole_command_child_cpu_seconds'] for g in groups],
            'cache_counters_total': totals, 'cache_exercised': enabled and totals['hits'] > 0,
            'cache_peak_payload_bytes': [r['peak_payload_bytes'] for r in cache_stats],
            'cache_peak_entries': [r['peak_entries'] for r in cache_stats],
            'cache_payload_bytes_after_close': [r['shared_chunk_cache_after_close']['payload_bytes'] for r in rows] if enabled else [],
            'cache_entries_after_close': [r['shared_chunk_cache_after_close']['entries'] for r in rows] if enabled else []}
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
        for index, (enabled, profile) in enumerate(ORDER):
            item = run_one(h, args, index, enabled, source, profile)
            if summary['runs']:
                h.require(item['source_digest'] == summary['runs'][0]['source_digest'], 'source digest changed')
                h.require(item['refs'] == summary['runs'][0]['refs'], 'cross-cache/profile trio bytes differ')
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
