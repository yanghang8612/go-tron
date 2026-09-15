#!/usr/bin/env python3
"""Replay one captured private range; never stop a service or open production.

The fixed A/B/B/A order reduces order bias. Every run authenticates its input
and compares all logical hot/cold rows. Timings cover the complete production
trio builder, not publication, pruning, GC or the original source LSM layout.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import statistics
import stat
import subprocess
import sys
import time


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def file_sha(path):
    h = hashlib.sha256()
    require(stat.S_ISREG(os.lstat(str(path)).st_mode), 'hash requires a regular non-symlink file')
    with os.fdopen(os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), 'hash target changed type')
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()


def private_manifest(path):
    result = {}
    for root, directories, files in os.walk(str(path), followlinks=False):
        for name in directories + files:
            require(not Path(root, name).is_symlink(), 'private input contains symlink')
        for name in files:
            item = Path(root, name)
            require(item.is_file(), 'private input contains non-regular file')
            result[str(item.relative_to(path))] = file_sha(item)
    return result


def save(path, value):
    with open(str(path), 'x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def terminate(child):
    if child is None:
        return
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGINT, signal.SIGTERM, signal.SIGHUP))
    try:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        finally:
            # The leader may exit before a descendant in its private group.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=10)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def run_one(args, out, index, mode, profile=False):
    label = '{:02d}-{}{}'.format(index, mode, '-profile' if profile else '')
    command = [str(args.binary), 'db', 'benchmark-history-cold',
               '--input-dir', str(args.input_dir), '--output-dir', str(out / label),
               '--copy-mode', mode, '--iterations', '1' if profile else '3',
               '--compression-format', 'auto',
               '--max-duration', '5m']
    if profile:
        command.append('--cpu-profile')
    env = {'PATH': '/data/go/bin:/usr/local/bin:/usr/bin:/bin', 'LANG': 'C',
           'HOME': os.environ['HOME'], 'GOMAXPROCS': '2', 'GOMEMLIMIT': '10GiB'}
    started = time.monotonic()
    child = None
    # Signals are deferred only across fork/assignment so no orphaned probe can
    # be lost before finally obtains its process-group handle.
    with open(str(out / (label + '.stdout.json')), 'xb') as stdout, \
            open(str(out / (label + '.stderr.log')), 'xb') as stderr:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGINT, signal.SIGTERM, signal.SIGHUP))
        try:
            try:
                child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                         stdout=stdout, stderr=stderr, start_new_session=True,
                                         preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous))
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)
            code = child.wait(timeout=330)
        finally:
            terminate(child)
    require(code == 0, 'diagnostic failed: ' + label)
    path = out / label / 'report.json'
    require(path.stat().st_size <= 128 << 20, 'diagnostic report too large')
    report = json.loads(path.read_text())
    require(report.get('complete') and report.get('physical_verified') and
            report.get('manifest_file_sha256') == args.manifest_sha,
            'incomplete or wrong input report: ' + label)
    options = report.get('options', {})
    require(options.get('copy_mode') == mode and options.get('iterations') == (1 if profile else 3) and
            options.get('input_dir') == str(args.input_dir) and options.get('output_dir') == str(out / label) and
            options.get('cpu_profile') is profile and options.get('compression_format') == 'auto' and
            report.get('compression_format') == 'auto' and
            report.get('gomaxprocs') == 2, 'wrong replay options: ' + label)
    iterations = report.get('iterations', [])
    require(len(iterations) == (1 if profile else 3) and all(x.get('equivalent') is True for x in iterations),
            'logical equivalence failed: ' + label)
    for number, item in enumerate(iterations, 1):
        for field in ('build_wall_nanos', 'build_allocated_bytes', 'build_allocations', 'output_bytes'):
            require(type(item.get(field)) is int and item[field] >= 0, 'invalid measurement ' + field)
        require(all(type(ref.get('size')) is int and ref['size'] >= 0 for ref in item.get('refs', [])), 'invalid file size')
        require(item.get('iteration') == number and item.get('digest') == report.get('source_digest') and
                item.get('refs') == iterations[0].get('refs') and len(item.get('refs', [])) == 3 and
                item.get('output_bytes') == sum(ref['size'] for ref in item['refs']) and
                item.get('build_wall_nanos', 0) > 0, 'invalid iteration evidence: ' + label)
    if profile:
        require(file_sha(out / label / 'cpu.pprof') == report.get('cpu_profile_sha256'), 'missing or changed profile')
    return {'label': label, 'copy_mode': mode, 'profile': profile,
            'elapsed_seconds': time.monotonic() - started,
            'report_sha256': file_sha(path), 'source_digest': report['source_digest'],
            'refs': iterations[0]['refs'],
            'blocks': report['export']['blocks'],
            'iterations': [{k: x[k] for k in ('build_wall_nanos', 'build_allocated_bytes',
                           'build_allocations', 'output_bytes', 'equivalent')} for x in iterations]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--binary-sha', required=True)
    p.add_argument('--input-dir', type=Path, required=True)
    p.add_argument('--manifest-sha', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    args = p.parse_args()
    require(os.geteuid() != 0, 'run as the private capture owner, not root')
    for path in (args.binary, args.input_dir, args.output_dir):
        require(path.is_absolute() and path == path.resolve(), 'absolute non-symlink path required')
    for value in (args.binary_sha, args.manifest_sha):
        require(re.fullmatch('[0-9a-f]{64}', value) is not None, 'full SHA256 required')
    require(file_sha(args.binary) == args.binary_sha, 'diagnostic binary changed')
    require((args.input_dir / 'manifest.json').stat().st_size <= 128 << 20, 'input manifest too large')
    require(args.input_dir.stat().st_uid == os.geteuid() and
            stat.S_IMODE(args.input_dir.stat().st_mode) & 0o077 == 0, 'input must be owned by this user and private')
    require(file_sha(args.input_dir / 'manifest.json') == args.manifest_sha, 'input manifest changed')
    source = json.loads((args.input_dir / 'manifest.json').read_text())
    require(source.get('export', {}).get('complete') is True, 'incomplete capture')
    source_dir = Path(source['source_chaindata']).parent.parent.resolve()
    for excluded in (source_dir, args.input_dir):
        require(args.output_dir != excluded and excluded not in args.output_dir.parents,
                'output overlaps input or production')
    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    summary = {'complete': False, 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'binary_sha256': args.binary_sha, 'input_manifest_sha256': args.manifest_sha,
               'script_sha256': file_sha(__file__), 'runs': [],
               'scope': 'private warm-source complete trio build; excludes publication/pruning/GC/production LSM'}
    before = None
    try:
        before = private_manifest(args.input_dir)
        for i, mode in enumerate(('defensive', 'owned', 'owned', 'defensive')):
            item = run_one(args, args.output_dir, i, mode)
            if summary['runs']:
                require(item['source_digest'] == summary['runs'][0]['source_digest'], 'run input digest changed')
                require(item['refs'] == summary['runs'][0]['refs'], 'run cold files changed')
            summary['runs'].append(item)
            print(json.dumps({'finished': item['label'], 'equivalent': True}), flush=True)
        for i, mode in enumerate(('defensive', 'owned'), 4):
            item = run_one(args, args.output_dir, i, mode, profile=True)
            require(item['source_digest'] == summary['runs'][0]['source_digest'], 'profile input digest changed')
            require(item['refs'] == summary['runs'][0]['refs'], 'profile cold files changed')
            summary['runs'].append(item)
        require(private_manifest(args.input_dir) == before, 'private input files changed')
        require(file_sha(args.binary) == args.binary_sha, 'binary changed during replay')
        summary['results'] = {}
        for mode in ('defensive', 'owned'):
            rows = [x for r in summary['runs'] if not r['profile'] and r['copy_mode'] == mode for x in r['iterations']]
            walls = [x['build_wall_nanos'] / 1e9 for x in rows]
            summary['results'][mode] = {'wall_seconds': walls, 'median_wall_seconds': statistics.median(walls),
                'blocks_per_second_at_median_wall': source['export']['blocks'] / statistics.median(walls),
                'allocated_bytes': [x['build_allocated_bytes'] for x in rows],
                'output_bytes': [x['output_bytes'] for x in rows]}
        summary['complete'] = True
    except BaseException as exc:
        summary['error'] = repr(exc)
        raise
    finally:
        try:
            summary['input_unchanged'] = before is not None and private_manifest(args.input_dir) == before
        except Exception as exc:
            summary['input_unchanged'] = False
            summary['input_after_error'] = repr(exc)
        if not summary['input_unchanged']:
            summary['complete'] = False
        summary['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save(args.output_dir / 'summary.json', summary)
    require(summary['complete'], 'input after-check did not pass')
    print(json.dumps(summary['results'], sort_keys=True), flush=True)


def interrupted(signum, unused_frame):
    raise InterruptedError('operator signal ' + str(signum))


if __name__ == '__main__':
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, interrupted)
    try:
        main()
    except Exception as exc:
        print(json.dumps({'complete': False, 'error': repr(exc)}), file=sys.stderr)
        sys.exit(1)
