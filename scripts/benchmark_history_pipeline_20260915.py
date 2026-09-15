#!/usr/bin/env python3
"""A/B/B/A serial/shared-read-pipeline replay of one private 16-block capture.

Each run builds the same single cold trio. Native input authentication and full
hot/cold verification must pass; all output refs and logical digests must be
identical. Profile runs are separate from the twelve primary build samples.
No service, original source database, publication or pruning operation is used.

The small file/process/manifest helpers mirror the reviewed parallel wrapper;
this file is self-contained so native execution needs no dynamic Python helper.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import stat
import statistics
import subprocess
import sys
import time

SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
U64_MAX = (1 << 64) - 1
SUM_FIELDS = ('rows', 'payload_bytes', 'prev_bytes', 'large_prev_rows_ge_128kib',
              'large_prev_bytes_ge_128kib', 'delegation_rows', 'delegation_prev_bytes', 'tx_ranges')
DIGEST_FIELDS = SUM_FIELDS + ('max_prev_bytes', 'row_sha256', 'tx_range_sha256')
MODES = ('serial', 'pipeline', 'pipeline', 'serial')
RESOURCE_SCOPE = ('Child CPU delta covers the whole diagnostic command including input/source/cold '
                  'verification, not just timed build work. peak_rss_kib_cumulative is Linux '
                  'RUSAGE_CHILDREN maximum across all reaped children so far, not an independent '
                  'per-run peak. Go allocations/profile are process-wide. GOMEMLIMIT is a soft '
                  'runtime memory target, not a hard RSS cap.')
COMPARISON_SCOPE = ('Fixed original 16 blocks and one trio, owned Get, auto compression, GOMAXPROCS=4. '
                    'Only shared_read_pipeline differs. A/B/B/A each has three primary builds; '
                    'two separate one-build profiles are excluded from timing results. OS and '
                    'read caches are warm/order-dependent. No publication/pruning/GC or sustainable '
                    'online throughput claim. Build wall excludes external verification; child CPU does not.')


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def uint(value, label, positive=False, limit=U64_MAX):
    require(type(value) is int and (1 if positive else 0) <= value <= limit,
            'invalid unsigned measurement: ' + label)
    return value


def sha(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def read_file(path, limit):
    require(stat.S_ISREG(os.lstat(str(path)).st_mode), 'regular non-symlink file required')
    with os.fdopen(os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, 'invalid file type or size')
        data = stream.read(limit + 1)
        require(len(data) <= limit, 'file exceeds read limit')
        return data


def file_sha(path):
    h = hashlib.sha256()
    require(stat.S_ISREG(os.lstat(str(path)).st_mode), 'hash requires regular non-symlink file')
    with os.fdopen(os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), 'hash target changed type')
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()


def private_manifest(path):
    result = {}
    for root, directories, files in os.walk(str(path), followlinks=False):
        for name in directories + files:
            item = Path(root, name)
            info = os.lstat(str(item))
            require(info.st_uid == os.geteuid() and not stat.S_ISLNK(info.st_mode),
                    'private input contains foreign owner or symlink')
            require(stat.S_ISDIR(info.st_mode) if name in directories else stat.S_ISREG(info.st_mode),
                    'private input contains special file')
        for name in files:
            item = Path(root, name)
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
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
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
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=10)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def usage():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {'user_cpu_seconds': r.ru_utime, 'system_cpu_seconds': r.ru_stime,
            'peak_rss_kib_cumulative': r.ru_maxrss}


def check_export(source):
    require(uint(source.get('version'), 'version') == 1 and not source.get('error'), 'invalid capture wrapper')
    e = source.get('export', {})
    require(e.get('complete') is True and e.get('content_verified') is False and
            e.get('stop_reason') == 'complete' and not e.get('error'), 'incomplete physical export')
    for name in ('from_block', 'to_block', 'from_tx_num', 'to_tx_num', 'blocks', 'declared_decoded_bytes'):
        uint(e.get(name), name)
    require(e['blocks'] == 16 and e['to_block'] - e['from_block'] == 15 and
            e['from_tx_num'] <= e['to_tx_num'], 'capture must contain exactly 16 original blocks')
    require(uint(source.get('covered_block'), 'covered_block') < e['from_block'] and
            uint(source.get('finish_block'), 'finish_block') >= e['to_block'], 'invalid capture stage bounds')
    uint(e.get('physical_rows'), 'physical_rows', positive=True, limit=262144)
    uint(e.get('physical_bytes'), 'physical_bytes', positive=True, limit=1 << 30)
    uint(e['declared_decoded_bytes'], 'declared_decoded_bytes', limit=4 << 30)
    require(isinstance(e.get('entries'), list) and len(e['entries']) == e['physical_rows'] and
            sha(e.get('manifest_sha256')), 'invalid physical entry manifest')
    details = e.get('block_details', [])
    require(len(details) == 16, 'missing block plan')
    previous_end = None
    declared = 0
    for i, block in enumerate(details):
        require(uint(block.get('block'), 'block') == e['from_block'] + i, 'block plan is not consecutive')
        begin, end = uint(block.get('begin_tx_num'), 'begin_tx_num'), uint(block.get('end_tx_num'), 'end_tx_num')
        require(begin <= end and (previous_end is None or begin == previous_end + 1), 'tx ranges have gap or overlap')
        require(type(block.get('canonical_hash')) is str and
                re.fullmatch('(0x)?[0-9a-fA-F]{64}', block['canonical_hash']) is not None, 'invalid canonical hash')
        declared += uint(block.get('declared_decoded_bytes'), 'block declared bytes', limit=4 << 30)
        previous_end = end
    require(details[0]['begin_tx_num'] == e['from_tx_num'] and previous_end == e['to_tx_num'] and
            declared == e['declared_decoded_bytes'], 'block plan does not match full export')
    return e


def check_digest(d, hashes=True):
    require(type(d) is dict and set(d) == set(DIGEST_FIELDS), 'incomplete logical digest')
    for name in SUM_FIELDS + ('max_prev_bytes',):
        uint(d.get(name), name)
    for name in ('row_sha256', 'tx_range_sha256'):
        require(sha(d.get(name)) if hashes else d.get(name) == '', 'invalid digest SHA field ' + name)
    require(d['max_prev_bytes'] <= d['prev_bytes'] <= d['payload_bytes'] and
            d['large_prev_rows_ge_128kib'] <= d['rows'] and d['delegation_rows'] <= d['rows'] and
            d['large_prev_bytes_ge_128kib'] <= d['prev_bytes'] and
            d['delegation_prev_bytes'] <= d['prev_bytes'], 'inconsistent logical row statistics')
    return d


def check_report(report, args, output, mode, profile, source):
    count, enabled = (1 if profile else 3), mode == 'pipeline'
    require(mode in ('serial', 'pipeline'), 'unknown pipeline mode')
    require(uint(report.get('version'), 'report version') == 1 and report.get('complete') is True and
            report.get('physical_verified') is True and not report.get('error') and
            report.get('manifest_file_sha256') == args.manifest_sha and report.get('export') == source['export'],
            'incomplete or wrong physical input report')
    options = report.get('options', {})
    require(uint(options.get('iterations'), 'option iterations') == count and
            uint(options.get('max_duration_ns'), 'option duration') == 300000000000 and
            options.get('copy_mode') == 'owned' and options.get('compression_format') == 'auto' and
            options.get('shared_read_pipeline') is enabled and options.get('cpu_profile') is profile and
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
    if profile:
        require(file_sha(output / 'cpu.pprof') == report.get('cpu_profile_sha256') and
                type(report.get('cpu_profile_scope')) is str and bool(report['cpu_profile_scope']),
                'missing, changed or unscoped profile')
    else:
        require(not report.get('cpu_profile_sha256'), 'unexpected profiled primary sample')
    return iterations


def run_one(args, index, mode, source, profile=False):
    label = '{:02d}-{}{}'.format(index, mode, '-profile' if profile else '')
    output = args.output_dir / label
    command = [str(args.binary), 'db', 'benchmark-history-cold', '--input-dir', str(args.input_dir),
               '--output-dir', str(output), '--copy-mode', 'owned', '--compression-format', 'auto',
               '--shared-read-pipeline=' + ('true' if mode == 'pipeline' else 'false'),
               '--iterations', '1' if profile else '3', '--max-duration', '5m']
    if profile:
        command.append('--cpu-profile')
    env = {'PATH': '/data/go/bin:/usr/local/bin:/usr/bin:/bin', 'LANG': 'C',
           'HOME': os.environ['HOME'], 'GOMAXPROCS': '4', 'GOMEMLIMIT': '10GiB'}
    record = {'label': label, 'mode': mode, 'profile': profile, 'complete': False, 'started_utc': utc(),
              'command': command, 'environment': env, 'returncode': None,
              'resource_scope': RESOURCE_SCOPE, 'usage_before': usage()}
    started, child = time.monotonic(), None
    stdout_path, stderr_path = args.output_dir / (label + '.stdout.json'), args.output_dir / (label + '.stderr.log')
    try:
        require(mode in ('serial', 'pipeline'), 'unknown pipeline mode')
        require(file_sha(args.binary) == args.binary_sha, 'binary changed before run')
        with open(str(stdout_path), 'xb') as stdout, open(str(stderr_path), 'xb') as stderr:
            previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
            try:
                try:
                    child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                             start_new_session=True,
                                             preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous))
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous)
                record['returncode'] = child.wait(timeout=330)
            finally:
                terminate(child)
                if child is not None:
                    record['returncode'] = child.returncode
        require(record['returncode'] == 0, 'cold pipeline diagnostic failed: ' + label)
        report_path = output / 'report.json'
        report = json.loads(read_file(report_path, 128 << 20).decode('utf-8'))
        record['report_sha256'] = file_sha(report_path)
        iterations = check_report(report, args, output, mode, profile, source)
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
            record['elapsed_seconds'], record['finished_utc'] = time.monotonic() - started, utc()
            record['usage_after'] = usage()
            record['whole_command_child_cpu_seconds'] = sum(record['usage_after'][k] - record['usage_before'][k]
                                                            for k in ('user_cpu_seconds', 'system_cpu_seconds'))
            for name, path in (('stdout', stdout_path), ('stderr', stderr_path)):
                if path.exists():
                    record[name + '_sha256'] = file_sha(path)
            save(args.output_dir / (label + '.attempt.json'), record)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    return record


def summarize(runs):
    require([(r['mode'], r['profile']) for r in runs] == [(m, False) for m in MODES] +
            [('serial', True), ('pipeline', True)] and all(r['complete'] is True for r in runs), 'incomplete ABBA/profile order')
    result = {}
    for mode in ('serial', 'pipeline'):
        groups = [r for r in runs if r['mode'] == mode and not r['profile']]
        rows = [row for group in groups for row in group['iterations']]
        require(len(rows) == 6, 'missing primary build samples')
        walls = [r['build_wall_nanos'] / 1e9 for r in rows]
        median = statistics.median(walls)
        result[mode] = {'samples': 6, 'wall_seconds': walls, 'median_wall_seconds': median,
                        'min_wall_seconds': min(walls), 'max_wall_seconds': max(walls),
                        'blocks_per_second_at_median_wall': 16 / median,
                        'allocated_bytes': [r['build_allocated_bytes'] for r in rows],
                        'allocations': [r['build_allocations'] for r in rows],
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
    require(sys.platform.startswith('linux'), 'native Linux runner required for resource units')
    require(os.geteuid() != 0, 'run as the private capture owner, not root')
    for path in (args.binary, args.input_dir, args.output_dir):
        require(path.is_absolute() and path == path.resolve(), 'absolute non-symlink paths required')
    require(sha(args.binary_sha) and sha(args.manifest_sha), 'full SHA256 pins required')
    require(file_sha(args.binary) == args.binary_sha, 'binary SHA differs')
    info = args.input_dir.stat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) & 0o077 == 0,
            'input must be owned by this user and private')
    data = read_file(args.input_dir / 'manifest.json', 128 << 20)
    require(hashlib.sha256(data).hexdigest() == args.manifest_sha, 'manifest SHA differs')
    source = json.loads(data.decode('utf-8'))
    check_export(source)
    source_path = Path(source['source_chaindata'])
    require(source_path.is_absolute() and source_path.name == 'chaindata' and source_path.parent.name == 'gtron',
            'unrecognized original production path')
    for excluded in (source_path.parent.parent.resolve(), args.input_dir):
        require(args.output_dir != excluded and excluded not in args.output_dir.parents,
                'output overlaps input or original production datadir')
    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    summary = {'complete': False, 'started_utc': utc(), 'binary_sha256': args.binary_sha,
               'input_manifest_sha256': args.manifest_sha, 'script_sha256': file_sha(__file__),
               'input_dir': str(args.input_dir), 'output_dir': str(args.output_dir), 'runs': [],
               'comparison_scope': COMPARISON_SCOPE, 'resource_scope': RESOURCE_SCOPE}
    before = None
    try:
        before = private_manifest(args.input_dir)
        save(args.output_dir / 'input-files-before.json', before)
        order = [(mode, False) for mode in MODES] + [('serial', True), ('pipeline', True)]
        for index, (mode, profile) in enumerate(order):
            item = run_one(args, index, mode, source, profile)
            if summary['runs']:
                require(item['source_digest'] == summary['runs'][0]['source_digest'], 'ABBA source digest changed')
                require(item['refs'] == summary['runs'][0]['refs'], 'serial/pipeline/profile trio bytes differ')
            summary['runs'].append(item)
            print(json.dumps({'finished': item['label'], 'equivalent': True}), flush=True)
        summary['results'] = summarize(summary['runs'])
        summary['complete'] = True
    except BaseException as exc:
        summary['error'] = repr(exc)
        raise
    finally:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            try:
                after = private_manifest(args.input_dir)
                save(args.output_dir / 'input-files-after.json', after)
                summary['input_unchanged'] = before is not None and after == before
            except Exception as exc:
                summary['input_unchanged'], summary['input_after_error'] = False, repr(exc)
            try:
                summary['binary_unchanged'] = file_sha(args.binary) == args.binary_sha
            except Exception as exc:
                summary['binary_unchanged'], summary['binary_after_error'] = False, repr(exc)
            summary['complete'] = summary['complete'] and summary['input_unchanged'] and summary['binary_unchanged']
            summary['finished_utc'] = utc()
            save(args.output_dir / 'summary.json', summary)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    require(summary['complete'], 'input/binary after-check failed')
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
