#!/usr/bin/env python3
"""Python 3.6-compatible native tests/benchmarks; never controls a service or DB."""
import argparse
import collections
import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import time

RELEASE = Path('/data/gtron/releases/20260914-large-history')
SOURCE = RELEASE / 'source'
GO = '/data/go/bin/go'
OUTPUT = Path('/tmp/gtron-large-history-acceptance-20260914')
ENV_OVERRIDES = {'GOMAXPROCS': '2', 'GOTOOLCHAIN': 'local', 'CGO_ENABLED': '1', 'GOFLAGS': ''}
STATE_BENCH = 'BenchmarkLegacyDelegationMembershipArena'
RAWDB_BENCH = 'BenchmarkSharedHistoryAuthenticatedPlanner'
REPEATS = 5
SCENARIOS = ('repeated_seed', 'repeated_existing', 'single_large_existing_no_v2', 'shared_disabled')


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def file_sha(path):
    digest = hashlib.sha256()
    with open(str(path), 'rb') as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    temporary = path.with_name(path.name + '.tmp-' + str(os.getpid()))
    with open(str(temporary), 'w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(str(temporary), str(path))


def expected_cases(kind):
    if kind == 'state':
        return set('{}/degree={}/mixed={}/{}/{}'.format(STATE_BENCH, degree, mixed, workload, mode)
                   for degree, mixed in ((32, 'false'), (10000, 'false'), (100000, 'false'), (100000, 'true'))
                   for workload in ('Membership', 'NewEntry', 'DecodeEntry') for mode in ('Legacy', 'Arena'))
    require(kind == 'rawdb', 'unknown benchmark kind')
    return set('{}/{}/{}/{}'.format(RAWDB_BENCH, scenario, workload, mode)
               for scenario in SCENARIOS for workload in ('planner_only', 'codec_and_planner')
               for mode in ('legacy', 'candidate'))


def parse_benchmarks(text, kind):
    """Parse named value/unit pairs, preserving custom units and all repeats."""
    prefix = STATE_BENCH if kind == 'state' else RAWDB_BENCH
    rows = collections.defaultdict(list)
    for line_number, line in enumerate(text.splitlines(), 1):
        fields = line.strip().split()
        if not fields or not fields[0].startswith(prefix):
            continue
        # Go may print a benchmark's name alone before its measured row.
        if len(fields) == 1:
            continue
        match = re.match(r'^(.*)-(\d+)$', fields[0])
        require(match is not None, 'benchmark has no CPU suffix on line {}'.format(line_number))
        name, cpus = match.group(1), int(match.group(2))
        require(cpus == 2, 'benchmark CPU suffix differs from GOMAXPROCS=2: ' + fields[0])
        require(re.match(r'^[1-9][0-9]*$', fields[1]) is not None,
                'invalid benchmark iteration count on line {}'.format(line_number))
        require((len(fields) - 2) % 2 == 0, 'unpaired metric token on line {}'.format(line_number))
        metrics = {}
        for index in range(2, len(fields), 2):
            value, unit = float(fields[index]), fields[index + 1]
            require(math.isfinite(value) and value >= 0, 'nonfinite/negative benchmark metric: ' + line)
            require(unit not in metrics, 'duplicate benchmark metric unit: ' + line)
            metrics[unit] = value
        require(all(unit in metrics for unit in ('ns/op', 'B/op', 'allocs/op')),
                'required benchmark metrics missing: ' + line)
        require(metrics['ns/op'] > 0, 'benchmark ns/op must be positive')
        rows[name].append({'line': line_number, 'iterations': int(fields[1]), 'gomaxprocs_suffix': cpus,
                           'metrics': metrics})
    expected = expected_cases(kind)
    require(set(rows) == expected, 'benchmark cases differ for {}: missing={}, unexpected={}'.format(
        kind, sorted(expected - set(rows)), sorted(set(rows) - expected)))
    summary = {}
    for name in sorted(rows):
        values = rows[name]
        require(len(values) == REPEATS, '{}: expected {} rows, got {}'.format(name, REPEATS, len(values)))
        units = set(values[0]['metrics'])
        require(all(set(row['metrics']) == units for row in values), 'metric schema changed: ' + name)
        summary[name] = {'repeats': len(values), 'samples': values,
                         'metrics': {unit: {'median': statistics.median([row['metrics'][unit] for row in values]),
                                            'min': min(row['metrics'][unit] for row in values),
                                            'max': max(row['metrics'][unit] for row in values)}
                                     for unit in sorted(units)}}
    require(len(summary) == (24 if kind == 'state' else 16), 'expected case count changed')
    return {'case_count': len(summary), 'sample_count': sum(len(value) for value in rows.values()),
            'repeats_per_case': REPEATS, 'cases': summary}


def compact_benchmark_summary(state, rawdb):
    selected = {}
    for kind, benchmark in (('state', state), ('rawdb', rawdb)):
        for name, case in benchmark['cases'].items():
            if kind == 'state':
                keep = '/degree=100000/mixed=false/DecodeEntry/' in name
            else:
                keep = any('/' + scenario + '/' in name for scenario in
                           ('repeated_existing', 'single_large_existing_no_v2'))
            if keep:
                selected[name] = {unit: case['metrics'][unit] for unit in ('ns/op', 'B/op', 'allocs/op')}
    return selected


def stop_process_group(process):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def run_command(name, command, env, directory, timeout, state):
    stdout_path, stderr_path = directory / (name + '.stdout.txt'), directory / (name + '.stderr.txt')
    record = {'name': name, 'argv': command, 'cwd': str(SOURCE), 'started_utc': utc_now(),
              'environment_overrides': dict(ENV_OVERRIDES), 'environment_removed': ['GOROOT'],
              'wall_timeout_seconds': timeout, 'stdout': str(stdout_path), 'stderr': str(stderr_path),
              'returncode': None, 'completed': False}
    state['commands'].append(record)
    write_json(directory / 'native-summary.json', state)
    started = time.monotonic()
    process = None
    try:
        with open(str(stdout_path), 'wb') as stdout, open(str(stderr_path), 'wb') as stderr:
            process = subprocess.Popen(command, cwd=str(SOURCE), env=env, stdin=subprocess.DEVNULL,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            record['child_pid'] = process.pid
            try:
                record['returncode'] = process.wait(timeout=timeout)
            except BaseException:
                stop_process_group(process)
                record['returncode'] = process.returncode
                raise
        record['completed'] = True
    except BaseException as error:
        record['error'] = repr(error)
        raise
    finally:
        record['finished_utc'] = utc_now()
        record['elapsed_wall_seconds'] = time.monotonic() - started
        for key, path in (('stdout', stdout_path), ('stderr', stderr_path)):
            if path.exists():
                record[key + '_bytes'] = path.stat().st_size
                record[key + '_sha256'] = file_sha(path)
        write_json(directory / 'native-summary.json', state)
    require(record['returncode'] == 0, '{} returned {}'.format(name, record['returncode']))
    return stdout_path.read_text()


def validate_source(revision):
    require(re.match(r'^[0-9a-f]{40}$', revision) is not None, '--source-revision must be exact lowercase 40-hex')
    require(not RELEASE.is_symlink(), 'release must not be a symlink')
    require(SOURCE.is_dir() and not SOURCE.is_symlink(), 'release/source must be a real directory')
    marker = RELEASE / 'source-commit'
    require(marker.read_text() == revision + '\n', 'release source-commit does not match exact revision')
    require((SOURCE / 'go.mod').is_file(), 'release source go.mod missing')
    return {'source_revision': revision, 'release': str(RELEASE), 'source': str(SOURCE),
            'source_marker_sha256': file_sha(marker), 'go_mod_sha256': file_sha(SOURCE / 'go.mod')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    args = parser.parse_args()
    # Fail before invoking any subprocess when the candidate source differs.
    identity = validate_source(args.source_revision)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with open(str(OUTPUT / 'runner.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        directory = OUTPUT / (stamp + '-' + str(os.getpid()))
        directory.mkdir()
        state = {'complete': False, 'started_utc': utc_now(), 'invocation_argv': sys.argv,
                 'identity': identity, 'runner_sha256': file_sha(Path(__file__)), 'commands': [],
                 'output_directory': str(directory),
                 'limitations': ['All tests/benchmarks use source fixtures; no production datadir argument or service operation is issued.',
                                 'Benchmark success means Go passed and all expected repeated rows parsed; it is not a throughput claim or automatic deployment approval.',
                                 'B/op and allocs/op measure benchmark allocations, not RSS, live heap or disk growth.']}
        env = dict(os.environ)
        env.update(ENV_OVERRIDES)
        env.pop('GOROOT', None)
        summary_path = directory / 'native-summary.json'
        write_json(summary_path, state)
        try:
            version = run_command('go-version', [GO, 'version'], env, directory, 30, state).strip()
            require(version == 'go version go1.25.5 linux/amd64',
                    'native Go must be exactly go1.25.5 linux/amd64: ' + version)
            state['go_version'] = version
            common = [GO, 'test', '-p', '2', '-tags', 'sapling']
            run_command('state-actuator-tests', common + ['./core/state', './actuator', '-count=1', '-timeout=300s'],
                        env, directory, 720, state)
            state_text = run_command('state-benchmark', common + ['./core/state', '-run', '^$', '-bench',
                                     '^' + STATE_BENCH + '$', '-benchmem', '-benchtime=200ms', '-count=5', '-timeout=180s'],
                                     env, directory, 240, state)
            state['state_benchmark'] = parse_benchmarks(state_text, 'state')
            write_json(summary_path, state)
            rawdb_text = run_command('rawdb-benchmark', common + ['./core/rawdb', '-run', '^$', '-bench',
                                     '^' + RAWDB_BENCH + '$', '-benchmem', '-benchtime=200ms', '-count=5', '-timeout=180s'],
                                     env, directory, 240, state)
            state['rawdb_benchmark'] = parse_benchmarks(rawdb_text, 'rawdb')
            # Source identity must remain the same through the full sequence.
            require(validate_source(args.source_revision) == identity, 'source marker changed during acceptance')
            state['printable_benchmarks'] = compact_benchmark_summary(state['state_benchmark'], state['rawdb_benchmark'])
            state['complete'] = True
        except BaseException as error:
            state['error'] = repr(error)
            raise
        finally:
            state['finished_utc'] = utc_now()
            write_json(summary_path, state)
            write_json(OUTPUT / 'native-summary.json', state)
            print(json.dumps({'complete': state['complete'], 'summary': str(summary_path),
                              'summary_sha256': file_sha(summary_path), 'error': state.get('error'),
                              'state_cases': state.get('state_benchmark', {}).get('case_count'),
                              'rawdb_cases': state.get('rawdb_benchmark', {}).get('case_count'),
                              'benchmarks': state.get('printable_benchmarks', {})}, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
