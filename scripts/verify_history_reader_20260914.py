#!/usr/bin/env python3
"""Native reader acceptance with pinned helpers; Python 3.6 compatible."""
import argparse
import collections
import contextlib
import datetime
import fcntl
import hashlib
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260914-history-reader')
SOURCE = RELEASE / 'source'
OUTPUT = Path('/tmp/gtron-history-reader-acceptance-20260914')
HELPER_REVISION = '7b5d21b2bd56d60cf5113b47fdfe340e863993b8'
HELPER_PATH = 'scripts/verify_large_history_20260914.py'
HELPER_SHA = 'bb3977430c3dcc1507298ca5e44b1f7bc9f0b83163199460dd0060c54c1ad619'
BENCHMARK = 'BenchmarkSharedHistoryReaderFullPack'
FIXTURES = ('tiny_raw', 'small_snappy', 'raw_unique', 'raw_repeated',
            'snappy_unique', 'snappy_repeated', 'mixed_unique')
PACKAGES = ('./core/state', './core/state/snapshots', './core/state/pruning',
            './core/freezer', './actuator')
REPEATS = 5


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def load_helper():
    blob = subprocess.check_output(['git', 'show', HELPER_REVISION + ':' + HELPER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == HELPER_SHA, 'native acceptance helper SHA changed')
    helper = types.ModuleType('history_reader_native_helper')
    helper.__file__ = str(REPO / HELPER_PATH) + '@' + HELPER_REVISION
    exec(compile(blob, helper.__file__, 'exec'), helper.__dict__)
    # Each load has its own globals; no predecessor runner/module is mutated.
    helper.RELEASE, helper.SOURCE, helper.OUTPUT = RELEASE, SOURCE, OUTPUT
    helper.stop_process_group = stop_process_group
    return helper


def expected_cases():
    return set('{}/{}/{}/{}'.format(BENCHMARK, fixture, reader, mode)
               for fixture in FIXTURES for reader in ('has_get', 'coupled')
               for mode in ('legacy', 'candidate'))


def parse_benchmarks(text):
    rows = collections.defaultdict(list)
    for number, line in enumerate(text.splitlines(), 1):
        fields = line.strip().split()
        if not fields or not fields[0].startswith(BENCHMARK):
            continue
        # Go can emit a name before the measured row.
        if len(fields) == 1:
            continue
        match = re.match(r'^(.*)-(\d+)$', fields[0])
        require(match is not None and int(match.group(2)) == 2,
                'benchmark CPU suffix must be 2: ' + line)
        require(re.match(r'^[1-9][0-9]*$', fields[1]) is not None,
                'invalid benchmark iterations: ' + line)
        require((len(fields) - 2) % 2 == 0, 'unpaired benchmark metric: ' + line)
        metrics = {}
        for index in range(2, len(fields), 2):
            value, unit = float(fields[index]), fields[index + 1]
            require(math.isfinite(value) and value >= 0, 'invalid benchmark metric: ' + line)
            require(unit not in metrics, 'duplicate benchmark unit: ' + line)
            metrics[unit] = value
        require(all(unit in metrics for unit in ('ns/op', 'B/op', 'allocs/op')),
                'required benchmark units missing: ' + line)
        require(metrics['ns/op'] > 0, 'benchmark ns/op must be positive')
        rows[match.group(1)].append({'line': number, 'iterations': int(fields[1]),
                                     'gomaxprocs_suffix': 2, 'metrics': metrics})
    expected = expected_cases()
    require(set(rows) == expected, 'benchmark cases differ: missing={}, unexpected={}'.format(
        sorted(expected - set(rows)), sorted(set(rows) - expected)))
    cases = {}
    for name in sorted(rows):
        samples = rows[name]
        require(len(samples) == REPEATS, '{}: expected {} samples, got {}'.format(name, REPEATS, len(samples)))
        units = set(samples[0]['metrics'])
        require(all(set(sample['metrics']) == units for sample in samples), 'metric schema changed: ' + name)
        cases[name] = {'repeats': len(samples), 'samples': samples,
                       'metrics': {unit: {'median': statistics.median([row['metrics'][unit] for row in samples]),
                                          'min': min(row['metrics'][unit] for row in samples),
                                          'max': max(row['metrics'][unit] for row in samples)}
                                   for unit in sorted(units)}}
    count = sum(len(samples) for samples in rows.values())
    require(len(cases) == 28 and count == 140, 'reader benchmark case/sample count differs')
    return {'case_count': len(cases), 'sample_count': count, 'repeats_per_case': REPEATS, 'cases': cases}


def stop_process_group(process):
    # The Go parent may exit before a compiler/test descendant. Still terminate
    # the whole session on timeout/interruption, including TERM-resistant work.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=10)
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=10)


@contextlib.contextmanager
def interrupted_as_error():
    watched = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    previous = {number: signal.getsignal(number) for number in watched}
    def interrupt(number, frame):
        # A repeated signal must not interrupt process-group cleanup or the
        # durable failure record. Original handlers are restored on exit.
        for watched_number in watched:
            signal.signal(watched_number, signal.SIG_IGN)
        raise InterruptedError('native acceptance interrupted by signal {}'.format(number))
    try:
        for number in watched:
            signal.signal(number, interrupt)
        yield
    finally:
        for number in watched:
            signal.signal(number, previous[number])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-revision', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[0-9a-f]{40}', args.source_revision) is not None,
            '--source-revision must be exact lowercase 40-hex')
    h = load_helper()
    identity = h.validate_source(args.source_revision)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with open(str(OUTPUT / 'runner.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        directory = OUTPUT / (stamp + '-' + str(os.getpid()))
        directory.mkdir()
        summary_path = directory / 'native-summary.json'
        state = {'complete': False, 'started_utc': h.utc_now(), 'invocation_argv': list(sys.argv),
                 'identity': identity, 'runner_sha256': h.file_sha(Path(__file__)),
                 'helper_revision': HELPER_REVISION, 'helper_path': HELPER_PATH, 'helper_sha256': HELPER_SHA,
                 'output_directory': str(directory), 'commands': [],
                 'limitations': ['All commands use source fixtures; no production datadir or service operation is issued.',
                                 'Rawdb full tests are a separate prepare-stage requirement, not claimed by this supplemental runner.',
                                 'Complete means commands passed and all 28 cases have five valid rows, not a speedup gate or deployment approval.',
                                 'B/op and allocs/op are benchmark allocation costs, not RSS, live heap or disk growth.']}
        env = dict(os.environ)
        env.update(h.ENV_OVERRIDES)
        env.pop('GOROOT', None)
        h.write_json(summary_path, state)
        with interrupted_as_error():
            try:
                version = h.run_command('go-version', [h.GO, 'version'], env, directory, 30, state).strip()
                require(version == 'go version go1.25.5 linux/amd64',
                        'native Go must be exactly go1.25.5 linux/amd64: ' + version)
                state['go_version'] = version
                common = [h.GO, 'test', '-p', '2', '-tags', 'sapling']
                h.run_command('related-package-tests', common + list(PACKAGES) + ['-count=1', '-timeout=300s'],
                              env, directory, 1080, state)
                text = h.run_command('reader-benchmark', common + ['./core/rawdb', '-run', '^$', '-bench',
                                     '^' + BENCHMARK + '$', '-benchmem', '-benchtime=200ms', '-count=5', '-timeout=180s'],
                                     env, directory, 240, state)
                state['reader_benchmark'] = parse_benchmarks(text)
                require(h.validate_source(args.source_revision) == identity, 'source marker changed during acceptance')
                require([record['name'] for record in state['commands']] ==
                        ['go-version', 'related-package-tests', 'reader-benchmark'], 'command evidence incomplete')
                require(all(record['completed'] and record['returncode'] == 0 for record in state['commands']),
                        'command success evidence incomplete')
                state['complete'] = True
            except BaseException as error:
                state['error'] = repr(error)
                raise
            finally:
                state['finished_utc'] = h.utc_now()
                h.write_json(summary_path, state)
                h.write_json(OUTPUT / 'native-summary.json', state)
                print(h.json.dumps({'complete': state['complete'], 'summary': str(summary_path),
                                    'summary_sha256': h.file_sha(summary_path), 'error': state.get('error'),
                                    'reader_cases': state.get('reader_benchmark', {}).get('case_count'),
                                    'reader_samples': state.get('reader_benchmark', {}).get('sample_count')},
                                   sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
