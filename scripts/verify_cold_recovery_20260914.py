#!/usr/bin/env python3
"""Native cold recovery test acceptance with pinned helpers; Python 3.6 compatible."""
import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260914-cold-recovery')
SOURCE = RELEASE / 'source'
OUTPUT = Path('/tmp/gtron-cold-recovery-acceptance-20260914')
HELPER_REVISION = '7b5d21b2bd56d60cf5113b47fdfe340e863993b8'
HELPER_PATH = 'scripts/verify_large_history_20260914.py'
HELPER_SHA = 'bb3977430c3dcc1507298ca5e44b1f7bc9f0b83163199460dd0060c54c1ad619'
MODULE = 'github.com/tronprotocol/go-tron'
PACKAGE_ORDER = ('./core/state/snapshots', './core/state/pruning', './core/maintenance', './cmd/gtron')
FULL_PACKAGES = PACKAGE_ORDER[:3]
COMMAND_PATTERN = 'Test.*History'
# Exact new tests plus existing hard-pressure/cached-probe regressions. Full
# package execution below also retains every other existing test and fuzz seed.
REQUIRED_TESTS = {
    './core/state/snapshots': (
        'TestHistoryRecoveryObservationDoesNotWakeOrRunMaintenance',
        'TestHistoryRecoveryObservationDeterministicPressureReplay',
        'TestHistoryRecoveryObservationDoesNotInventFreshSamples',
        'TestHistoryRecoveryObservationProbeAndScope',
        'TestHistoryRecoveryObservationPendingOuterAndLatestDeadline',
        'TestHistoryRecoveryObservationFailuresCancelOnlyTheirGeneration',
        'TestHistoryRecoveryObservationCancellationAndConcurrency',
        'TestHistoryRecoveryObservationNextAdmissionRechecksPressure',
    ),
    './core/state/pruning': (
        'TestSnapshotLifecycleRecoveryObservationKeepsOriginalScheduling',
        'TestSnapshotLifecycleRecoveryObservationDoesNotRepeatMaintenance',
    ),
    './core/maintenance': (
        'TestStoragePressureHardLimitReached',
        'TestStoragePressureHardLimitRequiresFreshEngineSample',
    ),
    './cmd/gtron': ('TestHistoryPostingPressureNeverRefreshesDevice',),
}
TESTS_FROZEN = True


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def load_helper():
    blob = subprocess.check_output(['git', 'show', HELPER_REVISION + ':' + HELPER_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(hashlib.sha256(blob).hexdigest() == HELPER_SHA, 'native acceptance helper SHA changed')
    helper = types.ModuleType('cold_recovery_native_helper')
    helper.__file__ = str(REPO / HELPER_PATH) + '@' + HELPER_REVISION
    exec(compile(blob, helper.__file__, 'exec'), helper.__dict__)
    # Each load has its own globals; no predecessor runner/module is mutated.
    helper.RELEASE, helper.SOURCE, helper.OUTPUT = RELEASE, SOURCE, OUTPUT
    helper.stop_process_group = stop_process_group
    return helper


def validate_test_scope():
    require(TESTS_FROZEN and set(REQUIRED_TESTS) == set(PACKAGE_ORDER), 'native test scope not frozen')
    for package, names in REQUIRED_TESTS.items():
        require(names and len(names) == len(set(names)), 'required tests missing or duplicated: ' + package)
        require(all(isinstance(name, str) and re.fullmatch(r'Test[A-Za-z0-9_]+', name) for name in names),
                'required tests must be exact top-level names: ' + package)
        if package == './cmd/gtron':
            require(all(re.search(COMMAND_PATTERN, name) for name in names), 'cmd filter misses required tests')


def parse_test_json(text, packages):
    """Require complete package/test lifecycles, including every pinned critical test."""
    expected = {MODULE + package[1:]: package for package in packages}
    states = {name: {'started': False, 'passed': False, 'tests': {}} for name in expected}
    events, build_output_events = 0, 0
    build_import_paths = set()
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except ValueError:
            raise RuntimeError('invalid go test JSON line {}'.format(number))
        require(isinstance(event, dict), 'go test JSON event must be object')
        action, package, test = event.get('Action'), event.get('Package'), event.get('Test')
        # Go test -json interleaves compiler diagnostics as BuildEvents. Their
        # ImportPath may be a dependency or a test-variant package ID, not one
        # of TestEvent.Package. They never satisfy a package/test PASS gate.
        require(action != 'build-fail', 'go build failure: ' + str(event.get('ImportPath')))
        if action == 'build-output':
            require(isinstance(event.get('ImportPath'), str) and event['ImportPath'] and
                    isinstance(event.get('Output'), str), 'invalid go build output event')
            build_output_events += 1
            build_import_paths.add(event['ImportPath'])
            events += 1
            continue
        require(action in ('start', 'run', 'pause', 'cont', 'pass', 'skip', 'fail', 'output'),
                'unexpected go test action: ' + str(action))
        require(package in states, 'unexpected go test package: ' + str(package))
        state = states[package]
        require(not state['passed'], 'event after package pass: ' + package)
        require(action != 'fail', 'go test failure: {}/{}'.format(package, test or 'package'))
        events += 1
        if test is None:
            if action == 'start':
                require(not state['started'], 'duplicate package start: ' + package)
                state['started'] = True
            else:
                require(state['started'], 'package event before start: ' + package)
                if action == 'pass':
                    require(state['tests'], 'package passed without tests: ' + package)
                    require(all(item['terminal'] in ('pass', 'skip') for item in state['tests'].values()),
                            'package has unfinished tests: ' + package)
                    state['passed'] = True
                else:
                    require(action == 'output', 'package must finish with pass: ' + package)
            continue
        require(state['started'], 'test before package start: ' + package)
        require(isinstance(test, str) and test.startswith(('Test', 'Fuzz', 'Example')), 'invalid test name')
        tests = state['tests']
        if action == 'run':
            require(test not in tests, 'duplicate test run: ' + test)
            tests[test] = {'terminal': None, 'paused': False}
        else:
            require(test in tests, 'test event without run: ' + test)
            item = tests[test]
            require(item['terminal'] is None, 'event after test terminal: ' + test)
            if action in ('pass', 'skip'):
                item['terminal'] = action
            elif action == 'pause':
                require(not item['paused'], 'duplicate pause: ' + test)
                item['paused'] = True
            elif action == 'cont':
                require(item['paused'], 'continue without pause: ' + test)
                item['paused'] = False
            else:
                require(action == 'output', 'invalid test action: ' + str(action))
    result = {}
    for name, state in states.items():
        require(state['passed'], 'package pass missing: ' + name)
        required = REQUIRED_TESTS[expected[name]]
        require(all(test in state['tests'] and state['tests'][test]['terminal'] == 'pass' for test in required),
                'required tests did not execute and pass: ' + name)
        result[name] = {'package_pass': True, 'required_passed': sorted(required),
                        'passed': sorted(test for test, item in state['tests'].items() if item['terminal'] == 'pass'),
                        'skipped': sorted(test for test, item in state['tests'].items() if item['terminal'] == 'skip')}
    return {'event_count': events, 'build_output_events': build_output_events,
            'build_import_paths': sorted(build_import_paths), 'package_count': len(result), 'packages': result}


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
    validate_test_scope()
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
                 'output_directory': str(directory), 'commands': [], 'tests': {},
                 'required_tests': {name: list(tests) for name, tests in REQUIRED_TESTS.items()},
                 'limitations': ['Commands use source fixtures; no production datadir or service operation is issued.',
                                 'Snapshots, pruning and maintenance run full packages; cmd/gtron uses the recorded History filter.',
                                 'Rawdb full tests remain a separate prepare-stage requirement. Race checks are separately recorded by the integrator.',
                                 'No performance benchmark is required or inferred for this recovery-only change.',
                                 'Complete means command success plus all package and required-test PASS events, not deployment approval.']}
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
                common = [h.GO, 'test', '-json', '-p', '2', '-tags', 'sapling']
                text = h.run_command('related-package-tests', common + list(FULL_PACKAGES) + ['-count=1', '-timeout=300s'],
                                     env, directory, 1080, state)
                state['tests']['related-package-tests'] = parse_test_json(text, FULL_PACKAGES)
                h.write_json(summary_path, state)
                text = h.run_command('cmd-history-tests', common + ['./cmd/gtron', '-run', COMMAND_PATTERN,
                                     '-count=1', '-timeout=300s'], env, directory, 420, state)
                state['tests']['cmd-history-tests'] = parse_test_json(text, ('./cmd/gtron',))
                require(h.validate_source(args.source_revision) == identity, 'source marker changed during acceptance')
                require([record['name'] for record in state['commands']] ==
                        ['go-version', 'related-package-tests', 'cmd-history-tests'], 'command evidence incomplete')
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
                                    'tested_packages': [package for report in state['tests'].values()
                                                        for package in sorted(report['packages'])]},
                                   sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(repr(error), file=sys.stderr, flush=True)
        sys.exit(1)
