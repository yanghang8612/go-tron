#!/usr/bin/env python3
"""Toggle the prepared d656 v3 reader, preserving all native attestations.

Only the old-systemd multi-line startup-property parser is adapted. This tool
does not prepare/build binaries, replace source files, or revise saved records.
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
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260913-history-sharing')
ORIGINAL_COMMIT = 'd656be3c43eb237fac4d46a8847e5fe551bc6a78'
ORIGINAL_PATH = 'scripts/deploy_history_sharing_20260913.py'
ORIGINAL_SHA = 'a70e93e205913f93e13c77ffe89f241258dd3f09d716cffc91d3cb99d5053969'
SCRIPT_PATH = 'scripts/deploy_history_sharing_systemd_20260913.py'


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_root(path, limit):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022 and
                info.st_size <= limit, 'unsafe pinned ops/evidence file: ' + str(path))
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'pinned ops/evidence file exceeds limit')
    return data


def git(*args):
    return subprocess.check_output(['git', *args], cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)


def verify_wrapper(revision):
    require(re.fullmatch('[0-9a-f]{40}', revision or ''), 'exact wrapper ops commit required')
    require(git('rev-parse', '--verify', revision + '^{commit}').decode().strip() == revision,
            'wrapper revision is not an exact commit')
    git('merge-base', '--is-ancestor', ORIGINAL_COMMIT, revision)
    blob = git('show', revision + ':' + SCRIPT_PATH)
    require(blob == read_root(__file__, 256 << 10), 'executing wrapper differs from its pinned Git blob')
    return sha(blob)


def parse_properties(output, parse_commands):
    properties = {}
    for line in output.splitlines():
        if '=' not in line:
            continue
        name, value = line.split('=', 1)
        if name in ('ExecStart', 'ExecStartPre'):
            parse_commands(value)
            if name in properties:
                require(properties[name] and value, 'ambiguous empty repeated startup property')
                value = properties[name] + ' ; ' + value
        properties[name] = value
    return properties


def install_show(h, guard):
    def show(unit):
        output, _ = h.run([h.SYSTEMCTL, 'show', unit, '--no-pager'])
        if unit == h.SERVICE:
            return parse_properties(output, guard.parse_commands)
        return dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
    h.show = show


def load_prepared(prepared_sha, mode):
    require(mode in ('enable', 'disable'), 'only reader-compatible enable/disable allowed')
    require(re.fullmatch('[0-9a-f]{64}', prepared_sha or ''), 'reviewed original shared prepare SHA required')
    raw = read_root(RELEASE / 'shared-prepared.json', 8 << 20)
    require(sha(raw) == prepared_sha, 'reviewed original shared prepare record changed')
    record = json.loads(raw)
    require(record.get('shared_prepared') is True and record.get('source_commit') == ORIGINAL_COMMIT and
            record.get('script_commit') == ORIGINAL_COMMIT, 'unexpected original source/ops identity')
    blob = git('show', ORIGINAL_COMMIT + ':' + ORIGINAL_PATH)
    original_file = RELEASE / 'source' / ORIGINAL_PATH
    require(sha(blob) == ORIGINAL_SHA and read_root(original_file, 256 << 10) == blob,
            'original attested deployment script changed')
    original = types.ModuleType('pinned_shared_ops')
    # Crucially, inherited verify_prepared still hashes the exact original
    # d656 script/source, not this new operational wrapper.
    original.__file__ = str(original_file)
    exec(compile(blob, str(original_file), 'exec'), original.__dict__)
    process = record['preflight']['process']
    require(all(type(process.get(key)) is int and process[key] > 0 for key in ('pid', 'start_ticks')),
            'invalid original prepared process identity')
    args = types.SimpleNamespace(mode=mode, prepared_sha=prepared_sha, script_revision=ORIGINAL_COMMIT,
                                 current_pid=process['pid'], current_start_ticks=process['start_ticks'],
                                 current_record_sha=record['original_native']['prepared_sha256'])
    g, i, h, guard, guard_blob, old = original.load_modules(args)
    install_show(h, guard)
    return original, g, i, h, guard, guard_blob, old, args


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['enable', 'disable'])
    parser.add_argument('--script-revision', required=True)
    parser.add_argument('--prepared-sha', required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0, 'explicit root execution required')
    wrapper_sha = verify_wrapper(args.script_revision)
    original, g, i, h, guard, blob, old, original_args = load_prepared(args.prepared_sha, args.mode)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, g.interrupted)
    with open('/data/gtron/start.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        deployment = original.Deployment(g, i, h, guard, blob, old, original_args)
        if args.mode == 'disable':
            deployment.admit()
            result = {'disabled': True, 'result': g.protected_rollback(deployment)}
        else:
            result = g.activate_transaction(deployment)
        result['wrapper_commit'] = args.script_revision
        result['wrapper_sha256'] = wrapper_sha
        result['original_source_commit'] = ORIGINAL_COMMIT
        h.save_json('writer-systemd-wrapper-last.json', result)
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result.get('active') or result.get('disabled') else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'ok': False, 'error': repr(error)}), file=sys.stderr, flush=True)
        sys.exit(1)
