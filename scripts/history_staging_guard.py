#!/usr/bin/env python3
"""Persistent mainnet history-staging startup fence.

Install a root-owned copy outside the checkout before starting migration.
This program is read-only. It never opens either Pebble database.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import sys


SERVICE = 'gtron.service'
STATE_DIR = Path('/data/gtron/main')
PREPARED = STATE_DIR / 'HISTORY_STAGING_PREPARED.json'
LATCH = STATE_DIR / 'MIGRATION_IN_PROGRESS.json'
REQUIRED = STATE_DIR / 'HISTORY_STAGING_READER_REQUIRED.json'
REPIN_INTENT = STATE_DIR / 'HISTORY_STAGING_REPIN_INTENT.json'
UPGRADE_BINDING = Path('/var/lib/gtron-history-staging/apply-upgrade.json')
MAX_JSON = 16 << 10
MAX_BINARY = 512 << 20
SHA = re.compile(r'[0-9a-f]{64}\Z')


class StopIntent(Exception):
    """The timer should exit successfully without starting an inactive node."""


def read_root_json(path, limit=MAX_JSON):
    if not os.path.lexists(path):
        return None
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != 0 or
                before.st_mode & 0o022 or before.st_size > limit):
            raise ValueError('unsafe root-owned history-staging state: ' + str(path))
        raw = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if (len(raw) != before.st_size or len(raw) > limit or
            (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) !=
            (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns)):
        raise ValueError('history-staging state changed during read: ' + str(path))
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get('version') != 1:
        raise ValueError('unknown history-staging state version: ' + str(path))
    return value


def binary_sha(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0 or before.st_size > MAX_BINARY:
            raise ValueError('unsafe mainnet binary: ' + path)
        digest = hashlib.sha256()
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
        raise ValueError('mainnet binary changed during hash')
    return digest.hexdigest()


def service_binary():
    raw = subprocess.check_output(['/bin/systemctl', 'show', SERVICE,
                                   '--property=ExecStart', '--no-pager'], timeout=15).decode()
    matches = re.findall(r'\{([^{}]+)\}', raw)
    if len(matches) != 1:
        raise ValueError('expected exactly one effective gtron ExecStart')
    fields = {}
    for part in re.split(r'\s*;\s*', matches[0]):
        name, sep, value = part.partition('=')
        if sep:
            fields[name.strip()] = value.strip()
    path = fields.get('path')
    argv = shlex.split(fields.get('argv[]', ''))
    if (not path or not os.path.isabs(path) or fields.get('ignore_errors') != 'no' or
            not argv or argv[0] != path):
        raise ValueError('unsafe effective gtron ExecStart')
    return path


def capable(binary):
    result = subprocess.run([binary, 'db', 'history-staging', 'capability'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, timeout=15, check=False)
    if result.returncode != 0 or len(result.stdout) > MAX_JSON:
        return False
    try:
        value = json.loads(result.stdout)
    except (ValueError, UnicodeDecodeError):
        return False
    return value == {'format_version': 1, 'history_staging_reader': True,
                     'protocol_version': 1}


def check_identity(prepared, latch, required, digest, is_capable):
    if not SHA.fullmatch(digest):
        raise ValueError('invalid binary SHA')
    if prepared is None and latch is None and required is None:
        raise ValueError('history-staging startup fence is not initialized')
    if latch is not None:
        state = latch.get('state')
        candidate = latch.get('candidate_sha256')
        if not isinstance(candidate, str) or not SHA.fullmatch(candidate):
            raise ValueError('migration latch has no valid candidate SHA')
        if state == 'MIGRATION_IN_PROGRESS':
            raise ValueError('history-staging migration in progress; service must remain stopped')
        if state != 'VERIFIED_PENDING_ACTIVATION':
            raise ValueError('unknown history-staging migration state')
        if not latch.get('service_was_active', False):
            raise ValueError('service was originally inactive; activation requires offline verify')
        if digest != candidate or not is_capable:
            raise ValueError('pending activation requires the fixed capable candidate')
        return
    if required is not None:
        pinned = required.get('binary_sha256')
        if (required.get('format_version') != 1 or not isinstance(pinned, str) or
                not SHA.fullmatch(pinned) or digest != pinned or not is_capable or
                required.get('prune_mode') not in ('snap', 'archive') or
                type(required.get('history_window')) is not int or
                not 0 < required['history_window'] < (1 << 64)):
            raise ValueError('history-staging reader marker/binary capability mismatch')
        if required.get('stop_intent', False):
            raise ValueError('mainnet service has persistent inactive intent')
        return
    legacy = prepared.get('legacy_binary_sha256')
    if not isinstance(legacy, str) or not SHA.fullmatch(legacy) or digest != legacy:
        raise ValueError('unmigrated mainnet may start only its pinned legacy binary')


def require_no_repin():
    if os.path.lexists(REPIN_INTENT):
        raise ValueError('pending history-staging repin blocks startup and deployment')


def check_upgrade_fence(latch, prepared):
    if latch is None or 'upgrade_binding' not in latch:
        return
    if latch.get('upgrade_binding') != str(UPGRADE_BINDING):
        raise ValueError('upgrade binding path differs from trusted journal')
    journal = read_root_json(UPGRADE_BINDING, 65536)
    if journal is None or journal.get('state') != 'DONE' or journal.get('new_prepared') != prepared:
        raise ValueError('upgrade reader fences differ from completed journal')
    expected = journal.get('new_latch', {})
    for key, value in expected.items():
        if key != 'state' and latch.get(key) != value:
            raise ValueError('upgrade latch differs from completed executor binding')


def check_service():
    require_no_repin()
    prepared = read_root_json(PREPARED)
    latch = read_root_json(LATCH)
    required = read_root_json(REQUIRED)
    check_upgrade_fence(latch, prepared)
    binary = service_binary()
    digest = binary_sha(binary)
    needs_capability = (latch is not None and latch.get('state') == 'VERIFIED_PENDING_ACTIVATION') or required is not None
    check_identity(prepared, latch, required, digest,
                   capable(binary) if needs_capability else False)
    print(json.dumps({'ok': True, 'binary_sha256': digest, 'history_staging_guard': 1},
                     sort_keys=True))


def check_deploy():
    require_no_repin()
    prepared = read_root_json(PREPARED)
    latch = read_root_json(LATCH)
    required = read_root_json(REQUIRED)
    if latch is not None:
        raise ValueError('migration latch blocks ordinary deployment')
    if prepared is not None and required is None:
        raise ValueError('unmigrated source is pinned to its legacy binary')
    if required is None:
        raise ValueError('history-staging deployment fence is not initialized')
    if required.get('format_version') != 1:
        raise ValueError('unknown history-staging reader format')
    if required.get('stop_intent', False):
        raise StopIntent('mainnet service has persistent inactive intent')
    print(json.dumps({'ok': True, 'history_staging_deploy_guard': 1}, sort_keys=True))


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['check-service']:
            check_service()
        elif sys.argv[1:] == ['check-deploy']:
            check_deploy()
        else:
            raise ValueError('usage: history_staging_guard.py check-service|check-deploy')
    except StopIntent as error:
        print('history-staging guard: ' + str(error), file=sys.stderr)
        sys.exit(3)
    except Exception as error:
        print('history-staging guard: ' + str(error), file=sys.stderr)
        sys.exit(1)
