#!/usr/bin/env python3
"""Run the reviewed fresh acceptance once from the exact durable plan inputs."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys


REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260918-fresh-mainnet')
PLAN = RELEASE / 'fresh-cutover/plan.json'
TEMPLATE = RELEASE / 'fresh-cutover/effective-argv.json'
PREPARED = RELEASE / 'prepared.json'
BINARY = RELEASE / 'gtron'
ACCEPT = RELEASE / 'ops/accept_fresh_mainnet_20260918.py'
ACCEPT_GIT_PATH = 'scripts/accept_fresh_mainnet_20260918.py'
EXPECTED_PLAN_SHA256 = 'b66f87f81a41034ed1ccb3e8b60153fcf9900eadc3ce6f4659d32182e04a6ca2'
EXPECTED_TEMPLATE_SHA256 = '7ed4d5d97cb275eda407222c6615fee77d7851ccb152aeb931fb7ff6923eb085'
EXPECTED_SOURCE_REVISION = '16ea25075c380dd610d0200e9e6048fa46a2d84c'
EXPECTED_SCRIPT_REVISION = 'd9acc3fe9016915cb3076ec719a73d362c2c5bd0'
EXPECTED_BINARY_SHA256 = '378f41bfe69d0fb8d89ade226f6b70c620e8efd42f33a2d628b20a80debb6e18'
MEMORY_ENV = {'GOMEMLIMIT': '8GiB', 'GOGC': '100',
              'GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES': '536870912'}


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def regular(path, limit=1 << 30, root=True):
    path = Path(path)
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_size <= limit and
                (not root or before.st_uid == 0 and not before.st_mode & 0o022),
                'unsafe fixed input: ' + str(path))
        data = stream.read(limit + 1); after = os.fstat(stream.fileno())
    require(len(data) == before.st_size and
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
            'fixed input changed while reading: ' + str(path))
    return data


def sha(data):
    return hashlib.sha256(data).hexdigest()


def planned_template(plan):
    process = plan['process']
    raw = base64.b64decode(process['environ'], validate=True)
    environment = {}
    ignored = {b'INVOCATION_ID', b'JOURNAL_STREAM', b'SYSTEMD_EXEC_PID'}
    for row in raw.split(b'\0'):
        if not row:
            continue
        key, sep, value = row.partition(b'=')
        if key in ignored:
            continue
        name = key.decode()
        require(sep and name not in environment, 'ambiguous planned environment')
        environment[name] = value.decode()
    require(all(environment.get(key) == value for key, value in MEMORY_ENV.items()),
            'planned memory environment differs')
    return {'argv': process['argv'],
            'environment': {key: environment[key] for key in sorted(MEMORY_ENV)},
            'user': plan['configuration']['User']}


def accept_argv(plan, template_sha):
    args = plan['args']
    return ['/usr/bin/python3', str(ACCEPT),
            '--source-revision', args['source_revision'],
            '--script-revision', args['script_revision'],
            '--binary-sha256', args['binary_sha256'],
            '--prepared-sha256', args['prepared_sha256'],
            '--effective-argv-json', str(TEMPLATE),
            '--effective-argv-sha256', template_sha]


def main():
    require(os.geteuid() == 0 and sys.platform == 'linux', 'root Linux execution required')
    plan_raw, template_raw = regular(PLAN, 4 << 20), regular(TEMPLATE, 1 << 20)
    plan_sha, template_sha = sha(plan_raw), sha(template_raw)
    require(plan_sha == EXPECTED_PLAN_SHA256, 'fixed plan SHA differs')
    require(template_sha == EXPECTED_TEMPLATE_SHA256, 'fixed template SHA differs')
    plan, template = json.loads(plan_raw), json.loads(template_raw)
    require(plan.get('version') == 1 and template == planned_template(plan),
            'durable template differs from planned process semantics')
    args = plan.get('args', {})
    require(args.get('source_revision') == EXPECTED_SOURCE_REVISION and
            args.get('script_revision') == EXPECTED_SCRIPT_REVISION and
            args.get('binary_sha256') == EXPECTED_BINARY_SHA256 and
            re.fullmatch('[0-9a-f]{64}', args.get('prepared_sha256', '')),
            'planned candidate identity differs')
    prepared_raw, binary_raw = regular(PREPARED, 16 << 20), regular(BINARY, 512 << 20)
    require(sha(prepared_raw) == args['prepared_sha256'] and sha(binary_raw) == EXPECTED_BINARY_SHA256,
            'prepared or binary fixed input differs')
    prepared = json.loads(prepared_raw)
    require(prepared.get('source_commit') == EXPECTED_SOURCE_REVISION and
            prepared.get('binary_sha256') == EXPECTED_BINARY_SHA256,
            'prepared candidate record differs')
    blob = subprocess.check_output(['git', 'show', EXPECTED_SCRIPT_REVISION + ':' + ACCEPT_GIT_PATH],
                                   cwd=str(REPO), stdin=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE, timeout=60)
    accept_raw = regular(ACCEPT, 512 << 10)
    require(accept_raw == blob, 'on-disk accept script differs from reviewed Git blob')
    argv = accept_argv(plan, template_sha)
    print(json.dumps({'template_sha256': template_sha,
                      'matches_expected': template_sha == EXPECTED_TEMPLATE_SHA256,
                      'path': str(TEMPLATE)}, sort_keys=True), flush=True)
    print(json.dumps({'plan_sha256': plan_sha, 'matches_expected': True,
                      'template_semantic_match': True, 'accept_blob_match': True,
                      'candidate_input_match': True}, sort_keys=True), flush=True)
    print('accept_argv=' + repr(argv), flush=True)
    completed = subprocess.run(argv, cwd=str(REPO), stdin=subprocess.DEVNULL)
    raise SystemExit(completed.returncode)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'launched': False, 'error': str(error)[:4096]}, sort_keys=True),
              file=sys.stderr, flush=True)
        sys.exit(1)
