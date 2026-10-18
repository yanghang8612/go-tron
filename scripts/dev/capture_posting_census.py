#!/usr/bin/env python3
"""Capture an offline census image; restart the identical service before scanning.

No source payload is edited. Only immutable SSTs are hardlinked. Run without --capture
first; --capture takes both deployment locks and stops gtron for the copy.
"""
import argparse
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time

SOURCE = Path('/data/gtron/main/datadir/gtron')
LOCKS = ['/data/gtron/start.lock', '/run/lock/gtron-mainnet-release.lock']


def command(argv, timeout=60):
    return subprocess.check_output(argv, stderr=subprocess.STDOUT, timeout=timeout).decode().strip()


def service():
    rows = command(['systemctl', 'show', 'gtron', '--no-pager', '-p', 'MainPID',
                    '-p', 'ActiveState', '-p', 'SubState', '-p', 'ExecStart']).splitlines()
    return dict(row.split('=', 1) for row in rows if '=' in row)


def regular(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode):
        raise RuntimeError('not a regular file: ' + str(path))
    return s


def sha256_file(path):
    h = hashlib.sha256()
    with open(str(path), 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def inventory(source):
    rows = []
    for path in sorted(source.iterdir()):
        s = regular(path)
        rows.append({'name': path.name, 'size': s.st_size, 'blocks': s.st_blocks,
                     'dev': s.st_dev, 'ino': s.st_ino, 'mtime_ns': s.st_mtime_ns,
                     'mode': s.st_mode, 'uid': s.st_uid, 'gid': s.st_gid})
    return rows


def copy_pebble(source, dest):
    rows = inventory(source)
    if not any(r['name'] == 'CURRENT' for r in rows):
        raise RuntimeError('missing CURRENT')
    dest.mkdir(mode=0o700)
    for row in rows:
        name = row['name']
        src, dst = source / name, dest / name
        if name == 'LOCK':
            continue
        if name.endswith('.sst') and name[:-4].isdigit():
            os.link(str(src), str(dst), follow_symlinks=False)
            row['copy_method'] = 'immutable-sst-hardlink'
        else:
            shutil.copy2(str(src), str(dst), follow_symlinks=False)
            row['copy_method'] = 'independent-copy'
            row['sha256'] = sha256_file(dst)
            if regular(dst).st_ino == regular(src).st_ino:
                raise RuntimeError('mutable source hardlinked')
        if regular(dst).st_size != row['size']:
            raise RuntimeError('copy size mismatch')
    if inventory(source) != [{k: v for k, v in r.items() if k not in
                              ('copy_method', 'sha256')} for r in rows]:
        raise RuntimeError('source changed during stopped copy')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--capture', action='store_true')
    args = parser.parse_args()
    dest = Path(args.destination)
    if not dest.is_absolute() or dest.exists() or dest.parent.resolve() != Path('/data/gtron'):
        raise RuntimeError('destination must be new immediate /data/gtron child')
    if not dest.name.startswith('posting-census-'):
        raise RuntimeError('unexpected destination name')
    before = service()
    if before.get('ActiveState') != 'active' or int(before.get('MainPID', '0')) <= 0:
        raise RuntimeError('source service must initially be active')
    pid = before['MainPID']
    exe = os.readlink('/proc/' + pid + '/exe')
    binary_sha = sha256_file('/proc/' + pid + '/exe')
    cmdline = Path('/proc/' + pid + '/cmdline').read_bytes().split(bytes([0]))
    plan = {'source': str(SOURCE), 'destination': str(dest), 'before': before,
            'exe': exe, 'binary_sha256': binary_sha,
            'cmdline': [x.decode() for x in cmdline if x],
            'free_bytes': shutil.disk_usage(str(SOURCE)).free,
            'databases': {d: {'files': len(inventory(SOURCE / d)),
                             'mutable_bytes': sum(r['size'] for r in inventory(SOURCE / d)
                                                  if not r['name'].endswith('.sst'))}
                          for d in ['chaindata', 'history']}}
    print(json.dumps(plan), flush=True)
    if not args.capture:
        return
    if os.geteuid() != 0 or plan['free_bytes'] < 500 << 30:
        raise RuntimeError('root and 500 GiB free required')
    with ExitStack() as stack:
        for path in LOCKS:
            lock = stack.enter_context(open(path, 'a'))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if service() != before:
            raise RuntimeError('service changed during preflight')
        dest.mkdir(mode=0o700)
        plan['capture_started_unix'] = time.time()
        plan['complete'] = False
        stopped = False
        try:
            stopped = True  # even a timeout must attempt to restore the service
            command(['systemctl', 'stop', 'gtron'], timeout=620)
            after_stop = service()
            if after_stop.get('ActiveState') != 'inactive' or after_stop.get('MainPID') != '0':
                raise RuntimeError('service did not stop cleanly')
            with ExitStack() as db_locks:
                for d in ['chaindata', 'history']:
                    lock = db_locks.enter_context(open(str(SOURCE / d / 'LOCK'), 'a'))
                    fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                plan['captured'] = {d: copy_pebble(SOURCE / d, dest / d)
                                    for d in ['chaindata', 'history']}
                cold = SOURCE / 'state-snapshots' / 'manifest.json'
                regular(cold)
                shutil.copy2(str(cold), str(dest / 'cold-manifest.json'))
                plan['cold_manifest_sha256'] = hashlib.sha256((dest / 'cold-manifest.json').read_bytes()).hexdigest()
                plan['complete'] = True
        finally:
            # DB handles are closed by the nested stack; deployment locks
            # remain held through same-binary restart verification.
            plan['copy_ended_unix'] = time.time()
            (dest / 'capture.json').write_text(json.dumps(plan, indent=2))
            if stopped:
                command(['systemctl', 'start', 'gtron'], timeout=620)
                now = service()
                new_pid = now.get('MainPID', '0')
                if now.get('ActiveState') != 'active' or new_pid == '0':
                    raise RuntimeError('original service not restored')
                if os.readlink('/proc/' + new_pid + '/exe') != exe or sha256_file('/proc/' + new_pid + '/exe') != binary_sha:
                    raise RuntimeError('restored executable identity changed')
                plan['restored'] = now
                plan['restored_unix'] = time.time()
                (dest / 'capture.json').write_text(json.dumps(plan, indent=2))
        print(json.dumps({'complete': plan['complete'], 'restored': plan.get('restored'),
                          'destination': str(dest)}), flush=True)


if __name__ == '__main__':
    main()
