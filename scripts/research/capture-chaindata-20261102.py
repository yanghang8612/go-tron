"""One authorized short stop; immutable SST links and independent mutable files."""
import datetime
import fcntl
import hashlib
import json
import os
import pathlib
import pwd
import shutil
import subprocess
import time
import urllib.request

ROOT = pathlib.Path('/data/gtron/maintenance/chaindata-business-20261102-01')
SOURCE = pathlib.Path('/data/gtron/main/datadir/gtron')
EXPECTED = '/data/gtron/releases/auto-0096c0af878e-6611fc4273c457b4/gtron'
EXPECTED_SHA = '6611fc4273c457b47cad2ea47742a2eea0653b50e59fe0b1d74d10105bb41745'
now = lambda: datetime.datetime.utcnow().isoformat() + 'Z'

def command(*args, **kw):
    return subprocess.check_output(list(args), **kw).decode().strip()

def props():
    return dict(line.split('=', 1) for line in command('systemctl', 'show', 'gtron', '--no-pager', '-p', 'MainPID', '-p', 'ActiveState', '-p', 'SubState', '-p', 'ExecStart').splitlines())

def http(name, suffix):
    try:
        data = urllib.request.urlopen('http://127.0.0.1:8090/' + suffix, timeout=5).read()
        (ROOT / name).write_bytes(data)
    except Exception as exc:
        receipt.setdefault('http_errors', []).append([name, repr(exc)])

def write_receipt():
    temp = ROOT / 'capture-receipt.tmp'
    temp.write_text(json.dumps(receipt, indent=2) + '\n')
    temp.replace(ROOT / 'capture-receipt.json')

ROOT.mkdir(parents=True, exist_ok=False)
account = pwd.getpwnam('java-tron')
os.chown(str(ROOT), account.pw_uid, account.pw_gid)
lock = open('/data/gtron/start.lock', 'a')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
before = props()
assert before['ActiveState'] == 'active' and int(before['MainPID']) > 0, before
assert os.readlink('/proc/' + before['MainPID'] + '/exe') == EXPECTED
assert hashlib.sha256(pathlib.Path(EXPECTED).read_bytes()).hexdigest() == EXPECTED_SHA
assert shutil.disk_usage(str(ROOT)).free > 200 * 1024**3
receipt = dict(started_utc=now(), original_service=before, original_binary=EXPECTED, original_binary_sha256=EXPECTED_SHA, linked_sst_files=0, linked_sst_bytes=0, independently_copied_files=0, independently_copied_bytes=0, errors=[])
http('pre-stop-nodeinfo.json', 'wallet/getnodeinfo')
write_receipt()
stopping = False
try:
    receipt['stop_requested_utc'] = now()
    stopping = True
    subprocess.run(['systemctl', 'stop', 'gtron'], check=True, timeout=150)
    stopped = props()
    assert int(stopped['MainPID']) == 0 and stopped['ActiveState'] == 'inactive', stopped
    receipt['stopped_utc'] = now()
    write_receipt()
    with (ROOT / 'snapshot-files.jsonl').open('w') as listing:
        for database in ['chaindata', 'history']:
            source = SOURCE / database
            target = ROOT / database
            target.mkdir()
            os.chown(str(target), account.pw_uid, account.pw_gid)
            for base, dirs, files in os.walk(str(source), followlinks=False):
                rel = pathlib.Path(base).relative_to(source)
                dest = target / rel
                if rel.parts:
                    dest.mkdir(exist_ok=True)
                    os.chown(str(dest), account.pw_uid, account.pw_gid)
                for name in files:
                    src = pathlib.Path(base) / name
                    dst = dest / name
                    if name == 'LOCK':
                        continue
                    assert not src.is_symlink(), str(src)
                    st = src.stat()
                    linked = src.suffix in ['.sst', '.ldb']
                    if linked:
                        os.link(str(src), str(dst))
                        receipt['linked_sst_files'] += 1
                        receipt['linked_sst_bytes'] += st.st_size
                    else:
                        shutil.copy2(str(src), str(dst))
                        os.chown(str(dst), account.pw_uid, account.pw_gid)
                        receipt['independently_copied_files'] += 1
                        receipt['independently_copied_bytes'] += st.st_size
                        assert dst.stat().st_ino != st.st_ino
                    listing.write(json.dumps(dict(database=database, path=str(src.relative_to(source)), bytes=st.st_size, source_dev=st.st_dev, source_inode=st.st_ino, snapshot_inode=dst.stat().st_ino, immutable_link=linked)) + '\n')
    manifest = SOURCE / 'state-snapshots/manifest.json'
    shutil.copy2(str(manifest), str(ROOT / 'state-snapshots-manifest.json'))
    receipt['snapshot_complete_utc'] = now()
    receipt['snapshot_files_sha256'] = hashlib.sha256((ROOT / 'snapshot-files.jsonl').read_bytes()).hexdigest()
    receipt['snapshot_complete'] = True
except BaseException as exc:
    receipt['errors'].append(repr(exc))
    raise
finally:
    if stopping:
        receipt['resume_requested_utc'] = now()
        try:
            assert props()['ExecStart'] == before['ExecStart'], 'service identity changed during capture'
            subprocess.run(['systemctl', 'start', 'gtron'], check=True, timeout=180)
            after = props()
            pid = int(after['MainPID'])
            assert after['ActiveState'] == 'active' and pid > 0, after
            assert os.readlink('/proc/' + str(pid) + '/exe') == EXPECTED
            receipt['resumed_utc'] = now()
            receipt['resumed_service'] = after
            receipt['same_binary_resumed'] = True
            http('post-start-nodeinfo.json', 'wallet/getnodeinfo')
        except BaseException as exc:
            receipt['errors'].append('RESTORE: ' + repr(exc))
            raise
        finally:
            write_receipt()
    fcntl.flock(lock, fcntl.LOCK_UN)
print('CAPTURE_AND_ORIGINAL_RESTORE_COMPLETE', json.dumps(receipt), flush=True)
