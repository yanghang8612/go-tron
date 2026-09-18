#!/usr/bin/env python3
"""Start one candidate twice on an isolated empty mainnet datadir and attest it."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import pwd
import re
import signal
import socket
import stat
import subprocess
import sys
import time


RELEASE = Path('/data/gtron/releases/20260918-fresh-mainnet')
BINARY = RELEASE / 'gtron'
OUTPUT = RELEASE / 'fresh-acceptance'
REPO = Path('/data/gtron/go-tron')
SCRIPT_PATH = 'scripts/accept_fresh_mainnet_20260918.py'
GENESIS = '00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc'
MEMORY_ENV = {'GOMEMLIMIT': '8GiB', 'GOGC': '100',
              'GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES': '536870912'}
FORMAT_FLAGS = {'prune.mode': 'snap', 'db.cache': '4096',
                'history.shared-read-workers': '4', 'history.shared-chunk-cache': 'true',
                'history.reference-container': 'true'}
FORBIDDEN = {'config', 'genesis', 'testnet', 'dev', 'witness', 'snapshot.bootstrap', 'snapshot.reset',
             'sync.restart-from', 'sync.replay-stored-to'}


def require(value, message):
    if not value:
        raise RuntimeError(message)


def sha_file(path, limit=1 << 30, root=False):
    info = os.lstat(str(path)); require(stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode), 'unsafe file')
    require(info.st_size <= limit and (not root or info.st_uid == 0 and not info.st_mode & 0o022), 'unsafe file')
    digest = hashlib.sha256()
    with open(str(path), 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    require((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
            'file changed while hashing')
    return digest.hexdigest()


def git(*args):
    return subprocess.check_output(['git'] + list(args), cwd=str(REPO), stdin=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE, timeout=60)


def flag_rows(argv):
    rows, index = {}, 1
    while index < len(argv):
        token = argv[index]
        if not token.startswith('--'):
            index += 1; continue
        raw = token[2:]
        if '=' in raw:
            name, value = raw.split('=', 1); width = 1
        elif index + 1 < len(argv) and not argv[index + 1].startswith('--'):
            name, value, width = raw, argv[index + 1], 2
        else:
            name, value, width = raw, 'true', 1
        rows.setdefault(name, []).append(value); index += width
    return rows


def replace_flags(argv, replacements):
    result, index = [argv[0]], 1
    while index < len(argv):
        token = argv[index]
        if token.startswith('--'):
            name = token[2:].split('=', 1)[0]
            if name in replacements:
                index += 1 if '=' in token or index + 1 >= len(argv) or argv[index + 1].startswith('--') else 2
                continue
        result.append(token); index += 1
    for name, value in replacements.items():
        result.append('--' + name + '=' + str(value))
    return result


def validate_template(record):
    argv, environment = record.get('argv'), record.get('environment')
    require(isinstance(argv, list) and argv and all(isinstance(x, str) for x in argv), 'invalid argv template')
    require(isinstance(environment, dict), 'invalid environment template')
    require(record.get('user') == 'java-tron', 'service account identity differs')
    rows = flag_rows(argv)
    require(not (set(rows) & FORBIDDEN), 'unsafe mode in argv template')
    for name, value in FORMAT_FLAGS.items():
        require(rows.get(name) == [value], 'required production flag differs: ' + name)
    require(len(rows.get('datadir', ())) == 1, 'one explicit production datadir required')
    for name in ('log.file', 'p2p.port', 'discover.port', 'external.ip', 'http.port',
                 'jsonrpc.port', 'grpc.port', 'pprof.port', 'pprof.addr', 'metrics',
                 'metrics.addr', 'metrics.port', 'sync.stop-at', 'snapshot.dir',
                 'snapshot.etl.tempdir', 'sync.etl.tempdir'):
        require(len(rows.get(name, ())) <= 1, 'duplicate replaceable flag: ' + name)
    require(rows['datadir'] == ['/data/gtron/main/datadir'], 'production datadir template differs')
    cross = rows.get('history.cross-block-dedup')
    require(cross in (['true'], ['false']), 'one explicit cross-block flag required')
    for name, value in MEMORY_ENV.items():
        require(environment.get(name) == value, 'required memory environment differs: ' + name)
    require(set(environment) == set(MEMORY_ENV), 'unexpected runtime environment in acceptance template')
    return argv, environment


def reserve_ports():
    sockets, ports = [], {}
    for unused in range(128):
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.bind(('0.0.0.0', 0)); tcp.listen(1); p2p = tcp.getsockname()[1]
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp.bind(('0.0.0.0', p2p))
        except OSError:
            udp.close(); tcp.close(); continue
        sockets.extend([tcp, udp]); ports['p2p'] = p2p; break
    require('p2p' in ports, 'no dynamic TCP/UDP P2P port pair available')
    for name in ('http', 'jsonrpc', 'pprof', 'metrics'):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM); sock.bind(('0.0.0.0', 0)); sock.listen(1)
        sockets.append(sock); ports[name] = sock.getsockname()[1]
    require(len(set(ports.values())) == len(ports), 'port allocator collision')
    return sockets, ports


def command(template, datadir, log, ports):
    isolated = Path(datadir)
    replacements = {'datadir': datadir, 'log.file': log, 'p2p.port': ports['p2p'],
                    'discover.port': ports['p2p'], 'external.ip': '127.0.0.1',
                    'http.port': ports['http'], 'jsonrpc.port': ports['jsonrpc'],
                    'grpc.port': 0, 'pprof.port': ports['pprof'], 'pprof.addr': '127.0.0.1',
                    'metrics': 'true', 'metrics.addr': '127.0.0.1',
                    'metrics.port': ports['metrics'], 'sync.stop-at': 0,
                    'snapshot.dir': isolated / 'gtron/state-snapshots',
                    'snapshot.etl.tempdir': isolated / 'scratch/snapshot-etl',
                    'sync.etl.tempdir': isolated / 'scratch/sync-etl'}
    return replace_flags([str(BINARY)] + template[1:], replacements)


def http_json(port, path, body=None):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    try:
        encoded = None if body is None else json.dumps(body)
        connection.request('GET' if body is None else 'POST', path, body=encoded,
                           headers={'Content-Type': 'application/json'} if body is not None else {})
        response = connection.getresponse(); raw = response.read((4 << 20) + 1)
        require(response.status == 200 and len(raw) <= 4 << 20, 'probe HTTP failure')
        return json.loads(raw)
    finally:
        connection.close()


def block_height(block):
    require(isinstance(block, dict) and block.get('blockID', '').lower() == GENESIS,
            'fresh block identity is not mainnet genesis')
    raw = block.get('block_header', {}).get('raw_data', {})
    require(isinstance(raw, dict), 'fresh block header is invalid')
    if 'number' not in raw:
        return 0
    require(type(raw['number']) is int and raw['number'] == 0, 'stop-at=0 did not hold fresh head')
    return raw['number']


def metric(port, name):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    try:
        connection.request('GET', '/metrics'); response = connection.getresponse(); raw = response.read((4 << 20) + 1)
        require(response.status == 200 and len(raw) <= 4 << 20, 'metrics HTTP failure')
    finally:
        connection.close()
    for line in raw.decode().splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == name:
            return int(float(fields[1]))
    raise RuntimeError('metric missing: ' + name)


def drop_privileges(uid, gid):
    def apply():
        os.setgroups([]); os.setgid(gid); os.setuid(uid)
    return apply


def stop_child(child):
    if child.poll() is not None:
        return child.wait(timeout=1)
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        return child.wait(timeout=60)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return child.wait(timeout=30)


def run_once(template, environment, datadir, logdir, uid, gid, home, index):
    reservations, ports = reserve_ports()
    argv = command(template, str(datadir), str(logdir / ('node-%d.log' % index)), ports)
    stdout_path = OUTPUT / ('run-%d.log' % index)
    for sock in reservations:
        sock.close()
    with open(str(stdout_path), 'xb') as output:
        os.fchmod(output.fileno(), 0o600)
        child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                 env=dict(environment, HOME=home, USER='java-tron', LOGNAME='java-tron',
                                          PATH='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'),
                                 start_new_session=True, preexec_fn=drop_privileges(uid, gid))
        try:
            deadline, block = time.monotonic() + 120, None
            while time.monotonic() < deadline:
                require(child.poll() is None, 'candidate exited before acceptance')
                try:
                    block = http_json(ports['http'], '/wallet/getblockbynum', {'num': 0})
                    if block.get('blockID', '').lower() == GENESIS:
                        break
                except (OSError, ValueError, RuntimeError, http.client.HTTPException):
                    pass
                time.sleep(1)
            require(block is not None and block.get('blockID', '').lower() == GENESIS,
                    'mainnet genesis probe did not pass')
            head = http_json(ports['http'], '/wallet/getnowblock')
            require(block_height(head) == 0, 'stop-at=0 did not hold fresh head')
            budget = metric(ports['metrics'], 'state_snapshot_cold_manifest_cache_budget_bytes')
            require(budget == 536870912, 'manifest cache budget metric differs')
        finally:
            code = stop_child(child)
        require(code == 0, 'candidate did not exit cleanly after SIGTERM')
    return {'run': index, 'argv': argv, 'ports': ports, 'pid': child.pid,
            'genesis': GENESIS, 'head': 0, 'manifest_cache_budget': budget,
            'exit_code': code, 'log_sha256': sha_file(stdout_path)}


def inventory(root):
    rows = []
    for directory, subdirs, files in os.walk(str(root), followlinks=False):
        require(len(rows) + len(files) <= 20000, 'fresh inventory cap exceeded')
        require(not any((Path(directory) / name).is_symlink() for name in subdirs + files),
                'unexpected symlink in fresh datadir')
        rows.extend(str((Path(directory) / name).relative_to(root)) for name in files)
    return sorted(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source-revision', 'script-revision', 'binary-sha256', 'prepared-sha256',
                 'effective-argv-json', 'effective-argv-sha256'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0 and sys.platform == 'linux', 'root Linux execution required')
    require(re.fullmatch('[0-9a-f]{40}', args.source_revision) and
            re.fullmatch('[0-9a-f]{40}', args.script_revision) and
            re.fullmatch('[0-9a-f]{64}', args.binary_sha256) and
            re.fullmatch('[0-9a-f]{64}', args.prepared_sha256), 'candidate identity invalid')
    require(git('rev-parse', '--verify', args.script_revision + '^{commit}').decode().strip() ==
            args.script_revision and git('show', args.script_revision + ':' + SCRIPT_PATH) ==
            Path(__file__).resolve().read_bytes(), 'acceptance script Git identity differs')
    require(sha_file(BINARY, root=True) == args.binary_sha256 and
            sha_file(RELEASE / 'prepared.json', root=True) == args.prepared_sha256,
            'candidate prepared identity differs')
    prepared = json.loads((RELEASE / 'prepared.json').read_text())
    require(prepared.get('source_commit') == args.source_revision and
            prepared.get('candidate_scope', [None])[0] == 'fresh-mainnet', 'fresh candidate scope differs')
    argv_path = Path(args.effective_argv_json)
    require(sha_file(argv_path, 1 << 20, root=True) == args.effective_argv_sha256,
            'effective argv evidence differs')
    effective = json.loads(argv_path.read_text())
    template, environment = validate_template(effective)
    account = pwd.getpwnam('java-tron')
    require(not os.path.lexists(OUTPUT), 'fresh acceptance output already exists')
    # The service account needs search access only to reach its private runtime
    # child. Root-owned evidence files in OUTPUT remain mode 0600.
    OUTPUT.mkdir(mode=0o711)
    runtime = OUTPUT / 'runtime'; runtime.mkdir(mode=0o700)
    os.chown(str(runtime), account.pw_uid, account.pw_gid)
    datadir = runtime / 'datadir'; datadir.mkdir(mode=0o700)
    logdir = runtime / 'logs'; logdir.mkdir(mode=0o700)
    os.chown(str(datadir), account.pw_uid, account.pw_gid)
    os.chown(str(logdir), account.pw_uid, account.pw_gid)
    for path in (datadir / 'scratch', datadir / 'scratch/snapshot-etl',
                 datadir / 'scratch/sync-etl'):
        path.mkdir(mode=0o700); os.chown(str(path), account.pw_uid, account.pw_gid)
    first = run_once(template, environment, datadir, logdir, account.pw_uid,
                     account.pw_gid, account.pw_dir, 1)
    nodekey = datadir / 'nodekey'; require(nodekey.stat().st_size == 64, 'fresh nodekey size differs')
    nodekey_sha = sha_file(nodekey)
    second = run_once(template, environment, datadir, logdir, account.pw_uid,
                      account.pw_gid, account.pw_dir, 2)
    require(sha_file(nodekey) == nodekey_sha, 'nodekey changed across restart')
    inspect_stderr_path = OUTPUT / 'inspect.stderr.log'
    with open(str(inspect_stderr_path), 'xb') as inspect_stderr:
        os.fchmod(inspect_stderr.fileno(), 0o600)
        inspect = subprocess.check_output([str(BINARY), 'db', 'storage-alerts', '--datadir', str(datadir),
                                           '--db.cache', '16', '--json'], stdin=subprocess.DEVNULL,
                                          stderr=inspect_stderr, timeout=120,
                                          env={'HOME': account.pw_dir,
                                               'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'},
                                          preexec_fn=drop_privileges(account.pw_uid, account.pw_gid))
        inspect_stderr.flush(); os.fsync(inspect_stderr.fileno())
    alerts = json.loads(inspect); require(alerts.get('pruneModePersisted') is True and
                                         alerts.get('pruneMode') == 'snap', 'fresh prune mode differs')
    require((datadir / 'gtron/chaindata').is_dir() and (datadir / 'gtron/ancient').is_dir(),
            'fresh chain stores missing')
    result = {'accepted': True, 'source_commit': args.source_revision,
              'binary_sha256': args.binary_sha256, 'prepared_sha256': args.prepared_sha256,
              'script_commit': args.script_revision,
              'script_sha256': sha_file(Path(__file__).resolve()),
              'effective_argv_sha256': args.effective_argv_sha256, 'runs': [first, second],
              'effective_template': effective,
              'genesis': GENESIS, 'nodekey_sha256': nodekey_sha, 'nodekey_bytes': 64,
              'prune_mode': 'snap', 'prune_mode_persisted': True,
              'manifest_present': (datadir / 'gtron/state-snapshots/manifest.json').exists(),
              'inspect_stderr_sha256': sha_file(inspect_stderr_path),
              'inventory': inventory(datadir)}
    temporary = OUTPUT / '.summary.json.tmp'
    with open(str(temporary), 'xb') as output:
        os.fchmod(output.fileno(), 0o600)
        data = (json.dumps(result, sort_keys=True, indent=2) + '\n').encode()
        output.write(data); output.flush(); os.fsync(output.fileno())
    os.rename(str(temporary), str(OUTPUT / 'summary.json'))
    print(json.dumps({'accepted': True, 'summary_sha256': sha_file(OUTPUT / 'summary.json')}, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'accepted': False, 'error': str(error)[:4096]}), file=sys.stderr)
        sys.exit(1)
