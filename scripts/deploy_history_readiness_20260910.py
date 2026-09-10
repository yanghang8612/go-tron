#!/usr/bin/env python3
"""Pinned Git deployment, Python 3.6+. No database or unrelated unit writes.

prepare builds/tests without stopping gtron; activate/rollback require root.
The operator must review prepared.json and this script before explicit activate.
prepare requires --revision FULL40 and --script-revision FULL40 fetched from GitHub.
Commands never invoke a shell, cargo, kill, reset-failed, or remove a hold.
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260910-history-readiness')
SOURCE = RELEASE / 'source'
BINARY = RELEASE / 'gtron'
OLD_EXE = '/data/gtron/releases/20260910-state-hotpaths/gtron'
OLD_SHA = '081b4e6f60aeb348775c436d1e6821bd2cd6466858809116be381395c2758b944'
OLD_PID = 19704
BASE_PREFIX = '4c25c69b22ef50c665b49a26e0501026cb0f0a2c'
RELEASE_KIND = 'history-readiness-git-20260910'
SCRIPT_PATH = 'scripts/deploy_history_readiness_20260910.py'
SERVICE = 'gtron.service'
SYSTEMCTL = '/bin/systemctl'
GUARD = '/usr/local/libexec/gtron-mainnet-space-guard.py'
GUARD_CONFIG = '/etc/gtron/mainnet-space-guard.json'
GLOBAL_HOLD = '/var/lib/gtron-offline-maintenance.hold'
MAIN_HOLD = '/var/lib/gtron-mainnet-disk-stop.hold'
OTHERS = ['gtron-nile.service', 'gtron-deploy.service', 'gtron-deploy.timer']
TIMER = 'gtron-mainnet-space-guard.timer'
PRESERVED_UNITS = OTHERS + [TIMER, 'gtron-mainnet-space-guard.service']
MANIFEST_NAME = 'source-manifest.json'
EXPECTED_FILES = {'cmd/gtron/history_load.go': 'd583f7c54863d51366d261cf2f9356ab4bb50032dbb1fae5c3972515f4f87c41', 'cmd/gtron/history_parallel_ready.go': '402e129fe6e66e7825544c489b87291f9d0f1592b4eb5f71fd56b8e389b4b4a0', 'cmd/gtron/history_parallel_ready_test.go': '6ba03eb20177b2497469ee0bb318569023d080d71049ed1020fe6d713e08f2b3', 'cmd/gtron/history_resources.go': 'bcf41f54a95980ef63527ffd8a1056aac07f903ca73b21e9ddc2e0704c0d83d1', 'cmd/gtron/history_resources_test.go': '2f63834fd33a601430c452daaa9b4d1e91ef89e620bfbb242e7aa449a3a69362', 'cmd/gtron/main.go': 'f09639212ad8442773a127e1dcd6d217bbc9abe91bb896f4e88ae99c53899472', 'core/blockbuffer/buffer.go': '8135fcf6704678810e1bf35b12e7f04dad54f366fcf85779975a9a6fbc6c2c16', 'core/blockbuffer/commitment_cache_metrics_bench_test.go': '038319942c1dd8a78d745eab21bed82c5b85b36bc12b3049f4ac223c40881784', 'core/blockbuffer/commitment_cache_metrics_test.go': '4202db80762c9f33fce4725c83770c16c03516a7cf53c4e30ba480f5c12f8578', 'core/blockbuffer/layer_view.go': 'ed5a56cf85703cd44bbfb472070ce3311032768b639220ae1307965b4958509d', 'core/state/contract_strict.go': '4db7fd29f31a0bed348900ffe250735d55277f7734dd60487cd715dbb9ff2f86', 'core/state/state_code_cache.go': '6042728e7189495ae00ebb37225f9ad843cd299503d305bda27cc46d90afafd3', 'core/state/state_code_diagnostics.go': 'e367afca629036aa4957fb9cc71765f68f7c58bb1e10b96dcac1742186b1d967', 'core/state/state_code_diagnostics_bench_test.go': 'fb3f11151ae591bb70c7316f506076480c1d74084ef94301af80341237b5aaf2', 'core/state/state_code_diagnostics_test.go': '0922d97dfe2e85c704456aac4f1abc24f7e13b625f39ed7611452687c78ba767', 'core/state/state_code_store.go': '7d0a9d38b1ad9d39b7b1a5426141adbe9d3125fdfe2b74690b48b8e84f217bea', 'core/state/statedb.go': 'ad3e7f05319ca8262b4099358ac68581d84af146c7d38395fd9fa8299a7b6fe8', 'scripts/deploy_state_hotpaths_20260910.py': '9284814998e91ec0c5818c5de747e3197525e6e77c3b3bf01286320359a468cf'}
ALLOWED = set(EXPECTED_FILES)


def progress(message):
    print(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + ' ' + message, flush=True)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with open(str(path), 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def run(argv, cwd=None, env=None, timeout=30, check=True, log=None):
    result = subprocess.run([str(x) for x in argv], cwd=str(cwd) if cwd else None,
                            env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=timeout)
    output = result.stdout.decode('utf-8', 'replace')
    if log is not None:
        atomic_write(RELEASE / log, output.encode('utf-8'))
    if check and result.returncode:
        raise RuntimeError('command failed ({0}): {1}; see {2}'.format(
            result.returncode, ' '.join(str(x) for x in argv), log or output[-1500:]))
    return output, result.returncode


def atomic_write(path, data, metadata=None):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), metadata['mode'] if metadata else 0o600)
            if metadata:
                os.fchown(stream.fileno(), metadata['uid'], metadata['gid'])
        if metadata:
            for name, value in metadata.get('xattrs', {}).items():
                os.setxattr(temporary, name, base64.b64decode(value))
        os.replace(temporary, str(path))
        parent = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(name, value):
    atomic_write(RELEASE / name, (json.dumps(value, sort_keys=True, indent=2) + '\n').encode())


def load_json(name):
    with open(str(RELEASE / name)) as stream:
        return json.load(stream)


def saved_file(path):
    info = os.lstat(path)
    require(stat.S_ISREG(info.st_mode), 'not a regular file: ' + str(path))
    data = Path(path).read_bytes()
    attrs = {}
    for name in os.listxattr(path):
        attrs[name] = base64.b64encode(os.getxattr(path, name)).decode('ascii')
    return {'path': str(path), 'sha256': digest(data),
            'data_b64': base64.b64encode(data).decode('ascii'),
            'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
            'xattrs': attrs}


def show(unit):
    output, _ = run([SYSTEMCTL, 'show', unit, '--no-pager'])
    return dict(line.split('=', 1) for line in output.splitlines() if '=' in line)


def unit_snapshot(unit):
    props = show(unit)
    paths = [props.get('FragmentPath', '')] + shlex.split(props.get('DropInPaths', ''))
    require(paths[0], 'missing unit fragment: ' + unit)
    files = [saved_file(path) for path in paths if path]
    cat, _ = run([SYSTEMCTL, 'cat', unit, '--no-pager'])
    return {'properties': props, 'files': files, 'cat': cat}


def process_identity(pid):
    root = Path('/proc') / str(pid)
    exe = os.readlink(str(root / 'exe'))
    argv = [x.decode('utf-8', 'strict') for x in (root / 'cmdline').read_bytes().split(b'\0') if x]
    status = (root / 'stat').read_text()
    start_ticks = int(status[status.rfind(')') + 2:].split()[19])
    return {'pid': pid, 'exe': exe, 'exe_sha256': file_sha(root / 'exe'),
            'argv': argv, 'start_ticks': start_ticks,
            'stat': status, 'status': (root / 'status').read_text()}


def hold_snapshot(path):
    if not os.path.lexists(path):
        return {'exists': False}
    return {'exists': True, 'file': saved_file(path)}


def guard_check():
    require(os.path.lexists(GLOBAL_HOLD), 'global maintenance hold unexpectedly absent')
    require(not os.path.lexists(MAIN_HOLD), 'mainnet stop hold exists; never remove it here')
    require(show(TIMER).get('ActiveState') == 'active', 'space guard timer is not active')
    for unit in OTHERS:
        require(show(unit).get('ActiveState') == 'inactive', unit + ' is not inactive')
    command = ['/usr/bin/python3', GUARD, 'check']
    if os.geteuid() == 0:
        runuser = shutil.which('runuser')
        require(runuser is not None, 'runuser is required for the service-user guard check')
        command = [runuser, '-u', 'java-tron', '--'] + command
    else:
        require(pwd.getpwuid(os.geteuid()).pw_name == 'java-tron',
                'prepare must run as java-tron or root for the guard check')
    output, _ = run(command)
    result = json.loads(output)
    require(result.get('ok') is True, 'space guard failed')
    return result


def preflight(expected_pid=None, expected_exe=None, expected_sha=None):
    guard = guard_check()
    main = unit_snapshot(SERVICE)
    require(main['properties'].get('ActiveState') == 'active', 'mainnet is not active')
    pid = int(main['properties'].get('MainPID', '0'))
    if expected_pid is not None:
        require(pid == expected_pid, 'live PID changed; inspect before preparing/activating')
    process = process_identity(pid)
    if expected_exe:
        require(process['exe'] == expected_exe, 'live executable changed')
        require(process['argv'][0] == expected_exe, 'unexpected argv[0]')
    if expected_sha:
        require(process['exe_sha256'] == expected_sha, 'live executable checksum changed')
    stats = {}
    for path in ['/data', '/']:
        v = os.statvfs(path)
        stats[path] = {'free_bytes': v.f_bavail * v.f_frsize, 'free_inodes': v.f_favail}
    return {'captured_at': time.time(), 'main': main, 'process': process,
            'others': {unit: unit_snapshot(unit) for unit in PRESERVED_UNITS},
            'holds': {path: hold_snapshot(path) for path in [GLOBAL_HOLD, MAIN_HOLD]},
            'guard_config': saved_file(GUARD_CONFIG), 'guard_script': saved_file(GUARD),
            'guard_check': guard, 'filesystem': stats}


def effective_exec_file(snapshot):
    effective = []
    for item in snapshot['main']['files']:
        text = base64.b64decode(item['data_b64']).decode('utf-8')
        section = ''
        logical = ''
        for line in text.splitlines():
            stripped = line.strip()
            if not logical and (not stripped or stripped.startswith(('#', ';'))):
                continue
            logical += stripped
            if logical.endswith('\\'):
                logical = logical[:-1] + ' '
                continue
            if logical.startswith('[') and logical.endswith(']'):
                section = logical[1:-1]
            elif section == 'Service' and logical.startswith('ExecStart='):
                value = logical[len('ExecStart='):].strip()
                if not value:
                    effective = []
                else:
                    effective.append((item, value))
            logical = ''
        require(not logical, 'unterminated unit continuation')
    require(len(effective) == 1, 'expected exactly one effective ExecStart')
    item, value = effective[0]
    require(value.split()[0] == OLD_EXE, 'effective ExecStart is not the known release')
    require(base64.b64decode(item['data_b64']).count(OLD_EXE.encode()) == 1,
            'old executable must appear exactly once in its live file')
    require(item['uid'] == 0 and not item['mode'] & 0o022, 'unsafe unit ownership or permissions')
    return item


def same_configuration(before, now):
    for key in ['holds', 'guard_config', 'guard_script']:
        require(before[key] == now[key], key + ' changed after prepare')
    require(before['main']['files'] == now['main']['files'], 'main unit files changed after prepare')
    for unit in PRESERVED_UNITS:
        require(before['others'][unit]['files'] == now['others'][unit]['files'],
                unit + ' files changed after prepare')
    # Properties include volatile counters; retain only effective settings relevant to startup.
    for key in ['ExecStart', 'Environment', 'EnvironmentFiles', 'User', 'Group',
                'WorkingDirectory', 'TimeoutStopUSec', 'MemoryLimit', 'MemorySoftLimit',
                'Restart', 'Conditions', 'ExecStartPre', 'KillSignal']:
        require(before['main']['properties'].get(key) == now['main']['properties'].get(key),
                'effective service property changed: ' + key)


def verify_after_configuration(before, now, item, updated):
    expected = json.loads(json.dumps(before))
    for entry in expected['main']['files']:
        if entry['path'] == item['path']:
            entry['data_b64'] = base64.b64encode(updated).decode('ascii')
            entry['sha256'] = digest(updated)
    for key in ['holds', 'guard_config', 'guard_script']:
        require(expected[key] == now[key], key + ' changed during activation')
    require(expected['main']['files'] == now['main']['files'], 'unexpected main unit edit')
    for unit in PRESERVED_UNITS:
        require(expected['others'][unit]['files'] == now['others'][unit]['files'],
                unit + ' configuration changed during activation')
    for key in ['Environment', 'EnvironmentFiles', 'User', 'Group', 'WorkingDirectory',
                'TimeoutStopUSec', 'MemoryLimit', 'MemorySoftLimit', 'Restart', 'KillSignal']:
        require(expected['main']['properties'].get(key) == now['main']['properties'].get(key),
                'unexpected changed effective setting: ' + key)


def safe_extract_base(archive):
    SOURCE.mkdir()
    with tarfile.open(str(archive)) as bundle:
        for member in bundle:
            rel = Path(member.name)
            require(not rel.is_absolute() and '..' not in rel.parts, 'unsafe base archive path')
            target = SOURCE / rel
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                require(member.isfile(), 'unsupported base archive member: ' + member.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(member) as stream:
                    target.write_bytes(stream.read())
                os.chmod(str(target), member.mode & 0o777)


def resolve_commit(ref):
    output, _ = run(['git', 'rev-parse', '--verify', ref + '^{commit}'], cwd=REPO)
    value = output.strip()
    require(re.fullmatch(r'[0-9a-f]{40}', value), 'invalid resolved commit: ' + ref)
    return value


def validate_revision(revision, script_revision):
    require(isinstance(revision, str) and re.fullmatch(r'[0-9a-f]{40}', revision),
            'prepare --revision must be an exact 40-character lowercase commit SHA')
    require(resolve_commit(revision) == revision, 'revision does not resolve exactly')
    require(EXPECTED_FILES, 'release file manifest is not populated')
    base = resolve_commit(BASE_PREFIX)
    require(run(['git', 'merge-base', '--is-ancestor', base, revision], cwd=REPO,
                check=False)[1] == 0, 'candidate does not descend from the running base')
    admitted_refs = []
    for ref in ['FETCH_HEAD', 'refs/remotes/origin/master']:
        try:
            tip = resolve_commit(ref)
        except RuntimeError:
            continue
        if run(['git', 'merge-base', '--is-ancestor', revision, tip], cwd=REPO,
               check=False)[1] == 0:
            admitted_refs.append({'ref': ref, 'commit': tip})
    require(admitted_refs, 'candidate is not reachable from fetched GitHub master/FETCH_HEAD')
    diff, _ = run(['git', 'diff', '--name-status', '--no-renames', base, revision, '--'], cwd=REPO)
    changed = set()
    for line in diff.splitlines():
        parts = line.split('\t')
        require(len(parts) == 2 and parts[0] in ('A', 'M'), 'unexpected diff operation: ' + line)
        name = parts[1]
        require(name in ALLOWED or name == SCRIPT_PATH or
                (name.startswith('docs/') and name.endswith('.md')),
                'candidate includes an unrelated changed path: ' + name)
        changed.add(name)
    require(ALLOWED <= changed, 'candidate must contain every pinned changed file')
    require(isinstance(script_revision, str) and re.fullmatch(r'[0-9a-f]{40}', script_revision),
            'prepare --script-revision must be an exact 40-character lowercase commit SHA')
    require(resolve_commit(script_revision) == script_revision, 'script revision does not resolve exactly')
    require(run(['git', 'merge-base', '--is-ancestor', revision, script_revision], cwd=REPO,
                check=False)[1] == 0, 'ops script commit does not contain the candidate ancestor')
    script_refs = []
    for ref in ['FETCH_HEAD', 'refs/remotes/origin/perf/history-readiness-diagnostics-20260910']:
        try:
            tip = resolve_commit(ref)
        except RuntimeError:
            continue
        if run(['git', 'merge-base', '--is-ancestor', script_revision, tip], cwd=REPO,
               check=False)[1] == 0:
            script_refs.append({'ref': ref, 'commit': tip})
    require(script_refs, 'ops script commit is not reachable from fetched ops branch/FETCH_HEAD')
    script_bytes = subprocess.check_output(['git', 'show', script_revision + ':' + SCRIPT_PATH],
                                           cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=30)
    require(digest(script_bytes) == file_sha(__file__),
            'executing script bytes differ from the pinned Git commit')
    head = resolve_commit('HEAD')
    require(head in (base, revision), 'server checkout HEAD is neither running base nor candidate')
    return {'revision': revision, 'running_base_commit': base, 'checkout_head': head,
            'admitted_refs': admitted_refs, 'changed_files': sorted(changed), 'name_status': diff,
            'script_revision': script_revision, 'script_sha256': digest(script_bytes),
            'script_admitted_refs': script_refs}


def verify_source_files(revision):
    hashes = {}
    for name in sorted(ALLOWED):
        target = SOURCE / name
        require(target.is_file() and not target.is_symlink(), 'source missing or symlink: ' + name)
        hashes[name] = file_sha(target)
        require(hashes[name] == EXPECTED_FILES[name], 'pinned source checksum mismatch: ' + name)
    source_manifest = {'source_commit': revision, 'files': hashes}
    save_json(MANIFEST_NAME, source_manifest)
    return source_manifest


def verify_prepared(record):
    require(record.get('prepared') is True and record.get('release_kind') == RELEASE_KIND and
            re.fullmatch(r'[0-9a-f]{40}', record.get('source_commit', '')),
            'missing successful prepare record')
    require(BINARY.is_file() and not BINARY.is_symlink(), 'candidate binary missing or symlink')
    require(file_sha(BINARY) == record['binary_sha256'], 'candidate checksum changed')
    require(file_sha(OLD_EXE) == OLD_SHA, 'rollback release checksum changed')


def prepare(args):
    progress('prepare: validating pinned Git revision and deployment scope')
    admission = validate_revision(args.revision, args.script_revision)
    revision = admission['revision']
    RELEASE.mkdir(parents=True, exist_ok=True)
    lock = open(str(RELEASE / '.prepare.lock'), 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (RELEASE / 'prepared.json').exists():
        record = load_json('prepared.json')
        require(record.get('source_commit') == revision, 'release directory belongs to another commit')
        require(record.get('script_revision') == args.script_revision and
                record.get('script_sha256') == admission['script_sha256'], 'prepared script identity changed')
        verify_prepared(record)
        print(json.dumps({'prepared': True, 'already_prepared': True,
                          'source_commit': revision, 'binary_sha256': record['binary_sha256']}), flush=True)
        return
    require(not SOURCE.exists(), 'incomplete source directory exists; inspect it, do not overwrite')
    dirty, _ = run(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=REPO)
    require(all(line[3:] == 'third_party/librustzcash' for line in dirty.splitlines()),
            'unexpected tracked server changes')
    progress('prepare: capturing live service, rollback binary, guards, holds and configuration')
    before = preflight(OLD_PID, OLD_EXE, OLD_SHA)
    active_file = effective_exec_file(before)
    save_json('preflight.json', before)
    save_json('git-admission.json', admission)
    go = shutil.which('go')
    require(go is not None, 'go not found in PATH')
    env = dict(os.environ, CGO_ENABLED='1', GOMAXPROCS='2', GOFLAGS='', GOTOOLCHAIN='local')
    go_version, _ = run([go, 'version'], env=env)
    require(go_version.strip() == 'go version go1.25.5 linux/amd64', 'unexpected Go toolchain')
    progress('prepare: git archive ' + revision + ' into isolated source directory')
    source_archive = RELEASE / 'source.tar'
    run(['git', 'archive', '--format=tar', '--output=' + str(source_archive), revision], cwd=REPO)
    safe_extract_base(source_archive)
    manifest = verify_source_files(revision)
    rust = REPO / 'third_party/librustzcash'
    rust_library = rust / 'target/release/librustzcash.a'
    require(rust_library.is_file(), 'native librustzcash static library missing')
    rust_sha = file_sha(rust_library)
    rust_target = SOURCE / 'third_party/librustzcash'
    if rust_target.exists():
        require(rust_target.is_dir() and not any(rust_target.iterdir()), 'nonempty archived rust path')
        rust_target.rmdir()
    rust_target.parent.mkdir(parents=True, exist_ok=True)
    rust_target.symlink_to(rust, target_is_directory=True)
    progress('prepare: Go1.25.5 CGO sapling targeted tests, max two Go workers')
    test_pattern = ('Test(History|RuntimeHistory|StateCode|StrictObject|CodeVerification|GetCodeStrict|CommitmentParent|ReturnCommitmentParent|BaseReadCache|EmptyRootsVector|'
                    'AppendCommitmentsVector|CombineKnownDepth25)')
    run([go, 'test', '-p', '2', '-tags', 'sapling', './cmd/gtron', './core/state', './core/blockbuffer',
         './core/zksnark', '-run', test_pattern, '-count=1', '-timeout=300s', '-v'],
        cwd=SOURCE, env=env, timeout=1800, log='native-targeted-tests.log')
    progress('prepare: executing native Sapling availability and Pedersen probe')
    probe = RELEASE / 'native-sapling-probe.go'
    probe.write_text('package main\nimport("fmt";"os";"github.com/tronprotocol/go-tron/core/zksnark")\n'
                     'func main(){if !zksnark.Available(){os.Exit(2)};'
                     'v,e:=zksnark.Uncommitted();if e!=nil{panic(e)};'
                     'fmt.Printf("nativeSapling=true uncommitted=%x\\n",v)}\n')
    run([go, 'run', '-p', '2', '-tags', 'sapling', str(probe)], cwd=SOURCE, env=env,
        timeout=600, log='native-sapling-probe.log')
    require('nativeSapling=true' in (RELEASE / 'native-sapling-probe.log').read_text(),
            'native Sapling probe did not attest availability')
    progress('prepare: building Linux amd64 candidate with CGO_ENABLED=1 and tags=sapling')
    run([go, 'build', '-p', '2', '-tags', 'sapling', '-o', str(BINARY), './cmd/gtron'],
        cwd=SOURCE, env=env, timeout=1800, log='build.log')
    os.chmod(str(BINARY), 0o755)
    info, _ = run([go, 'version', '-m', str(BINARY)], env=env, log='build-info.txt')
    require('CGO_ENABLED=1' in info and '-tags=sapling' in info and 'GOARCH=amd64' in info and
            'GOOS=linux' in info, 'candidate build settings mismatch')
    run(['file', str(BINARY)], log='binary-file.txt')
    run(['ldd', str(BINARY)], log='binary-ldd.txt')
    run([str(BINARY), 'version'], cwd=RELEASE, log='binary-version.txt')
    progress('prepare: rechecking source hashes, original process and unchanged configuration')
    verify_source_files(revision)
    after = preflight(OLD_PID, OLD_EXE, OLD_SHA)
    same_configuration(before, after)
    require(before['process']['start_ticks'] == after['process']['start_ticks'], 'old PID reused')
    require(file_sha(rust_library) == rust_sha, 'native static library changed during build')
    record = {'prepared': True, 'prepared_at': time.time(), 'release_kind': RELEASE_KIND,
              'source_commit': revision, 'running_base_commit': admission['running_base_commit'],
              'script_revision': admission['script_revision'], 'script_sha256': admission['script_sha256'],
              'source_archive_sha256': file_sha(source_archive), 'source_manifest': manifest,
              'binary_sha256': file_sha(BINARY), 'rust_library_sha256': rust_sha,
              'go_version': go_version.strip(), 'git_admission': admission,
              'preflight': before, 'exec_file': active_file}
    save_json('prepared.json', record)
    atomic_write(RELEASE / 'source-commit', (revision + '\n').encode())
    atomic_write(RELEASE / 'SHA256SUMS', (record['binary_sha256'] + '  gtron\n').encode())
    progress('prepare: complete; service was not stopped or restarted')
    print(json.dumps({'prepared': True, 'release': str(RELEASE), 'source_commit': revision,
                      'binary_sha256': record['binary_sha256'], 'exec_file': active_file['path']}), flush=True)


def wallet_head():
    # Ignore any proxy variables: this is always the server loopback wallet.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:8090/wallet/getnowblock', timeout=5) as response:
        data = json.loads(response.read(8 * 1024 * 1024).decode('utf-8'))
    value = data['block_header']['raw_data']['number']
    require(type(value) is int and value >= 0, 'invalid wallet head')
    return value


def wait_healthy(expected_exe, expected_sha, expected_argv, seconds=240):
    deadline = time.monotonic() + seconds
    first = None
    last_error = None
    while time.monotonic() < deadline:
        try:
            props = show(SERVICE)
            require(props.get('ActiveState') == 'active', 'service not active')
            proc = process_identity(int(props.get('MainPID', '0')))
            require(proc['exe'] == expected_exe and proc['exe_sha256'] == expected_sha,
                    'unexpected running executable')
            require(proc['argv'] == expected_argv, 'running arguments differ beyond executable')
            head = wallet_head()
            if first is None:
                first = head
            elif head > first:
                return {'process': proc, 'head_first': first, 'head_last': head,
                        'verified_at': time.time()}
        except (OSError, ValueError, KeyError, RuntimeError) as exc:
            last_error = str(exc)
        time.sleep(3)
    raise RuntimeError('service did not become healthy and advance head: ' + str(last_error))


def activation_files(record):
    item = record['exec_file']
    original = base64.b64decode(item['data_b64'])
    require(original.count(OLD_EXE.encode()) == 1, 'invalid original ExecStart backup')
    updated = original.replace(OLD_EXE.encode(), str(BINARY).encode(), 1)
    require(updated.replace(str(BINARY).encode(), OLD_EXE.encode(), 1) == original,
            'candidate config differs beyond executable')
    return item, original, updated


def rollback_internal(record):
    # A missing/corrupt candidate must never prevent restoration of the known old binary.
    require(record.get('prepared') is True and record.get('release_kind') == RELEASE_KIND and
            re.fullmatch(r'[0-9a-f]{40}', record.get('source_commit', '')),
            'missing successful prepare record')
    require(file_sha(OLD_EXE) == OLD_SHA, 'rollback release checksum changed')
    item, original, updated = activation_files(record)
    current = Path(item['path']).read_bytes()
    require(current in (original, updated), 'unit changed externally; refusing to overwrite')
    props = show(SERVICE)
    if props.get('ActiveState') == 'active':
        proc = process_identity(int(props.get('MainPID', '0')))
        if current == original and proc['exe'] == OLD_EXE and proc['exe_sha256'] == OLD_SHA:
            result = wait_healthy(OLD_EXE, OLD_SHA, record['preflight']['process']['argv'])
            save_json('activation-state.json', {'phase': 'rolled_back', 'result': result})
            return result
        require(proc['exe'] in (OLD_EXE, str(BINARY)), 'unrelated live executable; refusing to stop')
    save_json('activation-state.json', {'phase': 'rollback_stopping', 'time': time.time()})
    run([SYSTEMCTL, 'stop', SERVICE], timeout=660, log='rollback-stop.log')
    require(int(show(SERVICE).get('MainPID', '0')) == 0, 'service still running; will not force stop')
    require(Path(item['path']).read_bytes() in (original, updated), 'unit changed while stopping')
    atomic_write(item['path'], original, item)
    run([SYSTEMCTL, 'daemon-reload'], timeout=60, log='rollback-daemon-reload.log')
    guard_check()
    run([SYSTEMCTL, 'start', SERVICE], timeout=180, log='rollback-start.log')
    result = wait_healthy(OLD_EXE, OLD_SHA, record['preflight']['process']['argv'])
    save_json('activation-state.json', {'phase': 'rolled_back', 'result': result})
    return result


def activate(args):
    require(os.geteuid() == 0, 'activate requires explicit root execution')
    record = load_json('prepared.json')
    verify_prepared(record)
    item, original, updated = activation_files(record)
    current = Path(item['path']).read_bytes()
    props = show(SERVICE)
    pid = int(props.get('MainPID', '0'))
    if current == updated and pid:
        proc = process_identity(pid)
        if proc['exe'] == str(BINARY) and proc['exe_sha256'] == record['binary_sha256']:
            result = wait_healthy(str(BINARY), record['binary_sha256'],
                                  [str(BINARY)] + record['preflight']['process']['argv'][1:])
            guard_check()
            save_json('activation-state.json', {'phase': 'active', 'result': result})
            print(json.dumps({'active': True, 'already_active': True, 'pid': result['process']['pid']}))
            return
    require(current == original, 'interrupted/changed activation; use explicit rollback first')
    now = preflight(OLD_PID, OLD_EXE, OLD_SHA)
    same_configuration(record['preflight'], now)
    require(now['process']['start_ticks'] == record['preflight']['process']['start_ticks'],
            'old process identity changed')
    require(now['process']['argv'] == record['preflight']['process']['argv'], 'old argv changed')
    save_json('activation-before.json', now)
    save_json('activation-state.json', {'phase': 'switching', 'time': time.time()})
    changed = False
    try:
        # Roll back even if replace succeeds but a later directory fsync fails.
        changed = True
        atomic_write(item['path'], updated, item)
        run([SYSTEMCTL, 'daemon-reload'], timeout=60, log='activate-daemon-reload.log')
        run([SYSTEMCTL, 'restart', SERVICE], timeout=900, log='activate-restart.log')
        result = wait_healthy(str(BINARY), record['binary_sha256'],
                              [str(BINARY)] + record['preflight']['process']['argv'][1:])
        guard_check()
        after = preflight(expected_exe=str(BINARY), expected_sha=record['binary_sha256'])
        verify_after_configuration(record['preflight'], after, item, updated)
        save_json('activation-after.json', after)
        save_json('activation-state.json', {'phase': 'active', 'result': result})
        print(json.dumps({'active': True, 'pid': result['process']['pid'],
                          'head_first': result['head_first'], 'head_last': result['head_last'],
                          'binary_sha256': record['binary_sha256']}))
    except BaseException as failure:
        save_json('activation-failure.json', {'error': repr(failure), 'time': time.time()})
        if changed:
            try:
                rollback_internal(record)
            except BaseException as rollback_error:
                save_json('rollback-failure.json', {'error': repr(rollback_error), 'time': time.time()})
                raise RuntimeError('activation failed and rollback needs operator attention: ' +
                                   str(failure) + '; ' + str(rollback_error))
        raise


def status(args):
    props = show(SERVICE)
    result = {'service': props.get('ActiveState'), 'pid': int(props.get('MainPID', '0')),
              'prepared': (RELEASE / 'prepared.json').exists()}
    if result['pid']:
        proc = process_identity(result['pid'])
        result.update({key: proc[key] for key in ['exe', 'exe_sha256', 'start_ticks']})
    if (RELEASE / 'activation-state.json').exists():
        result['deployment_phase'] = load_json('activation-state.json').get('phase')
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['prepare', 'activate', 'rollback', 'status'])
    parser.add_argument('--revision', help='prepare only: exact fetched Git commit, 40 lowercase hex characters')
    parser.add_argument('--script-revision', help='prepare only: exact fetched ops-script Git commit')
    args = parser.parse_args()
    if args.mode == 'status':
        status(args)
    elif args.mode == 'prepare':
        prepare(args)
    else:
        require(os.geteuid() == 0, args.mode + ' requires explicit root execution')
        # Nonblocking activation lock shares the existing deploy script's inode.
        with open('/data/gtron/start.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if args.mode == 'activate':
                activate(args)
            else:
                result = rollback_internal(load_json('prepared.json'))
                print(json.dumps({'rolled_back': True, 'pid': result['process']['pid']}))


if __name__ == '__main__':
    try:
        main()
    except BaseException as exc:
        print(json.dumps({'ok': False, 'error': repr(exc)}), file=sys.stderr)
        sys.exit(1)
