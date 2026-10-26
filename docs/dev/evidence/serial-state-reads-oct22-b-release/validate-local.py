"""Validate the exact B-only Git commit using pinned existing native inputs."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

here = Path(__file__).resolve().parent
root = here.parents[3]
parser = argparse.ArgumentParser()
parser.add_argument('--commit', required=True)
args = parser.parse_args()
pins = json.loads((here / 'native-local-inputs.json').read_text())

def output(argv, cwd=root):
    return subprocess.check_output(argv, cwd=cwd, text=True).strip()

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def inputs():
    return {'go_sha256': sha(pins['go_path']),
            'go_version': output([pins['go_path'], 'version']),
            'native_sha256': sha(pins['native_archive']),
            'native_bytes': Path(pins['native_archive']).stat().st_size,
            'native_architecture': output(['/usr/bin/lipo', '-info', pins['native_archive']]).split(': ')[-1],
            'native_gitlink': output(['git', '-C', pins['native_repo'], 'rev-parse', 'HEAD']),
            'header_sha256': sha(root / 'core/zksnark/zksnark_capi.h')}

def check_inputs(observed):
    for key, value in observed.items():
        if value != pins[key]:
            raise SystemExit(f'fixed input mismatch: {key}: {value!r} != {pins[key]!r}')

commit = output(['git', 'rev-parse', args.commit + '^{commit}'])
if len(args.commit) != 40 or commit != args.commit:
    raise SystemExit('supply the exact full commit SHA')
parent = output(['git', 'rev-parse', commit + '^'])
if parent != '9030ce7a31016491ece014ac76e5656ec149417e':
    raise SystemExit('unexpected B-only commit parent')
out = root / 'build/benchmarks/serial-state-reads-oct22-b-release' / commit
if out.exists():
    raise SystemExit('validation output already exists; do not overwrite evidence')
lock = open(pins['shared_lock'], 'a')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
before = inputs()
check_inputs(before)
out.mkdir(parents=True)
record = {'commit': commit, 'parent': parent,
          'tree': output(['git', 'rev-parse', commit + '^{tree}']),
          'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'inputs_before': before, 'checks': [], 'success': False}
manifest = out / 'validation-manifest.json'

def save():
    manifest.write_text(json.dumps(record, indent=2) + '\n')

save()
source = out / 'source'
try:
    subprocess.run(['git', 'clone', '--shared', '--no-checkout', '--quiet', str(root), str(source)], check=True)
    subprocess.run(['git', 'checkout', '--detach', '--quiet', commit], cwd=source, check=True)
    native = source / 'third_party/librustzcash'
    subprocess.run(['git', 'clone', '--shared', '--no-checkout', '--quiet', pins['native_repo'], str(native)], check=True)
    subprocess.run(['git', 'checkout', '--detach', '--quiet', pins['native_gitlink']], cwd=native, check=True)
    destination = native / 'target/release/librustzcash.a'
    destination.parent.mkdir(parents=True)
    shutil.copyfile(pins['native_archive'], destination)
    if sha(destination) != pins['native_sha256']:
        raise RuntimeError('copied native hash mismatch')
    if sha(source / 'core/zksnark/zksnark_capi.h') != pins['header_sha256']:
        raise RuntimeError('Git-object header hash mismatch')
    if output(['git', 'status', '--porcelain'], source):
        raise RuntimeError('source is not clean before validation')
    expected = json.loads((source / 'docs/dev/evidence/serial-state-reads-oct22-b-v2/export-manifest.json').read_text())['source_hashes']['candidate']
    for path, digest in expected.items():
        if sha(source / path) != digest:
            raise RuntimeError('B frozen source mismatch: ' + path)
    record['frozen_B_files'] = expected
    env = dict(os.environ, CGO_ENABLED='1', GOFLAGS='', GOTOOLCHAIN='local')
    binary = out / 'gtron'
    commands = [
        ('sapling-full', [pins['go_path'], 'test', '-tags=sapling', './...', '-count=1', '-timeout=900s']),
        ('blockbuffer-race', [pins['go_path'], 'test', '-race', '-tags=sapling', './core/blockbuffer', '-count=1', '-timeout=900s']),
        ('sapling-build', [pins['go_path'], 'build', '-buildvcs=true', '-tags=sapling', '-o', str(binary), './cmd/gtron']),
    ]
    for name, command in commands:
        check = {'name': name, 'command': command, 'cwd': str(source),
                 'env': {'CGO_ENABLED': '1', 'GOFLAGS': '', 'GOTOOLCHAIN': 'local'},
                 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
        record['checks'].append(check)
        save()
        with (out / (name + '.log')).open('w') as log:
            result = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
        check['exit'] = result.returncode
        check['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
        if result.returncode:
            raise RuntimeError(name + ' failed; raw evidence retained')
    metadata = output([pins['go_path'], 'version', '-m', str(binary)], source)
    (out / 'binary-build-info.txt').write_text(metadata + '\n')
    if ('vcs.revision=' + commit) not in metadata or 'vcs.modified=false' not in metadata:
        raise RuntimeError('binary VCS identity is not the clean B commit')
    record['binary_sha256'] = sha(binary)
    record['inputs_after'] = inputs()
    check_inputs(record['inputs_after'])
    if sha(destination) != pins['native_sha256'] or output(['git', 'status', '--porcelain'], source):
        raise RuntimeError('source/native input changed during validation')
    record['source_status_after'] = 'clean'
    record['success'] = True
finally:
    record['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
    fcntl.flock(lock, fcntl.LOCK_UN)
    lock.close()
print(manifest)
