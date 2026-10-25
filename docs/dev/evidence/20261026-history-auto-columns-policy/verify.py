#!/usr/bin/env python3
"""Read-only local policy evidence verifier; never executes archived scripts."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import zipfile

HERE = pathlib.Path(__file__).resolve().parent
ARCHIVE_SHA256 = 'b0211b34fe0ab3d39ac7df958a03018318298506c6add0b5fa9faa7b550acebf'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-root', type=pathlib.Path, default=HERE.parents[3])
    parser.add_argument('--binary', type=pathlib.Path)
    args = parser.parse_args()
    path = HERE / 'local-policy-checks.zip'
    assert sha(path.read_bytes()) == ARCHIVE_SHA256
    with zipfile.ZipFile(path) as z:
        assert z.testzip() is None
        manifest = json.loads(z.read('evidence-manifest.json'))
        assert len(z.namelist()) == len(set(z.namelist()))
        assert set(z.namelist()) == set(manifest) | {'evidence-manifest.json'}
        files = {}
        for name, digest in manifest.items():
            n = pathlib.PurePosixPath(name)
            assert not n.is_absolute() and '..' not in n.parts
            files[name] = z.read(name)
            assert sha(files[name]) == digest, name
    status = json.loads(files['local-status.json'])
    scope = json.loads(files['scope.json'])
    assert status['complete'] and status['phase'] == 'complete'
    assert scope['local_complete'] and not scope['native_policy_complete']
    assert not status['production_mutations'] and not scope['production_mutations']
    labels = {'focused', 'packages', 'race', 'vet', 'sapling-build'}
    assert {c['label'] for c in status['checks']} == labels
    for check in status['checks']:
        assert check['returncode'] == 0 and check['seconds'] > 0
        assert sha(files[check['label']+'.log']) == check['log_sha256']
    packages = {line.split()[1] for line in files['packages.log'].decode().splitlines() if line.startswith('ok')}
    assert packages == {'github.com/tronprotocol/go-tron/'+n for n in
                        ('core/rawdb', 'core/state', 'core/state/snapshots', 'core', 'cmd/gtron')}
    for test in ('TestAutoColumnsOpaqueCDCAndOrdinaryBuild', 'TestAutoColumnsUnorderedFallbackKeepsResolvedPolicy',
                 'TestAutoColumnsMixedMergeAndRestore', 'TestAutoColumnsLegacyFactoryAndMigrationCompatibility',
                 'TestAutoColumnsGenericCompressionFooterAdmission', 'TestRuntimeHistoryCompressionPrecedence',
                 'TestDBHistorySharedPipelineFullTrioByteEquivalence'):
        assert '--- PASS: '+test in files['focused.log'].decode(), test
    source = json.loads(files['source-inventory.json'])
    baseline = json.loads(files['baseline-source-inventory.json'])
    assert len(source) == status['source_files'] == scope['source_files'] == 2146
    assert len(baseline) == scope['frozen_source_check']['files'] == 2145
    assert not scope['frozen_source_check']['mismatches']
    assert sha(files['source-inventory.json']) == status['source_inventory_sha256'] == scope['source_inventory_sha256']
    assert sha(files['baseline-source-inventory.json']) == scope['baseline_source_inventory_sha256']
    changed = {n for n, h in source.items() if baseline.get(n) != h}
    assert len(changed) == 14 and changed == set(scope['changed_source_files'])
    assert set(source)-set(baseline) == {'core/state/snapshots/history_auto_columns_policy_test.go'}
    assert {n.removeprefix('source/') for n in files if n.startswith('source/')} == changed
    for name in changed:
        assert sha(files['source/'+name]) == source[name]
    actual_names = set(subprocess.check_output(['rg', '--files', '-g', '*.go', '-g', 'go.mod', '-g', 'go.sum'], cwd=args.source_root, text=True).splitlines())
    assert actual_names == set(source), 'Current source file set differs'
    for name, expected in source.items():
        assert sha((args.source_root/name).read_bytes()) == expected, name
    assert sha(files['tracked-source.patch']) == status['tracked_patch_sha256']
    first = json.loads(files['first-revision/local-status.json'])
    assert not first['complete'] and first['phase'] == 'failed' and first['error'] == 'KeyboardInterrupt()'
    assert [c['label'] for c in first['checks']] == ['focused']
    interrupted = json.loads(files['first-revision/interrupted-run.json'])
    assert interrupted['server_jobs_untouched'] and interrupted['pid'] in interrupted['verified_owned_pids']
    live = files['frozen-experiment/server-progress-live-14.json']
    meta = json.loads(files['frozen-experiment/server-progress-live-14.json.transport.json'])
    assert meta['nonce'] and meta['name'] == 'progress-live-14.json'
    assert meta['bytes'] == len(live) and meta['sha256'] == sha(live)
    live = json.loads(live)
    assert live['progress']['files'] == 3650 and not live['progress']['complete']
    assert all(j['proc_exists'] and j['pid'] for j in live['jobs'].values())
    assert '391 28550' in live['jobs']['capture']['ps']
    assert not live['acceptance']['complete'] and not live['publish']['complete']
    assert not live['production_mutations']
    if args.binary:
        assert sha(args.binary.read_bytes()) == status['binary_sha256']
    print(json.dumps(dict(source_files_verified=len(source), changed_source_files=len(changed),
                          local_checks_passed=len(labels), first_incomplete_revision_retained=True,
                          frozen_experiment_live_files=live['progress']['files'], native_policy_complete=False,
                          full_fixed_copy_acceptance_complete=False, production_mutations=False), indent=2))


if __name__ == '__main__':
    main()
