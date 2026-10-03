#!/usr/bin/env python3
"""Run native executor-upgrade integration in a private Linux mount namespace.

Build: go test -c ./cmd/gtron -o /tmp/history-upgrade-native.test
Run as root: python3 scripts/dev/history_staging_upgrade_root_test.py /tmp/history-upgrade-native.test
No production file is modified. Only temporary fixture DBs are created.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('test_binary', type=Path)
    parser.add_argument('--inside', type=Path)
    args = parser.parse_args()
    if sys.platform != 'linux' or os.geteuid() != 0:
        raise RuntimeError('native integration requires Linux root')
    binary = args.test_binary.resolve(strict=True)
    namespace = os.readlink('/proc/self/ns/mnt')
    if args.inside is None:
        if not Path('/data/gtron').is_dir() or not Path('/var/lib').is_dir():
            raise RuntimeError('mount targets /data/gtron and /var/lib must already exist')
        with tempfile.TemporaryDirectory(prefix='gtron-upgrade-native-') as directory:
            fixture = Path(directory)
            app, var = fixture / 'app', fixture / 'var'
            (app / 'main').mkdir(parents=True, mode=0o700)
            (var / 'gtron-history-staging').mkdir(parents=True, mode=0o700)
            (app / 'start.lock').touch(mode=0o600)
            (app / 'main' / 'NATIVE_UPGRADE_TEST_ONLY').write_text('isolated mount namespace\n')
            env = dict(os.environ, GO_TRON_UPGRADE_HOST_MOUNT_NAMESPACE=namespace)
            subprocess.run(['unshare', '--mount', sys.executable, str(Path(__file__).resolve()),
                            str(binary), '--inside', str(fixture)], env=env, check=True)
        return
    if namespace == os.environ.get('GO_TRON_UPGRADE_HOST_MOUNT_NAMESPACE') or \
            'GO_TRON_UPGRADE_HOST_MOUNT_NAMESPACE' not in os.environ:
        raise RuntimeError('refusing mounts in the original host namespace')
    subprocess.run(['mount', '--make-rprivate', '/'], check=True)
    subprocess.run(['mount', '--bind', str(args.inside / 'app'), '/data/gtron'], check=True)
    subprocess.run(['mount', '--bind', str(args.inside / 'var'), '/var/lib'], check=True)
    env = dict(os.environ, GO_TRON_UPGRADE_ROOT_NAMESPACE='1')
    subprocess.run([str(binary), '-test.run=^TestHistoryStagingUpgradeNativeRoot$',
                    '-test.v', '-test.timeout=180s'], env=env, check=True)


if __name__ == '__main__':
    main()
