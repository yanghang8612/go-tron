#!/usr/bin/env python3
"""Root-owned systemd pre-start guard for the opt-in shared-history reader.

This guard protects the configured systemd service, not arbitrary direct binary
execution. Its pinned reader remains mandatory even if the data marker vanishes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import sys

FLAG = '--history.cross-block-dedup='


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def parse_commands(value):
    """Strict systemd command-array parsing; volatile execution state is ignored."""
    require(isinstance(value, str) and len(value) <= 65536, 'invalid command serialization')
    if not value:
        return []
    commands, end = [], 0
    for match in re.finditer(r'\{[^{}]*\}', value):
        separator = value[end:match.start()].strip()
        require(separator in ('', ';') and (commands or not separator), 'unknown command separator')
        fields = {}
        for part in re.split(r'\s*;\s*', match.group()[1:-1]):
            key, equal, field = part.partition('=')
            key, field = key.strip(), field.strip()
            require(equal and field and key not in fields, 'missing/duplicate command field')
            fields[key] = field
        require(set(fields) == {'path', 'argv[]', 'ignore_errors', 'start_time',
                                'stop_time', 'pid', 'code', 'status'}, 'unknown command fields')
        require(fields['path'].startswith('/') and '\n' not in fields['path'] and
                (fields['argv[]'] == fields['path'] or fields['argv[]'].startswith(fields['path'] + ' ')),
                'invalid executable/argv')
        require(fields['ignore_errors'] in ('yes', 'no') and fields['pid'].isdigit(), 'invalid command state')
        require(all(fields[n].startswith('[') and fields[n].endswith(']') for n in ('start_time', 'stop_time')),
                'invalid command timestamps')
        require(re.fullmatch(r'(\(null\)|[a-zA-Z_]+|[0-9]+)', fields['code']) and
                re.fullmatch(r'[0-9]+(?:/(?:[0-9]+|[A-Z][A-Z0-9_]*))?', fields['status']),
                'invalid command exit status')
        commands.append({name: fields[name] for name in ('path', 'argv[]', 'ignore_errors')})
        end = match.end()
    require(commands and not value[end:].strip(), 'unknown/truncated command serialization')
    return commands


def read_root_file(path, limit):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022 and
                info.st_size <= limit, 'unsafe root-owned file: ' + str(path))
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'file exceeded read limit')
    return data


def hash_root_file(path, limit):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    digest, total = hashlib.sha256(), 0
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022 and
                info.st_size <= limit, 'unsafe root-owned binary: ' + str(path))
        while True:
            data = stream.read(1 << 20)
            if not data:
                break
            total += len(data)
            require(total <= limit, 'binary exceeded hash limit')
            digest.update(data)
        after = os.fstat(stream.fileno())
        require((info.st_size, info.st_mtime_ns, info.st_ctime_ns) ==
                (after.st_size, after.st_mtime_ns, after.st_ctime_ns) and total == info.st_size,
                'binary changed during hashing')
    return digest.hexdigest()


def reader_marker(binary, checksum, source):
    require(re.fullmatch('[0-9a-f]{64}', checksum or '') and re.fullmatch('[0-9a-f]{40}', source or ''),
            'invalid reader identity')
    return {'version': 1, 'required_reader': 3, 'bucket_blocks': 1024,
            'binary': str(binary), 'binary_sha256': checksum, 'source_commit': source}


def check_command(value, binary, checksum, source, marker):
    commands = parse_commands(value)
    require(len(commands) == 1 and commands[0]['ignore_errors'] == 'no', 'expected one mandatory ExecStart')
    command = commands[0]
    argv = shlex.split(command['argv[]'])
    require(command['path'] == str(binary) and argv and argv[0] == str(binary),
            'configured executable is not the pinned v3 reader')
    flags = [arg for arg in argv if arg.startswith(FLAG) or arg == FLAG[:-1]]
    require(flags in ([FLAG + 'false'], [FLAG + 'true']), 'one explicit shared-history writer flag required')
    expected = reader_marker(binary, checksum, source)
    if marker is None:
        require(flags == [FLAG + 'false'], 'writer enabled before durable reader marker')
    else:
        require(marker == expected, 'minimum reader marker identity changed')
    return flags[0] == FLAG + 'true'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--marker', required=True)
    args = parser.parse_args()
    require(Path(args.binary).is_absolute() and Path(args.marker).is_absolute(), 'absolute fixed paths required')
    marker_path = Path(args.marker)
    marker = json.loads(read_root_file(marker_path, 4096)) if os.path.lexists(marker_path) else None
    output = subprocess.check_output(['/bin/systemctl', 'show', 'gtron.service', '--property=ExecStart', '--no-pager'],
                                     stdin=subprocess.DEVNULL, timeout=15).decode('utf-8').strip()
    require(output.startswith('ExecStart='), 'missing effective ExecStart')
    enabled = check_command(output[len('ExecStart='):], args.binary, args.sha256, args.source, marker)
    # The service account may read these immutable bytes; only root may change
    # the file or guard's pinned arguments in the systemd drop-in.
    digest = hash_root_file(args.binary, 512 << 20)
    require(digest == args.sha256, 'configured reader binary checksum changed')
    print(json.dumps({'reader_guard': True, 'writer_enabled': enabled, 'source_commit': args.source}))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('shared history reader guard: ' + str(error), file=sys.stderr)
        sys.exit(1)
