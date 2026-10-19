"""Reproduce on matching 659ab078 sources using temporary Go overlays only."""
from pathlib import Path
import argparse
import json
import os
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument('--repo', required=True)
args = parser.parse_args()
repo = Path(args.repo).resolve()
evidence = Path(__file__).resolve().parent
env = dict(os.environ, GOTOOLCHAIN='go1.25.5')
with tempfile.TemporaryDirectory(prefix='gtron-cold-latency-') as scratch:
    scratch = Path(scratch)
    sources = {}
    for relative in ['core/rawdb/accessors_state_changeset.go', 'core/state/history.go']:
        original = subprocess.check_output(['git', 'show', '659ab078:'+relative], cwd=repo).decode()
        assert (repo/relative).read_text() == original, 'requires matching deployed runtime sources'
        sources[relative] = original
    rawdb = sources['core/rawdb/accessors_state_changeset.go']
    marker = rawdb.index('func ReadFirstStateDomainChangeByKeyBlockRangeContext(')
    old = '\t\tif err != nil || !ok {\n\t\t\treturn nil, err\n\t\t}'
    new = '\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n\t\tif !ok {\n\t\t\tbreak\n\t\t}'
    fixed_rawdb = rawdb[:marker] + rawdb[marker:].replace(old, new, 1)
    assert fixed_rawdb != rawdb
    history = sources['core/state/history.go']
    old = 'r.readStateAccountKVAsOf(addr, kvdomains.ContractMetadata, contractMetaKVKey, blockNum, r.headNum)'
    new = 'r.readStateKVAsOf(addr, envelope.AccountKVGeneration, kvdomains.ContractMetadata, contractMetaKVKey, blockNum, r.headNum)'
    assert history.count(old) == 1
    fixed_history = history.replace(old, new)
    replacements = {
        str(repo/'core/rawdb/cold_latency_diagnostic_test.go'): str(evidence/'testdata/diagnostic_rawdb_test.go'),
        str(repo/'core/state/cold_latency_diagnostic_test.go'): str(evidence/'testdata/diagnostic_state_test.go'),
    }
    def run(name, packages, expression):
        overlay = scratch/(name+'.json')
        overlay.write_text(json.dumps({'Replace':replacements}))
        result = subprocess.run(['go','test','-overlay='+str(overlay)]+packages+
            ['-run',expression,'-count=1','-timeout=180s','-v'], cwd=repo, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(result.stdout.decode(), end='')
        return result
    baseline = run('baseline',['./core/rawdb'],'^TestDiagnosticColdLatencyFirstSeekUnindexedTail$')
    assert baseline.returncode != 0 and b'first-change seek missed unindexed block 3' in baseline.stdout
    for relative, contents in [('core/rawdb/accessors_state_changeset.go',fixed_rawdb),
                               ('core/state/history.go',fixed_history)]:
        target = scratch/Path(relative).name
        target.write_text(contents)
        replacements[str(repo/relative)] = str(target)
    candidate = run('candidate',['./core/rawdb','./core/state'],
        'TestDiagnosticColdLatency|TestReadFirstStateDomainChange|TestStateDomainChangeBlockRangeReadsScanUnindexedStageTail|TestPersistentHistoryReader|TestStateAccountV5.*History|TestContract.*History')
    assert candidate.returncode == 0
print('Reproduced baseline tail omission and passing isolated candidate; repository runtime sources unchanged.')
