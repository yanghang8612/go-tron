#!/usr/bin/env python3
"""Recompute V11 receipts, storage accounting, HTTP and correctness evidence.

Requires Python >=3.9. This verifies archived observations, not a substitute for
rerunning the native Go transformer/scanner on the retained immutable inputs.
"""
import collections
import gzip
import hashlib
import json
import re
import statistics
from pathlib import Path

P=Path(__file__).resolve().parent
ROOT=P.parents[3]

def data(n):
    b=(P/n).read_bytes()
    return gzip.decompress(b) if n.endswith('.gz') else b

def read(n):return json.loads(data(n))
def lines(n):return list(map(json.loads,data(n).splitlines()))
for n,digest in read('SHA256SUMS.json').items():
    assert hashlib.sha256((P/n).read_bytes()).hexdigest()==digest,n
rows=lines('corpus-files.jsonl.gz')
ends={x['path']:x for x in read('end-output-checksums.json.gz')}
previous=P.parent/'20261025-history-key-owner'
prior_checks=json.loads((previous/'SHA256SUMS.json').read_text())
assert hashlib.sha256((previous/'corpus-files.jsonl.gz').read_bytes()).hexdigest()==prior_checks['corpus-files.jsonl.gz']
prior={x['candidate']['path']:x for x in map(json.loads,gzip.decompress((previous/'corpus-files.jsonl.gz').read_bytes()).splitlines())}
prior_ends=json.loads(gzip.decompress((previous/'end-output-checksums.json.gz').read_bytes()))
assert hashlib.sha256((previous/'end-output-checksums.json.gz').read_bytes()).hexdigest()==prior_checks['end-output-checksums.json.gz']
totals=collections.Counter();versions=collections.Counter();byname={}
for x in rows:
    name=x['source']['path'];v=x['audit'];e=ends[name];old=prior[name]
    assert name not in byname;byname[name]=x
    assert x['source']==old['candidate']
    assert v['SourceBytes']==x['source']['size'] and v['CandidateBytes']==x['candidate']['size']==e['size']
    assert x['source']['checksum']=='sha256:'+e['source_sha256']
    assert x['candidate']['checksum']=='sha256:'+e['sha256']
    assert x['source_inode']!=x['candidate_inode']
    assert x['source_inode']==old['candidate_inode']
    for k in ['Keys','Records','DictionaryDigest','OffsetBytes']:
        assert v[k]==old['audit'][k],(name,k)
    assert v['SourcePostingBytes']==old['audit']['PostingBytes']
    versions[v['Version']]+=1
    for k,z in v.items():
        if isinstance(z,int) and k!='Version':totals[k]+=z
space=read('space-final.json')
assert len(rows)==len(ends)==len(prior)==space['files']==19949
assert dict(totals)==space['counts']
assert {str(k):v for k,v in versions.items()}==space['formats']
assert totals['SourceBytes']==119501234404 and totals['Keys']==2443423104 and totals['Records']==14302423656
assert sum(e['allocated_bytes'] for e in ends.values())==space['candidate_allocated']
assert sum(e['allocated_bytes'] for e in prior_ends)==space['source_allocated']
assert space['saved_bytes']==totals['SourceBytes']-totals['CandidateBytes']
assert space['allocated_saved_bytes']==space['source_allocated']-space['candidate_allocated']
for side in ['source','candidate']:
    assert sum(c[side] for c in space['components'])==totals[side.title()+'Bytes']
assert sum(x['candidate']['size']<x['source']['size'] for x in rows)==space['smaller_files']
assert sum(x['candidate']['size']>x['source']['size'] for x in rows)==space['larger_files']
assert sum(x['candidate']['size']==x['source']['size'] for x in rows)==space['equal_files']
assert space['smaller_files']+space['larger_files']+space['equal_files']==19949
# Every growing file is included; no unconditional savings assertion per file.
assert totals['SourcePostingBytes']-totals['PostingBytes']==17729740632
complete=lines('complete-input-files.jsonl.gz')
allocation=read('complete-input-space-final.json')
assert allocation['complete'] and allocation['files']==len(complete)==4609
assert sum(x['audit']['Keys'] for x in complete)==292521921
per_file={x['path']:x for x in allocation['per_file']}
assert len(per_file)==len(complete)
for x in complete:
    r=per_file[x['source']['path']]
    assert r['source_bytes']==x['source']['size']==x['audit']['SourceBytes']
    assert r['candidate_bytes']==x['candidate']['size']==x['audit']['CandidateBytes']
    assert 'sha256:'+r['source_sha256']==x['source']['checksum']
    assert 'sha256:'+r['candidate_sha256']==x['candidate']['checksum']
for field in ['source_bytes','candidate_bytes','source_allocated_bytes','candidate_allocated_bytes']:
    assert allocation[field]==sum(r[field] for r in per_file.values())
assert allocation['saved_allocated_bytes']==allocation['source_allocated_bytes']-allocation['candidate_allocated_bytes']
routing=read('complete-input-routing.json')
assert routing['complete'] and routing['strict_reconstruction_passed'] and routing['canonical_frontiers_unchanged']
assert all(r['covered_pairs']==4609 and r['membership_payload_unchanged'] for r in routing['routes'])
canonical=read('canonical-companions-final.json')
assert canonical['complete'] and canonical['chain_frontiers_unchanged']
assert canonical['files']==len(canonical['per_file'])
assert canonical['bytes']==sum(x['bytes'] for x in canonical['per_file'])
assert all(x['canonical_ref_unchanged'] and x['baseline_inode']==x['candidate_inode'] for x in canonical['per_file'])
requests=lines('http-requests.jsonl.gz');assert len(requests)==286
resources={}
for z in lines('server-requests.jsonl.gz'):
    x=z['request']
    if x['id']==999999:continue
    k=z['mode'],x['id'];assert k not in resources;resources[k]=x
assert len(resources)==len(requests)
def io(s):return {k:int(v) for k,v in (line.split(':',1) for line in s.splitlines())}
def cpu(t):return t['Sec']*1000+t['Usec']/1000
groups=collections.defaultdict(list)
metrics=collections.defaultdict(lambda:collections.defaultdict(list))
for r in requests:
    assert r['correct'] and r['response']['id']==r['id'] and 'error' not in r['response']
    assert r['response']['result']==r['expected']
    x=resources[r['mode'],r['id']];assert x['params'][1]==r['tag']
    if r['cache'] not in ['warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process']:continue
    k=r['mode'],r['cache'],r['tag'];groups[k].append(r['milliseconds'])
    before=io(x['io_before']);after=io(x['io_after'])
    v=dict(milliseconds=r['milliseconds'],server_ms=x['elapsed_ns']/1e6,alloc_bytes=x['alloc_bytes'],
           cpu_ms=sum(cpu(x['usage_after'][f])-cpu(x['usage_before'][f]) for f in ['Utime','Stime']),
           **{f:after[f]-before[f] for f in ['rchar','syscr','read_bytes']})
    for f,z in v.items():metrics[k][f].append(z)
assert sum(map(len,groups.values()))==238 and len(groups)==42
for s in read('http-summary.json.gz'):
    v=groups[s['mode'],s['cache'],s['tag']]
    assert len(v)==s['n'] and statistics.median(v)==s['median_ms'] and min(v)==s['min_ms'] and max(v)==s['max_ms']
    assert len(v)=={'warm':10,'fresh-process-os-uncontrolled':3,'private-os-cold-fresh-process':4}[s['cache']]
for s in read('http-resource-summary.json'):
    v=metrics[s['mode'],s['cache'],s['tag']]
    for f,z in v.items():assert s[f]==dict(median=statistics.median(z),min=min(z),max=max(z)),(s['mode'],s['tag'],f)
h=read('http-final.json');proofs=read('cold-residency-proofs.json')
assert h['complete'] and h['all_responses_correct'] and h['requests']==286 and h['measured_requests']==238
assert len(proofs)==h['cold_residency_proofs']==56
assert max(x['fraction'] for x in proofs.values())==h['max_pre_start_residency']<0.01
for mode in ['baseline','candidate']:
    manifest=read('http-'+mode+'-private-files.json.gz')
    assert len(manifest)==h['private_inputs_sha_verified_files'][mode]
    assert all(x['sha256'] and x['source_inode']!=x['private_inode'] for x in manifest)
    assert read('nine-million-'+mode+'-workload.json')['responses_correct']
mu=read('muhash-census.json');restore=read('history-restore.json')
assert mu['complete'] and mu['muhash_verified'] and mu['source_rows']==[3484117,481,141418493]
assert bytes(mu['muhash_root']).hex()==h['head']['root']=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
assert restore['complete'] and sum(w['rows'] for w in restore['windows'])==22496
for w in restore['windows']:
    assert w['candidate_row_sha256']==w['full_row_sha256']==w['restored_row_sha256'] and w['range_rows_verified']==20
native=read('native-status.json.gz')
assert native['complete'] and not native['production_mutations']
assert native['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf'
assert len(native['production_source_inventory'])==1078
for name,digest in native['production_source_inventory'].items():
    assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==digest,name
local=read('local/local-verification.json')
assert local['complete'] and local['commit']=='b6a6fdf508b281f6864f888563e92eaa656be594' and not local['pushed']
for name,x in local['checks'].items():
    assert x['successful'] and hashlib.sha256((P/'local'/name).read_bytes()).hexdigest()==x['sha256']
for x in read('local/local-equivalence.json')['fixtures']:
    assert x['full_layout_checked'] and x['native_local_bytes_identical']
state=read('http-comparison-status.json');context=read('final-context.json')
assert state['complete'] and state['all_responses_correct'] and not state['environment_identity_changed']
assert state['before_identity']==state['after_identity']
assert context['MainPID']==str(state['after_identity']['pid'])
assert context['ActiveState']==state['after_identity']['active_state']
assert context['sha256']==state['after_identity']['sha256']
exit_observation=read('production-exit-observation.json')
assert exit_observation['read_only']
assert 'Stopping gtron mainnet snap node' in exit_observation['journal']
assert 'Clean shutdown' in exit_observation['journal']
assert 'ExecMainStatus=0' in exit_observation['systemctl']
assert 'ExecMainExitTimestamp=Sun 2026-10-25 11:13:43 UTC' in exit_observation['systemctl']
# Production had already stopped before both HTTP arms. Do not substitute its
# previous running identity or compare this window with earlier loaded runs.
assert state['before_identity']==dict(pid=0,sha256=None,active_state='inactive')
assert native['binaries']['candidate.test']==h['candidate_sha256']==state['candidate_binary_sha256']
assert native['binaries']['baseline.test']==h['baseline_sha256']==state['baseline_binary_sha256']
for expected in read('query-trace-summary.json'):
    name,mode=expected['name'],expected['mode'];markers=0;pending={};counts=collections.defaultdict(collections.Counter)
    prefix=Path('/data/gtron/history-tx-runs-20261025-01')/(mode+'-input')
    for line in data('traces/'+name+'-'+mode+'.trace.gz').decode(errors='replace').splitlines():
        if re.search(r'read\(\d+</proc/\d+/io>, "rchar:',line):markers+=1;continue
        if markers!=1:continue
        m=re.match(r'\s*(\d+)\s+(.*)',line)
        if not m:continue
        tid,body=m.groups()
        if '<unfinished ...>' in body:pending[tid]=body.split('<unfinished ...>')[0];continue
        if body.startswith('<... ') and ' resumed>' in body:
            before=pending.pop(tid,None)
            if before is None:continue
            body=before+body.split(' resumed>',1)[1]
        m=re.match(r'(read|pread64)\(\d+<([^>]+)>',body);n=re.search(r'= (\d+)\s*$',body)
        if not m or not n or int(n.group(1))==0:continue
        file=Path(m.group(2));size=int(n.group(1))
        try:file.relative_to(prefix)
        except ValueError:continue
        counts[file.suffix or '(no suffix)'].update(calls=1,bytes=size,small_reads=int(size<256))
    assert markers==expected['query_io_markers']==2 and dict(counts)==expected['by_suffix']
    for k,f in [('database_read_calls','calls'),('database_read_bytes','bytes'),('small_read_calls','small_reads')]:
        assert expected[k]==sum(x[f] for x in counts.values())
print('Verified 19,949 V11 cohort receipts; saved',space['saved_bytes'],'B; allocated saved',space['allocated_saved_bytes'],
      'B; 238 timed / 286 correct HTTP checks; MuHash; 22,496 restored rows; all 1,078 production source hashes.')
