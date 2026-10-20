from pathlib import Path
import re,json,collections
p=Path(__file__).resolve().parent/'cpu-20261020T005832';raw=(p/'query-raw.txt').read_text();sampletext,rest=raw.split('Locations\n',1);loctext=rest.split('Mappings',1)[0]
loc={};current=None
for line in loctext.splitlines():
 m=re.match(r'^\s*(\d+):',line)
 if m:current=int(m.group(1));loc[current]=line
 elif current is not None:loc[current]+='\n'+line
samples=[]
for line in sampletext.splitlines():
 m=re.match(r'^\s*(\d+)\s+(\d+):\s+([\d ]+)',line)
 if m:
  chain='\n'.join(loc[int(x)] for x in m.group(3).split())
  if 'GetStorageAtBlockContext' in chain:samples.append((int(m.group(2)),chain))
functions=['readStateAccountKVAsOf','IterateStateDomainChangesByBlockRangeContext','decodePersistedStateDomainChangeBlock','rlp.DecodeBytes','(*Manager).IterateStateDomainChangesByKey','openHistorySegmentForReadWithCacheLimitContext','zstd.(*Decoder).DecodeAll','historyIndexPages','openHistoryIndexPages','readStateAccountLatestAsOf','readStateKVAsOf','stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry']
total=sum(n for n,c in samples);out={'query_cpu_ns':total,'matched_sample_rows':len(samples),'cumulative_functions':{},'exclusive_high_level_phases_ns':dict(collections.Counter()),'note':'All GetStorageAtBlockContext stacks, may include observer probes. CPU percentages, not wall time; inclusive functions overlap.'}
for f in functions:
 n=sum(n for n,c in samples if f in c);out['cumulative_functions'][f]={'cpu_ns':n,'percent_of_query_cpu':100*n/total}
phases=collections.Counter()
for n,c in samples:phases['metadata' if 'readStateAccountKVAsOf' in c else 'account' if 'readStateAccountLatestAsOf' in c else 'storage' if 'readStateKVAsOf' in c else 'other']+=n
out['exclusive_high_level_phases_ns']=dict(phases);(p/'attribution.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
sp=Path(__file__).resolve().parent/'live-20261020T005730';stacks=json.loads((sp/'active-query-stacks.json').read_text());s=[]
for x in stacks:
 text=x['stack'];ranges=[]
 for m in re.finditer(r'IterateStateDomainChangesByBlockRangeContext\([^\n]*?\}, (0x[0-9a-f]+), (0x[0-9a-f]+),',text):
  a,b=[int(v,16) for v in m.groups()];ranges.append({'from':a,'to':b,'blocks':b-a+1})
 s.append({'file':x['file'],'state':text.splitlines()[0],'phase':'metadata' if 'readStateAccountKVAsOf' in text else 'account' if 'readStateAccountLatestAsOf' in text else 'other','tail_ranges':ranges,'cold':'.IterateStateDomainChangesByKey' in text,'pack_decode':'decodePersistedStateDomainChangeBlock' in text})
(sp/'stack-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))
