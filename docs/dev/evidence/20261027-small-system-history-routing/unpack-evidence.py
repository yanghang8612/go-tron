"""Verify the sealed archive and every indexed member before extracting."""
import hashlib
import json
import pathlib
import sys
import tarfile

archive, receipt, output = map(pathlib.Path, sys.argv[1:4])
seal = json.loads(receipt.read_text())
digest = hashlib.sha256()
with archive.open("rb") as stream:
    for data in iter(lambda: stream.read(4 << 20), b""):
        digest.update(data)
assert archive.stat().st_size == seal["bytes"]
assert digest.hexdigest() == seal["sha256"]
assert not output.exists(), "Choose a new extraction directory"
with tarfile.open(archive, "r:xz") as bundle:
    members = bundle.getmembers()
    assert len(members) == seal["members"]
    assert len({member.name for member in members}) == len(members)
    assert sum(member.size for member in members) < 800_000_000
    for member in members:
        name = pathlib.PurePosixPath(member.name)
        assert member.isfile() and not name.is_absolute()
        assert ".." not in name.parts and str(name) == member.name
    index = json.load(bundle.extractfile("evidence-index.json"))
    assert set(index) == {m.name for m in members} - {"evidence-index.json"}
    for member in members:
        if member.name == "evidence-index.json":
            continue
        expected = index[member.name]
        assert member.size == expected["bytes"]
        actual = hashlib.sha256()
        with bundle.extractfile(member) as stream:
            for data in iter(lambda: stream.read(4 << 20), b""):
                actual.update(data)
        assert actual.hexdigest() == expected["sha256"], member.name
    output.mkdir(parents=True)
    for member in members:
        target = output / member.name
        target.parent.mkdir(parents=True, exist_ok=True)
        with bundle.extractfile(member) as source, target.open("xb") as dest:
            for data in iter(lambda: source.read(4 << 20), b""):
                dest.write(data)
print(json.dumps(dict(complete=True, members=len(members), sha256=seal["sha256"])))
