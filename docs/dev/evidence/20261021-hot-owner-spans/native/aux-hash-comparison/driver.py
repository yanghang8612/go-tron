import subprocess,pathlib
p=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-02')
with (p/"aux-hash-bench.json").open("wb") as f:subprocess.run(['/usr/bin/env', 'GOMAXPROCS=1', 'GOMEMLIMIT=256MiB', 'GOTOOLCHAIN=local', 'GOCACHE=/root/.cache/go-build', 'GOPATH=/root/go', '/data/go/bin/go', 'run', '/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-02/aux-hash-bench.go'],cwd=p/"source",stdout=f,check=True)
