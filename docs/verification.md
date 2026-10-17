# Verification record

Verified on 18 October 2026 (Asia/Kolkata) on Ubuntu with Docker Desktop and a dedicated five-node kind cluster. Application source: `cfbb3b5`. Later delivery commits add deployment files and documentation.

## Go and shell checks

All checks passed:

```sh
go test -race -count=1 ./...
go vet ./...
test -z "$(gofmt -l cmd internal)"
make build
./bin/gfs help
for script in scripts/*.sh; do bash -n "$script" || exit; done
shellcheck -x scripts/local.sh scripts/kind-common.sh scripts/kind-up.sh \
  scripts/kind-down.sh scripts/kind-client.sh scripts/kind-smoke.sh
```

The host checks used Go 1.27.2, invoked directly from `/snap/go/current/bin/go` with `GOCACHE=/tmp/gfs-go-cache`. The container build separately passed using Go 1.25.5. ShellCheck ran from its official `koalaman/shellcheck:stable` container image.

The 16 Go tests cover:

- Path validation, bounded JSON, signed leases, tampering, and expiration.
- Atomic publication, concurrent publication conflicts, idempotent commit results, directory locking, and restart persistence.
- Corrupt checkpoints, failure to persist metadata, stale uploads after create/delete cycles, and checkpoint format migration.
- Immutable chunk writes, corruption healing, staging cleanup, incorrect size/checksum, and path traversal rejection.
- Authenticated primary-to-secondary replication and failure when a required secondary cannot acknowledge.
- Client replica failover and preservation of an existing download destination after failure.
- Binary and Unicode file flows, multiple chunks, overwrite, empty files, listing, deletion, and master restart.
- Node expiration, replica repair, corruption repair, and collection when an offline node returns.

No races were reported. A separate local process smoke test started the master and four chunkservers, round-tripped all 256 byte values through the actual CLI, and deleted the file. Stopping the runner closed all five listeners while retaining its metadata directory.

## Real Kubernetes failure test

The final deployment used image `gfs-lab:cfbb3b5-1792263530`, Kubernetes v1.34.0, one control-plane node, and four storage workers. The application used three replicas and 1 MiB chunks.

`./scripts/kind-smoke.sh` passed these checks:

1. Upload and download a file spanning four chunks; compare the bytes.
2. Stop `gfs-lab-worker`, which hosts `chunk-0`.
3. Download the same file successfully while that worker is stopped.
4. Wait for repair: three live chunkservers, zero under-replicated chunks, and zero unavailable chunks.
5. Restart the worker and observe four live chunkservers.
6. Recreate the master pod and verify the namespace and file bytes survive.
7. Verify empty files, conditional deletion, and rejection of invalid credentials.

The final result was recorded at `2026-10-17T19:00:47Z` in the ignored local file `.kind/smoke-result.json`:

```text
File:   /smoke/1792263609-2793986/file.bin
SHA256: 3f468fb6948f19998755e73a2b1d97416aad2de394cd25bb837893e297c81f1f
Final:  4 live chunkservers, 0 under-replicated chunks, 0 unavailable chunks
```

A file from the previous deployment also survived the rolling application update and checkpoint format migration with its original SHA-256. All workers stopped by testing were restarted. The cluster was left running after verification.

## Limits of this evidence

These checks cover local processes, container failure, application recovery, and pod restart. They do not simulate host power loss or independent physical disk failure. All kind nodes share one host. Full cluster deletion and recreation, sustained capacity, and production operation were not tested.

The GitHub Actions workflow was added for repeatable formatting, vet, race-test, build, and shell syntax checks. Its remote execution was not part of this local verification. The kind failure test remains an explicit local check.
