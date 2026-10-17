# GFS in Go

A runnable GFS-inspired learning system: one master, four chunkservers, three replicas per chunk, a streaming CLI, and a reproducible five-node kind cluster. All application code is Go and uses the standard library.

The client asks the master where to store chunks, writes directly to the selected primary, and publishes the file only after every replica has persisted and verified the data. Reads go directly to chunkservers and fall back to another copy if a server fails or returns corrupt bytes. The master detects missing heartbeats and repairs replication.

**This implements whole-file uploads and atomic replacements using immutable generations.** It does not implement GFS record append, arbitrary in-place writes, a POSIX mount, or multiple masters. See [design and failure semantics](docs/design.md).

## Run in kind

Requirements: Linux or macOS, Docker running, kind, Bash 4.3+, `jq`, and Git. Allow roughly 6 GiB of Docker memory for the lab and at least 2 GiB free disk space beyond images and data. No cloud account is needed.

```sh
./scripts/kind-up.sh
./scripts/kind-client.sh status
./scripts/kind-smoke.sh
```

The setup builds the Go image, creates a dedicated `gfs-lab` cluster, and starts the master, four chunkservers, and a CLI pod. It takes a matching `kubectl` from the node image. It keeps its kubeconfig in `.kind/kubeconfig`; your default Kubernetes context is not changed.

The smoke test uploads a file spanning four chunks, stops one **GFS storage worker**, verifies a byte-identical download, waits for three live replicas, restarts the worker, and restarts the master to verify persistence. It also checks empty files, deletion, and rejected credentials. The worker is restarted on test failure. Results are saved in `.kind/smoke-result.json`.

The CLI pod is inside the cluster so it can reach chunkserver DNS names directly. Upload a host file and download it again:

```sh
export KUBECONFIG="$PWD/.kind/kubeconfig"
client_pod=$(./bin/kubectl --context kind-gfs-lab -n gfs get pod -l app=client -o jsonpath='{.items[0].metadata.name}')
./bin/kubectl --context kind-gfs-lab -n gfs cp ./example.bin "$client_pod:/work/example.bin"
./scripts/kind-client.sh put /work/example.bin /examples/example.bin
./scripts/kind-client.sh ls /examples/
./scripts/kind-client.sh stat /examples/example.bin
./scripts/kind-client.sh get /examples/example.bin /work/download.bin
./bin/kubectl --context kind-gfs-lab -n gfs cp "$client_pod:/work/download.bin" ./download.bin
cmp ./example.bin ./download.bin
./scripts/kind-client.sh rm /examples/example.bin
```

Run `kind-up.sh` again after changing code to rebuild and roll out a new image. For a cluster teardown that **retains GFS data and its token**:

```sh
./scripts/kind-down.sh
```

The master and chunkserver data live in `.kind/data/`, mounted into individual kind nodes. The declared 5 GiB PV capacity is scheduling metadata, not a host-disk quota. Local volumes pin each chunkserver to its worker. Deleting the cluster or pods does not delete those host directories. The CLI pod's `/work` is temporary; copy wanted downloads back to the host. Do not manually delete `.kind/data` unless you intend to destroy the files and namespace. Back up all persistent directories together while the lab is stopped.

## Run without Kubernetes

Requirements: Go 1.23 or newer and Bash 4.3+. The local runner starts five separate processes, binds only loopback interfaces, and retains data on exit.

```sh
make build
./scripts/local.sh
```

In another terminal in the same directory:

```sh
export GFS_TOKEN_FILE="$PWD/.data/local/token"
export MASTER_URL=http://127.0.0.1:8080
./bin/gfs put ./example.bin /examples/example.bin
./bin/gfs get /examples/example.bin ./download.bin
./bin/gfs status
```

Use `GFS_MASTER_PORT` and `GFS_CHUNK_PORT_BASE` when the default ports 8080 and 9001–9004 are occupied. Set `GFS_DATA_ROOT` for another persistent location. The runner generates a private token file; it never prints the token. Ctrl-C stops only the processes started by the runner.

To run each component independently, see `./bin/gfs master -h` and `./bin/gfs chunk -h`. All configuration is loaded at startup. Restart the process after changing it.

| Setting | Default | Purpose |
| --- | --- | --- |
| `GFS_TOKEN` or `GFS_TOKEN_FILE` | Required | Shared lab token, at least 24 characters |
| `MASTER_URL` | `http://127.0.0.1:8080` | Master endpoint for clients and chunkservers |
| `DATA_DIR` | Role-specific `.data` path | Master checkpoint or chunk files |
| `LISTEN_ADDR` | `:8080` master / `:9000` chunk | Server listen address |
| `NODE_ID`, `ADVERTISE_URL` | Required for chunkservers | Unique identity and reachable HTTP origin |
| `REPLICATION` | `3` | Required replicas before publishing |
| `CHUNK_SIZE` | `67108864` (64 MiB) | Maximum chunk size; kind uses 1 MiB for the demo |
| `NODE_TIMEOUT` | `10s` | Heartbeat expiration |
| `HEARTBEAT_INTERVAL` | `3s` | Inventory reporting interval |
| `REPAIR_INTERVAL` | `3s` | Master maintenance interval |
| `UPLOAD_TTL` | `15m` | Upload and placement lease lifetime |
| `GC_GRACE` | `1h` | Retention after chunks become unreferenced |

`GC_GRACE` must be at least `UPLOAD_TTL + 120s`. Files are limited to 1 GiB and 4096 chunks, whichever limit is reached first. Each chunkserver allows four concurrent mutations and streams bytes with bounded buffers. The token grants full lab access, including internal APIs; it is not a tenant permission model. HTTP has no TLS. Use the lab on a trusted machine/network.

## Check the implementation

```sh
make test       # go test -race -count=1 ./...
make vet        # go vet ./...
make build
./scripts/kind-smoke.sh
```

GitHub Actions runs formatting, vet, race tests, build, and shell syntax checks. The real Kubernetes failure test is an explicit local step. See [verification record](docs/verification.md) for the checks performed during implementation.

| Code | Responsibility |
| --- | --- |
| `cmd/gfs` | Server lifecycle, configuration, and CLI |
| `internal/protocol` | HTTP contract, validation, token checks, signed leases |
| `internal/metadata` | Durable checkpoint, atomic publication, tombstones |
| `internal/chunk` | Chunk persistence, verification, replication, inventories |
| `internal/master` | Placement, namespace APIs, liveness, repair, collection |
| `internal/client` | Streaming upload/download and checksum failover |
| `internal/integration` | Complete file-flow and concurrent-write tests |
| `k8s`, `scripts` | Local and kind setup, data placement, failure test |
