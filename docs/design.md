# Go GFS learning system

A Go client asks the master for chunk placement, sends data to a primary chunkserver, and publishes a complete file generation only after all three replicas have persisted and verified their bytes. Reads go directly to replicas, with checksum verification and failover. Four chunkservers leave capacity for repair after one worker fails.

Files use immutable generations. The master allocates unique chunk IDs and signed, expiring write leases. A compare-and-swap publishes the new generation atomically. This implements safe whole-file replacement, rather than the paper's in-place mutations, record append or POSIX semantics. Expired or stale primaries cannot alter published chunks.

The implementation uses only Go's standard library. Master metadata is an atomically replaced, fsynced JSON checkpoint, protected by a process lock. Updating it costs O(total metadata), a deliberate lab limit rather than a scalable metadata database. Chunk data and checksums are atomically published together after fsync. Heartbeats report verified inventories. The master restores replication and retains deletion tombstones for returning nodes.

Defaults: 64 MiB chunks, three replicas, four chunkservers, 1 GiB maximum file, at most 4096 chunks per file. There is one master and no automatic master failover. Shared-token HTTP is for a trusted local lab; it is not tenant isolation or transport encryption. All kind nodes share the host hardware.

Acceptance: upload a multichunk file, stop a worker, download identical bytes, and restore three live replicas. No cloud resources are needed. Keep at least 2 GiB free; data needs roughly three times input bytes plus old generations and temporary writes. Cluster data survives teardown in the host data directory.

References: [GFS paper](https://research.google/pubs/the-google-file-system/), [kind configuration](https://kind.sigs.k8s.io/docs/user/configuration/), [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).

## Publication and failure rules

1. The client hashes one bounded chunk at a time and sends descriptors to the master. The master captures the path version and chooses three distinct live chunkservers per chunk.
2. The client streams bytes to the selected primary under an HMAC-signed lease. The primary verifies size and SHA-256, persists the chunk, then forwards the same bytes to secondaries. Each replica syncs data, metadata, the staging directory, and the parent directory before acknowledging publication.
3. The master independently checks every selected copy. It then persists the complete manifest and a new namespace version in one checkpoint update. Until this succeeds, readers see the previous complete generation or a missing file.
4. A competing publication or deletion changes the path version and causes the old upload to fail with a conflict. Version tombstones also cover absent paths, preventing an upload started before a create/delete cycle from recreating the path unexpectedly.
5. A retry of a committed upload returns its original manifest. The client resolves an uncertain commit response before attempting to abort an upload. It reports the upload ID if the master is unavailable and the result cannot be determined.

Chunk IDs are immutable and never reused by the master. Thus a delayed primary can at worst finish writing an unreferenced staged chunk; it cannot modify a newer file generation. This is different from implementing the original paper's mutable-chunk primary lease protocol.

## Recovery and retention

Heartbeats include a checksum-verified inventory plus the IDs of physical directories, including corrupt copies. Replica locations and liveness are rebuilt in memory after a master restart. Existing files remain durable, but clients may need to wait for the next heartbeat before reading them. New uploads require the full replication factor; reads can succeed with one valid replica.

The master repairs committed chunks with fewer than three live copies by asking another chunkserver to copy from a surviving replica. The target verifies the bytes before acknowledgment. If every copy is missing or corrupt, the file is unavailable; the master reports it and cannot manufacture lost data. Extra healthy replicas after a worker rejoins are retained. This favors preservation over aggressively deleting surplus copies in a small lab.

Overwritten, deleted, aborted, and expired-upload chunks become candidates for collection. The master first records their retirement time, then waits for `GC_GRACE`. Tombstones are retained so a server that was offline during collection also deletes its obsolete copies when it returns. Downloads that outlast this retention window can fail; failed downloads leave the previous local destination unchanged. This lab does not provide indefinite snapshot read leases.

The checkpoint contains a checksum, format version, namespace versions, manifests, upload records, and chunk retirement records. Corrupt metadata causes startup to fail instead of silently replacing the namespace. A persistence error makes the running store stop serving metadata until restart. An exclusive directory lock prevents two local masters from sharing the same data directory. Checkpoint format 1 from the initial development build upgrades to format 2 on startup without deleting files.

## Limits and trade-offs

- Metadata operations serialize and rewrite the checkpoint. Historical uploads, namespace versions, and chunk tombstones are retained. This deliberately favors inspectability over long-lived large-catalog performance.
- Heartbeats scrub every local chunk; large inventories consume disk bandwidth and may need a longer node timeout. Inventories are limited to 50,000 entries per node. This is a small learning lab, not a capacity-tested service.
- The three replicas are on distinct Kubernetes workers, but all kind nodes run on the same physical host and share its disk. Tests cover process/container failure and recovery logic, not independent hardware failure or power-loss durability.
- The single master has persistent storage but no replica or automatic leader failover. Loss of its host directory requires a backup restore. Copying its checkpoint while it is being replaced is not the documented backup procedure; stop the lab first.
- A shared token authorizes both client and internal node operations. Treat every token holder as a trusted administrator. Tokens and kubeconfig files stay out of Git. No endpoints should be exposed to the public internet.
- SHA-256 is checked both on storage reads and by the client. File transfer uses bounded buffers; it does not load an entire file into memory. Peer connection establishment has a two-second deadline and each transfer has a 120-second deadline.

## Deployment boundaries

The kind cluster has one control-plane container and four worker containers. Kubernetes runs the GFS master as an application pod on the control-plane node, and each chunkserver is pinned by its local PV to a different worker. The GFS master is independent of Kubernetes' control plane.

Kubernetes restarts application pods. GFS detects missing chunkserver heartbeats and repairs chunk replicas. A local PV cannot move a failed worker's disk to another node; that is why GFS copies a surviving replica onto another worker. The supplied host mounts retain each node's data across cluster recreation.

## HTTP surface

All endpoints except health/readiness probes require `Authorization: Bearer <token>`. The Go client is the supported user interface; these routes also make the protocol easy to inspect.

| Component | Route | Meaning |
| --- | --- | --- |
| Master | `GET /status` | Live nodes, replication deficits, unavailable chunks, latest repair error |
| Master | `GET /files?prefix=...` | List published files |
| Master | `GET /file?path=...` | Published manifest with current replica locations |
| Master | `DELETE /file?path=...` | Delete only if the `If-Match` generation still matches |
| Master | `POST /uploads` | Allocate chunks and return signed placement leases |
| Master | `GET /uploads/{id}` | Resolve an upload's current state |
| Master | `POST /uploads/{id}/commit` | Verify all selected copies and atomically publish |
| Master | `DELETE /uploads/{id}` | Abort an uncommitted upload |
| Master | `POST /heartbeat` | Register the node's verified and physical inventories |
| Chunkserver | `PUT /chunks/{id}` | Store under `X-GFS-Lease`; primary forwards to secondaries |
| Chunkserver | `GET /chunks/{id}` | Return verified bytes; HTTP byte ranges are supported |
| Chunkserver | `GET /chunks/{id}/meta` | Verify the stored bytes and return their descriptor |
| Chunkserver | `POST /repair` | Copy a signed chunk from an authorized source replica |
| Chunkserver | `DELETE /chunks/{id}` | Remove a retired chunk |

An upload request contains `path`, total `size`, and `chunks` with each chunk's `size` and `sha256`. An optional `base` generation is a caller-specified precondition; use the empty string to require an absent file. Internal namespace versions additionally guard against intervening create/delete cycles.
