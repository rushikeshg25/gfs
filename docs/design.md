# Go GFS learning system

A Go client asks the master for chunk placement, sends data to a primary chunkserver, and publishes a complete file generation only after all three replicas have persisted and verified their bytes. Reads go directly to replicas, with checksum verification and failover. Four chunkservers leave capacity for repair after one worker fails.

Files use immutable generations. The master allocates unique chunk IDs and signed, expiring write leases. A compare-and-swap publishes the new generation atomically. This implements safe whole-file replacement, rather than the paper's in-place mutations, record append or POSIX semantics. Expired or stale primaries cannot alter published chunks.

The implementation uses only Go's standard library. Master metadata is an atomically replaced, fsynced JSON checkpoint, protected by a process lock. Updating it costs O(total metadata), a deliberate lab limit rather than a scalable metadata database. Chunk data and checksums are atomically published together after fsync. Heartbeats report verified inventories. The master restores replication and retains deletion tombstones for returning nodes.

Defaults: 64 MiB chunks, three replicas, four chunkservers, 1 GiB maximum file, at most 4096 chunks per file. There is one master and no automatic master failover. Shared-token HTTP is for a trusted local lab; it is not tenant isolation or transport encryption. All kind nodes share the host hardware.

Acceptance: upload a multichunk file, stop a worker, download identical bytes, and restore three live replicas. No cloud resources are needed. Keep at least 2 GiB free; data needs roughly three times input bytes plus old generations and temporary writes. Cluster data survives teardown in the host data directory.

References: [GFS paper](https://research.google/pubs/the-google-file-system/), [kind configuration](https://kind.sigs.k8s.io/docs/user/configuration/), [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).
