// Package master coordinates metadata and placement. Chunk bytes never pass through it.
package master

import (
	"context"
	"gfs/internal/metadata"
	p "gfs/internal/protocol"
	"net/http"
	"sort"
	"sync"
	"time"
)

type Config struct {
	Token                           string
	Replication                     int
	ChunkSize                       int64
	NodeTimeout, UploadTTL, GCGrace time.Duration
}
type node struct {
	p.Replica
	seenAt  time.Time
	chunks  map[string]p.Spec
	present map[string]bool
}
type Master struct {
	Store           *metadata.Store
	config          Config
	rpc             *p.RPC
	mu              sync.RWMutex
	nodes           map[string]*node
	placement       uint64
	maintenance     sync.Mutex
	lastRepairError string
}

func New(store *metadata.Store, config Config) (*Master, error) {
	if config.Replication == 0 {
		config.Replication = 3
	}
	if config.ChunkSize == 0 {
		config.ChunkSize = p.MaxChunkSize
	}
	if config.NodeTimeout == 0 {
		config.NodeTimeout = 10 * time.Second
	}
	if config.UploadTTL == 0 {
		config.UploadTTL = 15 * time.Minute
	}
	if config.GCGrace == 0 {
		config.GCGrace = time.Hour
	}
	if len(config.Token) < 24 || config.Replication < 1 || config.Replication > 10 || config.ChunkSize < 1 || config.ChunkSize > p.MaxChunkSize || config.NodeTimeout <= 0 || config.UploadTTL <= 0 || config.GCGrace < config.UploadTTL+p.TransferSeconds*time.Second {
		return nil, p.Error(400, "invalid master configuration (GC grace must exceed upload TTL plus transfer timeout)")
	}
	return &Master{Store: store, config: config, rpc: p.NewRPC(config.Token), nodes: map[string]*node{}}, nil
}
func (m *Master) Heartbeat(in p.Inventory) error {
	if err := in.Replica.Validate(); err != nil {
		return err
	}
	if len(in.Chunks) > 50000 || len(in.Present) > 50000 {
		return p.Error(413, "inventory exceeds lab limit")
	}
	chunks := map[string]p.Spec{}
	present := map[string]bool{}
	for _, id := range in.Present {
		if !p.ValidID(id) || present[id] {
			return p.Error(400, "invalid present chunk list")
		}
		present[id] = true
	}
	for _, c := range in.Chunks {
		if err := c.Validate(); err != nil {
			return err
		}
		if _, exists := chunks[c.ID]; exists {
			return p.Error(400, "duplicate chunk in inventory")
		}
		chunks[c.ID] = c
		present[c.ID] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range m.nodes {
		if n.ID != in.ID && n.URL == in.URL && time.Since(n.seenAt) < m.config.NodeTimeout {
			return p.Error(409, "node URL already registered")
		}
	}
	m.nodes[in.ID] = &node{Replica: in.Replica, seenAt: time.Now(), chunks: chunks, present: present}
	return nil
}
func (m *Master) alive() []p.Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := []p.Replica{}
	for _, n := range m.nodes {
		if time.Since(n.seenAt) < m.config.NodeTimeout {
			result = append(result, n.Replica)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
func (m *Master) Locations(spec p.Spec) []p.Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := []p.Replica{}
	for _, n := range m.nodes {
		if time.Since(n.seenAt) < m.config.NodeTimeout && p.Same(n.chunks[spec.ID], spec) {
			result = append(result, n.Replica)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
func (m *Master) remember(replica p.Replica, spec p.Spec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.nodes[replica.ID]
	if n != nil && n.Replica == replica {
		n.chunks[spec.ID] = spec
		n.present[spec.ID] = true
		n.seenAt = time.Now()
	}
}
func (m *Master) Manifest(path string) (p.Manifest, error) {
	if err := p.ValidatePath(path); err != nil {
		return p.Manifest{}, err
	}
	file, err := m.Store.File(path)
	if err != nil {
		return file, err
	}
	for i := range file.Chunks {
		file.Chunks[i].Replicas = m.Locations(file.Chunks[i].Spec)
	}
	return file, nil
}
func (m *Master) Plan(u p.Upload) p.Plan {
	plan := p.Plan{Upload: u, Leases: []string{}}
	for _, c := range u.Chunks {
		plan.Leases = append(plan.Leases, p.Sign(p.Lease{Chunk: c.Spec, Replicas: c.Replicas, Primary: c.Replicas[0].ID, ExpiresAt: u.ExpiresAt}, m.config.Token))
	}
	return plan
}
func (m *Master) Begin(in p.Begin) (p.Plan, error) {
	if err := p.ValidatePath(in.Path); err != nil {
		return p.Plan{}, err
	}
	if len(in.Chunks) > p.MaxChunks || in.Size < 0 || in.Size > p.MaxFileSize {
		return p.Plan{}, p.Error(400, "file exceeds lab limits")
	}
	var sum int64
	for _, spec := range in.Chunks {
		if spec.Size <= 0 || spec.Size > m.config.ChunkSize || !p.ValidHash(spec.SHA256) {
			return p.Plan{}, p.Error(400, "invalid upload chunk")
		}
		sum += spec.Size
	}
	if sum != in.Size {
		return p.Plan{}, p.Error(400, "file size does not match chunks")
	}
	state, err := m.Store.Snapshot()
	if err != nil {
		return p.Plan{}, err
	}
	base := state.PathVersions[in.Path]
	if in.Base != nil && *in.Base != state.Files[in.Path].Generation {
		return p.Plan{}, p.Error(409, "file changed")
	}
	nodes := m.alive()
	if len(in.Chunks) > 0 && len(nodes) < m.config.Replication {
		return p.Plan{}, p.Error(503, "not enough live chunkservers")
	}
	chunks := make([]p.Chunk, 0, len(in.Chunks))
	m.mu.Lock()
	for _, spec := range in.Chunks {
		spec.ID = p.NewID()
		replicas := make([]p.Replica, m.config.Replication)
		start := int(m.placement % uint64(len(nodes)))
		m.placement++
		for i := range replicas {
			replicas[i] = nodes[(start+i)%len(nodes)]
		}
		chunks = append(chunks, p.Chunk{Spec: spec, Replicas: replicas})
	}
	m.mu.Unlock()
	u := p.Upload{ID: p.NewID(), Path: in.Path, Generation: p.NewID(), Base: base, Chunks: chunks, ExpiresAt: time.Now().Add(m.config.UploadTTL).UnixMilli(), Status: "pending"}
	if err = m.Store.Create(u); err != nil {
		return p.Plan{}, err
	}
	return m.Plan(u), nil
}
func (m *Master) Commit(ctx context.Context, id string) (p.Manifest, error) {
	u, err := m.Store.Upload(id)
	if err != nil {
		return p.Manifest{}, err
	}
	if u.Status == "committed" {
		return m.Store.Commit(id, time.Now().UnixMilli())
	}
	if u.Status != "pending" || u.ExpiresAt <= time.Now().UnixMilli() {
		return p.Manifest{}, p.Error(409, "upload is no longer active")
	}
	for _, c := range u.Chunks {
		for _, r := range c.Replicas {
			var actual p.Spec
			if err = m.rpc.JSON(ctx, http.MethodGet, r.URL+"/chunks/"+c.ID+"/meta", nil, &actual); err != nil {
				return p.Manifest{}, err
			}
			if !p.Same(actual, c.Spec) {
				return p.Manifest{}, p.Error(502, "replica verification failed")
			}
			m.remember(r, actual)
		}
	}
	return m.Store.Commit(id, time.Now().UnixMilli())
}
func (m *Master) Status() (p.Status, error) {
	files, err := m.Store.Files()
	if err != nil {
		return p.Status{}, err
	}
	live := m.alive()
	status := p.Status{Replication: m.config.Replication, ChunkSize: m.config.ChunkSize, Files: len(files), LiveNodes: len(live), Writable: len(live) >= m.config.Replication, Nodes: []p.NodeStatus{}}
	for _, f := range files {
		for _, c := range f.Chunks {
			n := len(m.Locations(c.Spec))
			if n < m.config.Replication {
				status.UnderReplicated++
			}
			if n == 0 {
				status.Unavailable++
			}
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	status.LastRepairError = m.lastRepairError
	for _, n := range m.nodes {
		status.Nodes = append(status.Nodes, p.NodeStatus{Replica: n.Replica, Alive: time.Since(n.seenAt) < m.config.NodeTimeout, SeenAt: n.seenAt.UnixMilli(), Chunks: len(n.chunks)})
	}
	sort.Slice(status.Nodes, func(i, j int) bool { return status.Nodes[i].ID < status.Nodes[j].ID })
	return status, nil
}
