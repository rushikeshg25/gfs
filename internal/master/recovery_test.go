package master

import (
	"context"
	"crypto/sha256"
	"fmt"
	"gfs/internal/chunk"
	"gfs/internal/metadata"
	p "gfs/internal/protocol"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const recoveryToken = "test-token-at-least-24-characters"

type testNode struct {
	node   *chunk.Node
	server *httptest.Server
	root   string
}

func spawnNode(t *testing.T, id, root string) *testNode {
	t.Helper()
	store, err := chunk.OpenStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node, err := chunk.NewNode(store, p.Replica{ID: id, URL: "http://" + listener.Addr().String()}, recoveryToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(node.Handler())
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	n := &testNode{node, server, root}
	t.Cleanup(func() { server.Close(); store.Close() })
	return n
}
func TestRepairCorruptionAndReturningNodeGC(t *testing.T) {
	ctx := context.Background()
	store, err := metadata.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	m, err := New(store, Config{Token: recoveryToken})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []*testNode{}
	for _, id := range []string{"a", "b", "c", "d"} {
		nodes = append(nodes, spawnNode(t, id, t.TempDir()))
	}
	report := func(n *testNode) {
		t.Helper()
		chunks, present, err := n.node.Storage.Inventory()
		if err != nil {
			t.Fatal(err)
		}
		if err = m.Heartbeat(p.Inventory{Replica: n.node.Identity, Chunks: chunks, Present: present}); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range nodes {
		report(n)
	}
	plan, err := m.Begin(p.Begin{Path: "/safe", Size: 5, Chunks: []p.Spec{{Size: 5, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("hello")))}}})
	if err != nil {
		t.Fatal(err)
	}
	c := plan.Chunks[0]
	rpc := p.NewRPC(recoveryToken)
	res, err := rpc.Do(ctx, "PUT", c.Replicas[0].URL+"/chunks/"+c.ID, strings.NewReader("hello"), map[string]string{"X-GFS-Lease": plan.Leases[0]})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if _, err = m.Commit(ctx, plan.ID); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		report(n)
	}
	nodes[0].server.Close()
	nodes[0].node.Storage.Close()
	m.nodes["a"].seenAt = time.Time{}
	if err = m.Reconcile(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	locations := m.Locations(c.Spec)
	if len(locations) != 3 || locations[0].ID != "b" {
		t.Fatalf("repair locations: %+v", locations)
	}
	if _, err = nodes[3].node.Storage.Inspect(c.ID); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(nodes[1].root, c.ID, "data"), []byte("broke"), 0600); err != nil {
		t.Fatal(err)
	}
	report(nodes[1])
	if err = m.Reconcile(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = nodes[1].node.Storage.Inspect(c.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.Delete("/safe", plan.Generation); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = m.Reconcile(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err = nodes[2].node.Storage.Inspect(c.ID); err != nil {
		t.Fatal("collected before grace")
	}
	if err = m.Reconcile(ctx, now.Add(m.config.GCGrace+time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = nodes[2].node.Storage.Inspect(c.ID); err == nil {
		t.Fatal("retired chunk retained")
	}
	// Even a corrupted orphan must be collected when its previously offline server returns.
	nodes[0] = spawnNode(t, "a", nodes[0].root)
	os.WriteFile(filepath.Join(nodes[0].root, c.ID, "data"), []byte("broke"), 0600)
	report(nodes[0])
	if err = m.Reconcile(ctx, now.Add(m.config.GCGrace+2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(nodes[0].root, c.ID)); !os.IsNotExist(err) {
		t.Fatal("returning orphan was not collected")
	}
}
