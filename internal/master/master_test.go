package master

import (
	"context"
	"gfs/internal/metadata"
	p "gfs/internal/protocol"
	"strings"
	"testing"
	"time"
)

func TestPlacementExpiryAndEmptyFiles(t *testing.T) {
	store, err := metadata.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	m, err := New(store, Config{Token: "test-token-at-least-24-characters", NodeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	input := p.Begin{Path: "/hello", Size: 5, Chunks: []p.Spec{{Size: 5, SHA256: strings.Repeat("a", 64)}}}
	if _, err = m.Begin(input); err == nil {
		t.Fatal("allocated with no live nodes")
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if err = m.Heartbeat(p.Inventory{Replica: p.Replica{ID: id, URL: "http://" + id + ":9000"}}); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := m.Begin(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Chunks[0].Replicas) != 3 {
		t.Fatal("wrong replication factor")
	}
	if _, err = p.Verify(plan.Leases[0], m.config.Token, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, n := range m.nodes {
		n.seenAt = time.Time{}
	}
	if _, err = m.Begin(input); err == nil {
		t.Fatal("expired nodes allocated")
	}
	empty, err := m.Begin(p.Begin{Path: "/empty", Chunks: []p.Spec{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Commit(context.Background(), empty.ID); err != nil {
		t.Fatal(err)
	}
	file, err := m.Manifest("/empty")
	if err != nil || file.Size != 0 {
		t.Fatal("empty file failed")
	}
}
