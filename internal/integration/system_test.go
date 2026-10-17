package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"gfs/internal/chunk"
	"gfs/internal/client"
	"gfs/internal/master"
	"gfs/internal/metadata"
	p "gfs/internal/protocol"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const token = "integration-token-at-least-24-characters"

type lab struct {
	store   *metadata.Store
	master  *master.Master
	server  *httptest.Server
	nodes   []*chunk.Node
	servers []*httptest.Server
	client  *client.Client
	root    string
}

func newLab(t *testing.T) *lab {
	t.Helper()
	l := &lab{root: t.TempDir()}
	var err error
	l.store, err = metadata.Open(filepath.Join(l.root, "master"))
	if err != nil {
		t.Fatal(err)
	}
	l.master, err = master.New(l.store, master.Config{Token: token, ChunkSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	l.server = httptest.NewServer(l.master.Handler())
	l.client = client.New(l.server.URL, token)
	for i := 0; i < 4; i++ {
		s, err := chunk.OpenStorage(filepath.Join(l.root, fmt.Sprintf("node-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n, err := chunk.NewNode(s, p.Replica{ID: fmt.Sprintf("node-%d", i), URL: "http://" + listener.Addr().String()}, token)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(n.Handler())
		server.Listener.Close()
		server.Listener = listener
		server.Start()
		l.nodes = append(l.nodes, n)
		l.servers = append(l.servers, server)
	}
	t.Cleanup(func() {
		for _, server := range l.servers {
			server.Close()
		}
		l.server.Close()
		for _, n := range l.nodes {
			n.Storage.Close()
		}
		l.store.Close()
	})
	l.report(t)
	return l
}
func (l *lab) report(t *testing.T) {
	t.Helper()
	for _, n := range l.nodes {
		if err := n.Heartbeat(context.Background(), l.server.URL); err != nil {
			t.Fatal(err)
		}
	}
}
func TestWholeFileFlowAndMasterRestart(t *testing.T) {
	l := newLab(t)
	ctx := context.Background()
	data := append([]byte{0, 255, 127}, []byte("hello 🌏, immutable multichunk bytes")...)
	input := filepath.Join(l.root, "input")
	output := filepath.Join(l.root, "output")
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	first, err := l.client.Put(ctx, input, "/日本語/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Chunks) < 2 {
		t.Fatal("expected multiple chunks")
	}
	if _, err = l.client.Get(ctx, first.Path, output); err != nil {
		t.Fatal(err)
	}
	actual, _ := os.ReadFile(output)
	if !bytes.Equal(data, actual) {
		t.Fatal("download differs")
	}
	files, err := l.client.List(ctx, "/日本語/")
	if err != nil || len(files) != 1 {
		t.Fatal("list failed")
	}
	// Restart only the master and rebuild live locations from chunk reports.
	l.server.Close()
	l.store.Close()
	l.store, err = metadata.Open(filepath.Join(l.root, "master"))
	if err != nil {
		t.Fatal(err)
	}
	l.master, err = master.New(l.store, master.Config{Token: token, ChunkSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	l.server = httptest.NewServer(l.master.Handler())
	l.client = client.New(l.server.URL, token)
	l.report(t)
	if _, err = l.client.Get(ctx, first.Path, output); err != nil {
		t.Fatal(err)
	}
	actual, _ = os.ReadFile(output)
	if !bytes.Equal(data, actual) {
		t.Fatal("restart lost bytes")
	}
	os.WriteFile(input, []byte("replacement"), 0600)
	second, err := l.client.Put(ctx, input, first.Path)
	if err != nil || second.Generation == first.Generation {
		t.Fatalf("overwrite failed: %v", err)
	}
	if _, err = l.client.Get(ctx, first.Path, output); err != nil {
		t.Fatal(err)
	}
	actual, _ = os.ReadFile(output)
	if string(actual) != "replacement" {
		t.Fatal("old generation returned")
	}
	os.WriteFile(input, nil, 0600)
	empty, err := l.client.Put(ctx, input, "/empty")
	if err != nil || len(empty.Chunks) != 0 {
		t.Fatalf("empty upload: %v", err)
	}
	if _, err = l.client.Get(ctx, "/empty", output); err != nil {
		t.Fatal(err)
	}
	actual, _ = os.ReadFile(output)
	if len(actual) != 0 {
		t.Fatal("empty download not empty")
	}
	if err = l.client.Delete(ctx, first.Path); err != nil {
		t.Fatal(err)
	}
	if _, err = l.client.Stat(ctx, first.Path); err == nil {
		t.Fatal("deleted file visible")
	}
}
func TestIncompleteUploadAndConcurrentPublication(t *testing.T) {
	l := newLab(t)
	ctx := context.Background()
	rpc := p.NewRPC(token)
	begin := func(data string) p.Plan {
		t.Helper()
		var plan p.Plan
		err := rpc.JSON(ctx, "POST", l.server.URL+"/uploads", p.Begin{Path: "/race", Size: int64(len(data)), Chunks: []p.Spec{{Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(data)))}}}, &plan)
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	a, b := begin("first"), begin("other")
	if err := rpc.JSON(ctx, "POST", l.server.URL+"/uploads/"+a.ID+"/commit", nil, new(p.Manifest)); err == nil {
		t.Fatal("incomplete upload committed")
	}
	if _, err := l.client.Stat(ctx, "/race"); err == nil {
		t.Fatal("partial file visible")
	}
	for i, plan := range []p.Plan{a, b} {
		data := []string{"first", "other"}[i]
		c := plan.Chunks[0]
		response, err := rpc.Do(ctx, "PUT", c.Replicas[0].URL+"/chunks/"+c.ID, bytes.NewBufferString(data), map[string]string{"X-GFS-Lease": plan.Leases[0]})
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, plan := range []p.Plan{a, b} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			results <- rpc.JSON(ctx, "POST", l.server.URL+"/uploads/"+id+"/commit", nil, new(p.Manifest))
		}(plan.ID)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("expected one compare-and-swap winner, got %d", success)
	}
	out := filepath.Join(l.root, "out")
	if _, err := l.client.Get(ctx, "/race", out); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(out)
	if string(data) != "first" && string(data) != "other" {
		t.Fatal("mixed generation published")
	}
}
func TestExpiredUploadCannotPublish(t *testing.T) {
	store, err := metadata.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	m, err := master.New(store, master.Config{Token: token, UploadTTL: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := m.Begin(p.Begin{Path: "/expired", Chunks: []p.Spec{}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err = m.Commit(context.Background(), plan.ID); err == nil {
		t.Fatal("expired upload committed")
	}
}
