package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	p "gfs/internal/protocol"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadFailoverAndAtomicDestination(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("hello")) }))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("broke")) }))
	defer bad.Close()
	spec := p.Spec{ID: p.NewID(), Size: 5, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("hello")))}
	manifest := p.Manifest{Path: "/hello", Generation: p.NewID(), Size: 5, Chunks: []p.Chunk{{Spec: spec, Replicas: []p.Replica{{ID: "bad", URL: bad.URL}, {ID: "good", URL: good.URL}}}}}
	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { p.JSON(w, 200, manifest) }))
	defer master.Close()
	c := New(master.URL, "test-token-at-least-24-characters")
	dest := filepath.Join(t.TempDir(), "out")
	if _, err := c.Get(context.Background(), "/hello", dest); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(dest)
	if string(data) != "hello" {
		t.Fatal("failover returned incorrect bytes")
	}
	good.Close()
	if _, err := c.Get(context.Background(), "/hello", dest); err == nil {
		t.Fatal("corrupt download succeeded")
	}
	data, _ = os.ReadFile(dest)
	if string(data) != "hello" {
		t.Fatal("failed download changed destination")
	}
	entries, _ := os.ReadDir(filepath.Dir(dest))
	if len(entries) != 1 {
		t.Fatal("temporary output leaked")
	}
}
