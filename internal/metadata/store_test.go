package metadata

import (
	p "gfs/internal/protocol"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeUpload(base string) p.Upload {
	return p.Upload{ID: p.NewID(), Path: "/hello", Generation: p.NewID(), Base: base, Status: "pending", ExpiresAt: 10000, Chunks: []p.Chunk{{Spec: p.Spec{ID: p.NewID(), Size: 3, SHA256: strings.Repeat("a", 64)}}}}
}
func TestPublicationRestartAndCollection(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	if _, err := Open(root); err == nil {
		t.Fatal("second master acquired same directory")
	}
	a, b := makeUpload(""), makeUpload("")
	if err = s.Create(a); err != nil {
		t.Fatal(err)
	}
	if err = s.Create(b); err != nil {
		t.Fatal(err)
	}
	if _, err = s.File("/hello"); err == nil {
		t.Fatal("uncommitted file visible")
	}
	first, err := s.Commit(a.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Commit(a.ID, 2)
	if err != nil || first.CreatedAt != again.CreatedAt {
		t.Fatal("commit is not idempotent")
	}
	if _, err = s.Commit(b.ID, 2); err == nil {
		t.Fatal("concurrent writer did not conflict")
	}
	replacement := makeUpload(a.Generation)
	s.Create(replacement)
	s.Abort(replacement.ID)
	if _, err = s.Commit(replacement.ID, 3); err == nil {
		t.Fatal("aborted commit succeeded")
	}
	s.Close()
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.File("/hello")
	if err != nil || f.Generation != a.Generation {
		t.Fatal("restart lost namespace")
	}
	if err = s.Delete("/hello", "stale"); err == nil {
		t.Fatal("stale delete succeeded")
	}
	if err = s.Delete("/hello", a.Generation); err != nil {
		t.Fatal(err)
	}
	garbage, err := s.Garbage(20000, 1000)
	if err != nil || len(garbage) != 0 {
		t.Fatalf("premature collection: %v", err)
	}
	garbage, err = s.Garbage(21000, 1000)
	if err != nil || len(garbage) != 3 {
		t.Fatalf("missing garbage: %v %d", err, len(garbage))
	}
}
func TestCorruptCheckpointFailsClosed(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "metadata.json"), []byte("broken"), 0600)
	if _, err := Open(root); err == nil {
		t.Fatal("accepted corrupted metadata")
	}
}
