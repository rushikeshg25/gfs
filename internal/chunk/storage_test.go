package chunk

import (
	"crypto/sha256"
	"fmt"
	p "gfs/internal/protocol"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func specFor(data string) p.Spec {
	return p.Spec{ID: p.NewID(), Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(data)))}
}
func TestStorageDurabilityAndHealing(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	spec := specFor("hello")
	for i := 0; i < 2; i++ {
		if err = s.Put(spec, strings.NewReader("hello")); err != nil {
			t.Fatal(err)
		}
	}
	other := specFor("world")
	other.ID = spec.ID
	if err = s.Put(other, strings.NewReader("world")); err == nil {
		t.Fatal("immutable chunk overwritten")
	}
	if err = os.WriteFile(filepath.Join(root, spec.ID, "data"), []byte("broke"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Inspect(spec.ID); err == nil {
		t.Fatal("corruption undetected")
	}
	if err = s.Put(spec, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	os.Mkdir(filepath.Join(root, ".tmp-interrupted"), 0700)
	s.Close()
	s, err = OpenStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	chunks, _, err := s.Inventory()
	if err != nil || len(chunks) != 1 || chunks[0] != spec {
		t.Fatalf("inventory: %+v %v", chunks, err)
	}
	if _, err = os.Stat(filepath.Join(root, ".tmp-interrupted")); !os.IsNotExist(err) {
		t.Fatal("staging directory survived restart")
	}
	if err = s.Remove(spec.ID); err != nil {
		t.Fatal(err)
	}
}
func TestFailedWritesNeverPublish(t *testing.T) {
	s, err := OpenStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	spec := specFor("hello")
	for _, data := range []string{"hel", "hello!", "world"} {
		if err = s.Put(spec, strings.NewReader(data)); err == nil {
			t.Fatal("invalid data accepted")
		}
	}
	chunks, _, err := s.Inventory()
	if err != nil || len(chunks) != 0 {
		t.Fatal("partial write visible")
	}
	if _, err = s.Inspect("../escape"); err == nil {
		t.Fatal("path traversal accepted")
	}
}
