package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"gfs/internal/durable"
	p "gfs/internal/protocol"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Storage struct {
	root   string
	lock   *os.File
	mu     sync.Mutex
	busy   map[string]bool
	writes chan struct{}
	once   sync.Once
}

func OpenStorage(root string) (*Storage, error) {
	lock, err := durable.Lock(root)
	if err != nil {
		return nil, err
	}
	s := &Storage{root: root, lock: lock, busy: map[string]bool{}, writes: make(chan struct{}, 4)}
	entries, err := os.ReadDir(root)
	if err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".tmp-") || strings.HasPrefix(e.Name(), ".bad-") {
				if err = os.RemoveAll(filepath.Join(root, e.Name())); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		durable.Unlock(lock)
		return nil, err
	}
	return s, nil
}
func (s *Storage) dir(id string) (string, error) {
	if !p.ValidID(id) {
		return "", p.Error(400, "invalid chunk id")
	}
	return filepath.Join(s.root, id), nil
}
func (s *Storage) acquire(id string) error {
	if !p.ValidID(id) {
		return p.Error(400, "invalid chunk id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy[id] {
		return p.Error(409, "chunk mutation in progress")
	}
	select {
	case s.writes <- struct{}{}:
		s.busy[id] = true
		return nil
	default:
		return p.Error(429, "chunkserver write limit reached")
	}
}
func (s *Storage) release(id string) { s.mu.Lock(); delete(s.busy, id); s.mu.Unlock(); <-s.writes }
func (s *Storage) OpenVerified(id string) (*os.File, p.Spec, error) {
	var spec p.Spec
	dir, err := s.dir(id)
	if err != nil {
		return nil, spec, err
	}
	meta, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if os.IsNotExist(err) {
		return nil, spec, p.Error(404, "chunk not found")
	}
	if err != nil {
		return nil, spec, err
	}
	if json.Unmarshal(meta, &spec) != nil || spec.Validate() != nil || spec.ID != id {
		return nil, spec, p.Error(422, "chunk metadata corrupt")
	}
	f, err := os.Open(filepath.Join(dir, "data"))
	if os.IsNotExist(err) {
		return nil, spec, p.Error(422, "chunk data missing")
	}
	if err != nil {
		return nil, spec, err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(f, spec.Size+1))
	if err != nil {
		f.Close()
		return nil, spec, err
	}
	if n != spec.Size || hex.EncodeToString(hash.Sum(nil)) != spec.SHA256 {
		f.Close()
		return nil, spec, p.Error(422, "chunk checksum mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, spec, err
	}
	return f, spec, nil
}
func (s *Storage) Inspect(id string) (p.Spec, error) {
	f, spec, err := s.OpenVerified(id)
	if f != nil {
		f.Close()
	}
	return spec, err
}
func (s *Storage) Inventory() ([]p.Spec, []string, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, nil, err
	}
	specs := []p.Spec{}
	present := []string{}
	for _, e := range entries {
		if !p.ValidID(e.Name()) {
			continue
		}
		present = append(present, e.Name())
		spec, err := s.Inspect(e.Name())
		if err != nil {
			var fault *p.Fault
			if !errors.As(err, &fault) || (fault.Status != 404 && fault.Status != 422) {
				return nil, nil, err
			}
			log.Printf("inventory excluded %s: %v", e.Name(), err)
			continue
		}
		specs = append(specs, spec)
	}
	return specs, present, nil
}
func (s *Storage) Put(spec p.Spec, body io.Reader) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if err := s.acquire(spec.ID); err != nil {
		return err
	}
	defer s.release(spec.ID)
	temp, err := os.MkdirTemp(s.root, ".tmp-"+spec.ID+"-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	f, err := os.OpenFile(filepath.Join(temp, "data"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, hash), io.LimitReader(body, spec.Size+1))
	if copyErr != nil {
		f.Close()
		return copyErr
	}
	if n != spec.Size || hex.EncodeToString(hash.Sum(nil)) != spec.SHA256 {
		f.Close()
		return p.Error(422, "chunk size or checksum mismatch")
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	meta, _ := json.Marshal(spec)
	if err = durable.Replace(filepath.Join(temp, "meta.json"), meta); err != nil {
		return err
	}
	if err = durable.SyncDir(temp); err != nil {
		return err
	}
	existing, err := s.Inspect(spec.ID)
	if err == nil {
		if !p.Same(existing, spec) {
			return p.Error(409, "immutable chunk conflict")
		}
		return nil
	}
	var fault *p.Fault
	if !errors.As(err, &fault) || (fault.Status != 404 && fault.Status != 422) {
		return err
	}
	dest, _ := s.dir(spec.ID)
	bad := ""
	if _, err = os.Stat(dest); err == nil {
		bad = filepath.Join(s.root, ".bad-"+spec.ID+"-"+p.NewID())
		if err = os.Rename(dest, bad); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err = os.Rename(temp, dest); err != nil {
		return err
	}
	if err = durable.SyncDir(s.root); err != nil {
		return err
	}
	if bad != "" {
		if err = os.RemoveAll(bad); err != nil {
			return fmt.Errorf("remove corrupt predecessor: %w", err)
		}
	}
	return nil
}
func (s *Storage) Remove(id string) error {
	dir, err := s.dir(id)
	if err != nil {
		return err
	}
	if err = s.acquire(id); err != nil {
		return err
	}
	defer s.release(id)
	if err = os.RemoveAll(dir); err != nil {
		return err
	}
	return durable.SyncDir(s.root)
}
func (s *Storage) Close() error {
	var err error
	s.once.Do(func() { err = durable.Unlock(s.lock) })
	return err
}
