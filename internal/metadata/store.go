// Package metadata stores a durable namespace checkpoint. It is designed for a small lab.
package metadata

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gfs/internal/durable"
	p "gfs/internal/protocol"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type checkpoint struct {
	State  json.RawMessage `json:"state"`
	SHA256 string          `json:"sha256"`
}

type Record struct {
	p.Spec
	RetiredAt int64 `json:"retiredAt"`
}
type State struct {
	PathVersions map[string]string     `json:"pathVersions"`
	Version      int                   `json:"version"`
	Files        map[string]p.Manifest `json:"files"`
	Uploads      map[string]p.Upload   `json:"uploads"`
	Chunks       map[string]Record     `json:"chunks"`
}
type Store struct {
	mu       sync.Mutex
	root     string
	lock     *os.File
	state    State
	poisoned error
	closed   bool
}

func Open(root string) (*Store, error) {
	lock, err := durable.Lock(root)
	if err != nil {
		return nil, err
	}
	s := &Store{root: root, lock: lock, state: State{Version: 2, PathVersions: map[string]string{}, Files: map[string]p.Manifest{}, Uploads: map[string]p.Upload{}, Chunks: map[string]Record{}}}
	data, err := os.ReadFile(filepath.Join(root, "metadata.json"))
	if err == nil {
		var saved checkpoint
		err = json.Unmarshal(data, &saved)
		if err == nil {
			hash := sha256.Sum256(saved.State)
			if hex.EncodeToString(hash[:]) != saved.SHA256 {
				err = fmt.Errorf("metadata checkpoint checksum mismatch")
			} else {
				err = json.Unmarshal(saved.State, &s.state)
			}
		}
	} else if os.IsNotExist(err) {
		err = nil
	}
	if err == nil && ((s.state.Version != 1 && s.state.Version != 2) || s.state.Files == nil || s.state.Uploads == nil || s.state.Chunks == nil) {
		err = fmt.Errorf("invalid metadata checkpoint")
	}
	if err == nil && s.state.Version == 1 {
		// Upgrade the initial lab checkpoint without deleting existing files or uploads.
		s.state.PathVersions = map[string]string{}
		for path, file := range s.state.Files {
			s.state.PathVersions[path] = file.Generation
		}
		s.state.Version = 2
		encoded, encodeErr := encodeCheckpoint(s.state)
		if encodeErr != nil {
			err = encodeErr
		} else {
			err = durable.Replace(filepath.Join(root, "metadata.json"), encoded)
		}
	}
	if err == nil && s.state.PathVersions == nil {
		err = fmt.Errorf("missing namespace versions")
	}
	if err != nil {
		durable.Unlock(lock)
		return nil, fmt.Errorf("open metadata: %w", err)
	}
	return s, nil
}
func encodeCheckpoint(state State) ([]byte, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return json.Marshal(checkpoint{State: data, SHA256: hex.EncodeToString(hash[:])})
}
func clone(state State) (State, error) {
	var result State
	data, err := json.Marshal(state)
	if err == nil {
		err = json.Unmarshal(data, &result)
	}
	return result, err
}
func (s *Store) available() error {
	if s.closed {
		return p.Error(503, "metadata store closed")
	}
	if s.poisoned != nil {
		return p.Error(503, "metadata persistence failed; restart required")
	}
	return nil
}
func (s *Store) Snapshot() (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.available(); err != nil {
		return State{}, err
	}
	return clone(s.state)
}
func (s *Store) update(change func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.available(); err != nil {
		return err
	}
	next, err := clone(s.state)
	if err != nil {
		return err
	}
	if err = change(&next); err != nil {
		return err
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	before, _ := json.Marshal(s.state)
	if bytes.Equal(before, data) {
		return nil
	}
	// On any persistence error stop serving metadata: rename/fsync failures can have ambiguous durability.
	encoded, err := encodeCheckpoint(next)
	if err != nil {
		return err
	}
	if err = durable.Replace(filepath.Join(s.root, "metadata.json"), encoded); err != nil {
		s.poisoned = err
		return fmt.Errorf("persist metadata: %w", err)
	}
	s.state = next
	return nil
}
func (s *Store) Create(u p.Upload) error {
	return s.update(func(state *State) error {
		if _, ok := state.Uploads[u.ID]; ok {
			return p.Error(409, "upload already exists")
		}
		for _, c := range u.Chunks {
			if _, ok := state.Chunks[c.ID]; ok {
				return p.Error(409, "chunk already exists")
			}
		}
		state.Uploads[u.ID] = u
		for _, c := range u.Chunks {
			state.Chunks[c.ID] = Record{Spec: c.Spec}
		}
		return nil
	})
}
func (s *Store) Upload(id string) (p.Upload, error) {
	state, err := s.Snapshot()
	if err != nil {
		return p.Upload{}, err
	}
	u, ok := state.Uploads[id]
	if !ok {
		return u, p.Error(404, "upload not found")
	}
	return u, nil
}
func (s *Store) File(path string) (p.Manifest, error) {
	state, err := s.Snapshot()
	if err != nil {
		return p.Manifest{}, err
	}
	f, ok := state.Files[path]
	if !ok {
		return f, p.Error(404, "file not found")
	}
	return f, nil
}
func (s *Store) Files() ([]p.Manifest, error) {
	state, err := s.Snapshot()
	if err != nil {
		return nil, err
	}
	files := make([]p.Manifest, 0, len(state.Files))
	for _, f := range state.Files {
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}
func manifest(u p.Upload) p.Manifest {
	var size int64
	for _, c := range u.Chunks {
		size += c.Size
	}
	return p.Manifest{Path: u.Path, Generation: u.Generation, Size: size, Chunks: u.Chunks, CreatedAt: u.CommittedAt}
}
func (s *Store) Commit(id string, now int64) (p.Manifest, error) {
	var result p.Manifest
	err := s.update(func(state *State) error {
		u, ok := state.Uploads[id]
		if !ok {
			return p.Error(404, "upload not found")
		}
		if u.Status == "committed" {
			result = manifest(u)
			return nil
		}
		if u.Status != "pending" || u.ExpiresAt <= now {
			return p.Error(409, "upload is no longer active")
		}
		if state.PathVersions[u.Path] != u.Base {
			return p.Error(409, "file changed during upload")
		}
		u.Status = "committed"
		u.CommittedAt = now
		state.Uploads[id] = u
		result = manifest(u)
		state.Files[u.Path] = result
		state.PathVersions[u.Path] = u.Generation
		return nil
	})
	return result, err
}
func (s *Store) Abort(id string) error {
	return s.update(func(state *State) error {
		u, ok := state.Uploads[id]
		if !ok {
			return p.Error(404, "upload not found")
		}
		if u.Status == "committed" {
			return p.Error(409, "upload already committed")
		}
		u.Status = "aborted"
		state.Uploads[id] = u
		return nil
	})
}
func (s *Store) Delete(path, generation string) error {
	return s.update(func(state *State) error {
		file, ok := state.Files[path]
		if !ok {
			return p.Error(404, "file not found")
		}
		if file.Generation != generation {
			return p.Error(409, "file changed")
		}
		delete(state.Files, path)
		state.PathVersions[path] = p.NewID()
		return nil
	})
}
func (s *Store) Garbage(now, grace int64) ([]p.Spec, error) {
	garbage := []p.Spec{}
	err := s.update(func(state *State) error {
		live := map[string]bool{}
		for _, f := range state.Files {
			for _, c := range f.Chunks {
				live[c.ID] = true
			}
		}
		for id, u := range state.Uploads {
			if u.Status == "pending" && u.ExpiresAt <= now {
				u.Status = "aborted"
				state.Uploads[id] = u
			}
			if u.Status == "pending" {
				for _, c := range u.Chunks {
					live[c.ID] = true
				}
			}
		}
		for id, c := range state.Chunks {
			if live[id] {
				c.RetiredAt = 0
			} else if c.RetiredAt == 0 {
				c.RetiredAt = now
			}
			state.Chunks[id] = c
			if c.RetiredAt != 0 && c.RetiredAt <= now-grace {
				garbage = append(garbage, c.Spec)
			}
		}
		return nil
	})
	return garbage, err
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return durable.Unlock(s.lock)
}
