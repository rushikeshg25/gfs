// Package durable provides fsync-backed publication and exclusive Unix process locks.
package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func Replace(path string, data []byte) error {
	root := filepath.Dir(path)
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(root, ".state-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	return SyncDir(root)
}
func Lock(root string) (*os.File, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory already locked: %w", err)
	}
	return f, nil
}
func Unlock(f *os.File) error {
	if f == nil {
		return nil
	}
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
