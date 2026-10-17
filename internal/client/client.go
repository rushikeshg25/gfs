// Package client streams file data directly between the caller and chunkservers.
package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"gfs/internal/durable"
	p "gfs/internal/protocol"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Client struct {
	MasterURL string
	RPC       *p.RPC
}

func New(masterURL, token string) *Client {
	return &Client{MasterURL: strings.TrimRight(masterURL, "/"), RPC: p.NewRPC(token)}
}
func (c *Client) Status(ctx context.Context) (p.Status, error) {
	var out p.Status
	err := c.RPC.JSON(ctx, "GET", c.MasterURL+"/status", nil, &out)
	return out, err
}
func (c *Client) List(ctx context.Context, prefix string) ([]p.Manifest, error) {
	var out []p.Manifest
	err := c.RPC.JSON(ctx, "GET", c.MasterURL+"/files?prefix="+url.QueryEscape(prefix), nil, &out)
	return out, err
}
func (c *Client) Stat(ctx context.Context, path string) (p.Manifest, error) {
	var out p.Manifest
	if err := p.ValidatePath(path); err != nil {
		return out, err
	}
	err := c.RPC.JSON(ctx, "GET", c.MasterURL+"/file?path="+url.QueryEscape(path), nil, &out)
	return out, err
}
func (c *Client) Delete(ctx context.Context, path string) error {
	f, err := c.Stat(ctx, path)
	if err != nil {
		return err
	}
	res, err := c.RPC.Do(ctx, "DELETE", c.MasterURL+"/file?path="+url.QueryEscape(path), nil, map[string]string{"If-Match": f.Generation})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(io.Discard, res.Body)
	return err
}
func retry(ctx context.Context, run func() error) error {
	for attempt := 0; ; attempt++ {
		err := run()
		if err == nil {
			return nil
		}
		var fault *p.Fault
		if attempt >= 2 || !errors.As(err, &fault) || (fault.Status != 503 && fault.Status != 429 && !(fault.Status == 409 && strings.Contains(fault.Message, "in progress"))) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(100*(1<<attempt)) * time.Millisecond):
		}
	}
}
func (c *Client) Put(ctx context.Context, source, path string) (p.Manifest, error) {
	var result p.Manifest
	if err := p.ValidatePath(path); err != nil {
		return result, err
	}
	file, err := os.Open(source)
	if err != nil {
		return result, err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return result, err
	}
	if !before.Mode().IsRegular() || before.Size() > p.MaxFileSize {
		return result, p.Error(400, "source must be a regular file no larger than 1 GiB")
	}
	status, err := c.Status(ctx)
	if err != nil {
		return result, err
	}
	size := status.ChunkSize
	if size <= 0 || size > p.MaxChunkSize {
		return result, p.Error(502, "invalid server chunk size")
	}
	if (before.Size()+size-1)/size > p.MaxChunks {
		return result, p.Error(400, "file exceeds chunk count limit")
	}
	input := p.Begin{Path: path, Size: before.Size(), Chunks: []p.Spec{}}
	for offset := int64(0); offset < before.Size(); offset += size {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		length := min(size, before.Size()-offset)
		hash := sha256.New()
		n, err := io.Copy(hash, io.NewSectionReader(file, offset, length))
		if err != nil {
			return result, err
		}
		if n != length {
			return result, p.Error(409, "source changed while hashing")
		}
		input.Chunks = append(input.Chunks, p.Spec{Size: length, SHA256: hex.EncodeToString(hash.Sum(nil))})
	}
	var plan p.Plan
	if err = c.RPC.JSON(ctx, "POST", c.MasterURL+"/uploads", input, &plan); err != nil {
		return result, err
	}
	perform := func() error {
		if len(plan.Chunks) != len(input.Chunks) || len(plan.Leases) != len(plan.Chunks) {
			return p.Error(502, "invalid upload plan")
		}
		var offset int64
		for i, chunk := range plan.Chunks {
			if chunk.Size != input.Chunks[i].Size || chunk.SHA256 != input.Chunks[i].SHA256 || len(chunk.Replicas) == 0 {
				return p.Error(502, "upload plan changed chunk content")
			}
			start := offset
			err = retry(ctx, func() error {
				res, err := c.RPC.Do(ctx, "PUT", chunk.Replicas[0].URL+"/chunks/"+chunk.ID, io.NewSectionReader(file, start, chunk.Size), map[string]string{"X-GFS-Lease": plan.Leases[i]})
				if err != nil {
					return err
				}
				defer res.Body.Close()
				_, err = io.Copy(io.Discard, res.Body)
				return err
			})
			if err != nil {
				return err
			}
			offset += chunk.Size
		}
		after, err := file.Stat()
		if err != nil {
			return err
		}
		if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
			return p.Error(409, "source changed during upload")
		}
		return retry(ctx, func() error { return c.RPC.JSON(ctx, "POST", c.MasterURL+"/uploads/"+plan.ID+"/commit", nil, &result) })
	}
	if err = perform(); err == nil {
		return result, nil
	}
	// A lost commit response is ambiguous. Query before aborting; committed uploads are never undone.
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var current p.Plan
	if resolveErr := c.RPC.JSON(cleanup, "GET", c.MasterURL+"/uploads/"+plan.ID, nil, &current); resolveErr != nil {
		return result, fmt.Errorf("%w; upload %s has unknown status", err, plan.ID)
	}
	if current.Status == "committed" {
		resolveErr := c.RPC.JSON(cleanup, "POST", c.MasterURL+"/uploads/"+plan.ID+"/commit", nil, &result)
		return result, resolveErr
	}
	if cleanupErr := c.RPC.JSON(cleanup, "DELETE", c.MasterURL+"/uploads/"+plan.ID, nil, nil); cleanupErr != nil {
		return result, fmt.Errorf("%w; upload %s cleanup failed: %v", err, plan.ID, cleanupErr)
	}
	return result, err
}
func (c *Client) Get(ctx context.Context, path, destination string) (p.Manifest, error) {
	file, err := c.Stat(ctx, path)
	if err != nil {
		return file, err
	}
	output, err := os.CreateTemp(filepath.Dir(destination), ".gfs-download-")
	if err != nil {
		return file, err
	}
	defer output.Close()
	defer os.Remove(output.Name())
	var offset int64
	for _, chunk := range file.Chunks {
		if err = chunk.Spec.Validate(); err != nil {
			return file, err
		}
		copied := false
		failures := []string{}
		for _, node := range chunk.Replicas {
			if err = ctx.Err(); err != nil {
				return file, err
			}
			copyErr := func() error {
				if err := output.Truncate(offset); err != nil {
					return err
				}
				if _, err := output.Seek(offset, io.SeekStart); err != nil {
					return err
				}
				res, err := c.RPC.Do(ctx, http.MethodGet, node.URL+"/chunks/"+chunk.ID, nil, nil)
				if err != nil {
					return err
				}
				defer res.Body.Close()
				hash := sha256.New()
				n, err := io.Copy(io.MultiWriter(output, hash), io.LimitReader(res.Body, chunk.Size+1))
				if err != nil {
					return err
				}
				if n != chunk.Size || hex.EncodeToString(hash.Sum(nil)) != chunk.SHA256 {
					return p.Error(502, "download checksum mismatch")
				}
				return nil
			}()
			if copyErr == nil {
				offset += chunk.Size
				copied = true
				break
			}
			failures = append(failures, node.ID+": "+copyErr.Error())
		}
		if !copied {
			return file, p.Error(503, "chunk %s unavailable: %s", chunk.ID, strings.Join(failures, "; "))
		}
	}
	if offset != file.Size {
		return file, p.Error(502, "manifest file size mismatch")
	}
	if err = output.Sync(); err != nil {
		return file, err
	}
	if err = output.Close(); err != nil {
		return file, err
	}
	if err = os.Rename(output.Name(), destination); err != nil {
		return file, err
	}
	if err = durable.SyncDir(filepath.Dir(destination)); err != nil {
		return file, err
	}
	return file, nil
}
