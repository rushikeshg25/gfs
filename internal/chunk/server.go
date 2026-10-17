package chunk

import (
	"context"
	p "gfs/internal/protocol"
	"io"
	"net/http"
	"strings"
	"time"
)

type Node struct {
	Storage  *Storage
	Identity p.Replica
	token    string
	rpc      *p.RPC
}

func NewNode(storage *Storage, identity p.Replica, token string) (*Node, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if len(token) < 24 {
		return nil, p.Error(400, "token must contain at least 24 characters")
	}
	return &Node{storage, identity, token, p.NewRPC(token)}, nil
}
func (n *Node) Handler() http.Handler { return p.Guard(n.token, n.handle) }
func (n *Node) includes(lease p.Lease) bool {
	for _, r := range lease.Replicas {
		if r == n.Identity {
			return true
		}
	}
	return false
}
func (n *Node) handle(w http.ResponseWriter, r *http.Request) error {
	if strings.HasPrefix(r.URL.Path, "/chunks/") {
		rest := strings.TrimPrefix(r.URL.Path, "/chunks/")
		id := strings.TrimSuffix(rest, "/meta")
		meta := rest != id
		if !p.ValidID(id) {
			return p.Error(400, "invalid chunk id")
		}
		switch r.Method {
		case http.MethodGet:
			f, spec, err := n.Storage.OpenVerified(id)
			if err != nil {
				return err
			}
			defer f.Close()
			if meta {
				p.JSON(w, 200, spec)
				return nil
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-GFS-SHA256", spec.SHA256)
			http.ServeContent(w, r, id, time.Time{}, f)
			return nil
		case http.MethodPut:
			if meta {
				return p.Error(405, "method not allowed")
			}
			signed := r.Header.Get("X-GFS-Lease")
			lease, err := p.Verify(signed, n.token, time.Now())
			if err != nil {
				return err
			}
			if lease.Chunk.ID != id || !n.includes(lease) {
				return p.Error(403, "lease does not include this node")
			}
			secondary := r.Header.Get("X-GFS-Secondary") == "1"
			if !secondary && lease.Primary != n.Identity.ID {
				return p.Error(409, "send upload to the primary")
			}
			if err = n.Storage.Put(lease.Chunk, r.Body); err != nil {
				return err
			}
			if _, err = p.Verify(signed, n.token, time.Now()); err != nil {
				return err
			}
			if !secondary {
				for _, replica := range lease.Replicas {
					if replica.ID == n.Identity.ID {
						continue
					}
					f, _, err := n.Storage.OpenVerified(id)
					if err != nil {
						return err
					}
					response, err := n.rpc.Do(r.Context(), http.MethodPut, replica.URL+"/chunks/"+id, f, map[string]string{"X-GFS-Lease": signed, "X-GFS-Secondary": "1"})
					f.Close()
					if err != nil {
						return err
					}
					_, err = io.Copy(io.Discard, response.Body)
					response.Body.Close()
					if err != nil {
						return err
					}
				}
			}
			if _, err = p.Verify(signed, n.token, time.Now()); err != nil {
				return err
			}
			p.JSON(w, 200, map[string]string{"stored": id})
			return nil
		case http.MethodDelete:
			if meta {
				return p.Error(405, "method not allowed")
			}
			if err := n.Storage.Remove(id); err != nil {
				return err
			}
			p.JSON(w, 200, map[string]bool{"deleted": true})
			return nil
		}
	}
	if r.Method == http.MethodPost && r.URL.Path == "/repair" {
		var request p.Repair
		if err := p.Decode(r.Body, 32768, &request); err != nil {
			return err
		}
		lease, err := p.Verify(request.Lease, n.token, time.Now())
		if err != nil {
			return err
		}
		if lease.Primary != n.Identity.ID || !n.includes(lease) {
			return p.Error(403, "repair targets another node")
		}
		found := false
		for _, source := range lease.Replicas {
			if source == request.Source && source.ID != n.Identity.ID {
				found = true
			}
		}
		if !found {
			return p.Error(403, "invalid repair source")
		}
		response, err := n.rpc.Do(r.Context(), http.MethodGet, request.Source.URL+"/chunks/"+lease.Chunk.ID, nil, nil)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if err = n.Storage.Put(lease.Chunk, response.Body); err != nil {
			return err
		}
		if _, err = p.Verify(request.Lease, n.token, time.Now()); err != nil {
			return err
		}
		spec, err := n.Storage.Inspect(lease.Chunk.ID)
		if err != nil {
			return err
		}
		p.JSON(w, 200, spec)
		return nil
	}
	return p.Error(404, "route not found")
}
func (n *Node) Heartbeat(ctx context.Context, masterURL string) error {
	chunks, present, err := n.Storage.Inventory()
	if err != nil {
		return err
	}
	return n.rpc.JSON(ctx, http.MethodPost, masterURL+"/heartbeat", p.Inventory{Replica: n.Identity, Chunks: chunks, Present: present}, nil)
}
