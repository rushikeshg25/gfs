package master

import (
	"context"
	p "gfs/internal/protocol"
	"net/http"
	"time"
)

func (m *Master) repairError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.lastRepairError = ""
	} else {
		m.lastRepairError = err.Error()
	}
}

// Reconcile repairs published chunks, then collects expired generations after a grace period.
// Calls serialize; the caller owns scheduling and cancellation.
func (m *Master) Reconcile(ctx context.Context, now time.Time) error {
	m.maintenance.Lock()
	defer m.maintenance.Unlock()
	m.repairError(nil)
	files, err := m.Store.Files()
	if err != nil {
		return err
	}
	for _, file := range files {
		for _, c := range file.Chunks {
			if err = ctx.Err(); err != nil {
				return err
			}
			locations := m.Locations(c.Spec)
			if len(locations) == 0 || len(locations) >= m.config.Replication {
				continue
			}
			for _, target := range m.alive() {
				if len(locations) >= m.config.Replication {
					break
				}
				already := false
				for _, r := range locations {
					if r.ID == target.ID {
						already = true
					}
				}
				if already {
					continue
				}
				for _, source := range locations {
					lease := p.Sign(p.Lease{Chunk: c.Spec, Replicas: []p.Replica{source, target}, Primary: target.ID, ExpiresAt: time.Now().Add(p.TransferSeconds * time.Second).UnixMilli()}, m.config.Token)
					var actual p.Spec
					err = m.rpc.JSON(ctx, http.MethodPost, target.URL+"/repair", p.Repair{Lease: lease, Source: source}, &actual)
					if err == nil && !p.Same(actual, c.Spec) {
						err = p.Error(502, "repair verification failed")
					}
					if err != nil {
						m.repairError(err)
						continue
					}
					m.remember(target, actual)
					locations = m.Locations(c.Spec)
					break
				}
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	garbage, err := m.Store.Garbage(now.UnixMilli(), m.config.GCGrace.Milliseconds())
	if err != nil {
		return err
	}
	for _, c := range garbage {
		for _, target := range m.alive() {
			if err = ctx.Err(); err != nil {
				return err
			}
			m.mu.RLock()
			n := m.nodes[target.ID]
			present := n != nil && n.present[c.ID]
			m.mu.RUnlock()
			if !present {
				continue
			}
			err = m.rpc.JSON(ctx, http.MethodDelete, target.URL+"/chunks/"+c.ID, nil, nil)
			if err != nil {
				m.repairError(err)
				continue
			}
			m.mu.Lock()
			n = m.nodes[target.ID]
			if n != nil && n.Replica == target {
				delete(n.chunks, c.ID)
				delete(n.present, c.ID)
			}
			m.mu.Unlock()
		}
	}
	return nil
}
