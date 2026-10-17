package master

import (
	p "gfs/internal/protocol"
	"net/http"
	"strings"
)

func (m *Master) Handler() http.Handler {
	authenticated := p.Guard(m.config.Token, m.handle)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/readyz" {
			if _, err := m.Store.Snapshot(); err != nil {
				p.Fail(w, err)
			} else {
				p.JSON(w, 200, map[string]bool{"ok": true})
			}
			return
		}
		authenticated.ServeHTTP(w, r)
	})
}
func (m *Master) handle(w http.ResponseWriter, r *http.Request) error {
	switch r.URL.Path {
	case "/status":
		if r.Method == "GET" {
			status, err := m.Status()
			if err != nil {
				return err
			}
			p.JSON(w, 200, status)
			return nil
		}
	case "/heartbeat":
		if r.Method == "POST" {
			var in p.Inventory
			if err := p.Decode(r.Body, 16<<20, &in); err != nil {
				return err
			}
			if err := m.Heartbeat(in); err != nil {
				return err
			}
			p.JSON(w, 200, map[string]int{"accepted": len(in.Chunks)})
			return nil
		}
	case "/files":
		if r.Method == "GET" {
			files, err := m.Store.Files()
			if err != nil {
				return err
			}
			result := []p.Manifest{}
			prefix := r.URL.Query().Get("prefix")
			for _, f := range files {
				if strings.HasPrefix(f.Path, prefix) {
					result = append(result, f)
				}
			}
			p.JSON(w, 200, result)
			return nil
		}
	case "/file":
		path := r.URL.Query().Get("path")
		if err := p.ValidatePath(path); err != nil {
			return err
		}
		if r.Method == "GET" {
			file, err := m.Manifest(path)
			if err != nil {
				return err
			}
			p.JSON(w, 200, file)
			return nil
		}
		if r.Method == "DELETE" {
			generation := r.Header.Get("If-Match")
			if generation == "" {
				return p.Error(428, "If-Match generation is required")
			}
			if err := m.Store.Delete(path, generation); err != nil {
				return err
			}
			p.JSON(w, 200, map[string]bool{"deleted": true})
			return nil
		}
	case "/uploads":
		if r.Method == "POST" {
			var in p.Begin
			if err := p.Decode(r.Body, 2<<20, &in); err != nil {
				return err
			}
			plan, err := m.Begin(in)
			if err != nil {
				return err
			}
			p.JSON(w, 201, plan)
			return nil
		}
	}
	if strings.HasPrefix(r.URL.Path, "/uploads/") {
		rest := strings.TrimPrefix(r.URL.Path, "/uploads/")
		id := strings.TrimSuffix(rest, "/commit")
		commit := rest != id
		if !p.ValidID(id) {
			return p.Error(400, "invalid upload id")
		}
		if r.Method == "POST" && commit {
			file, err := m.Commit(r.Context(), id)
			if err != nil {
				return err
			}
			p.JSON(w, 200, file)
			return nil
		}
		if r.Method == "GET" && !commit {
			u, err := m.Store.Upload(id)
			if err != nil {
				return err
			}
			p.JSON(w, 200, m.Plan(u))
			return nil
		}
		if r.Method == "DELETE" && !commit {
			if err := m.Store.Abort(id); err != nil {
				return err
			}
			p.JSON(w, 200, map[string]bool{"aborted": true})
			return nil
		}
	}
	return p.Error(404, "route not found")
}
