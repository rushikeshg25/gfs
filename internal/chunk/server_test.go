package chunk

import (
	"context"
	p "gfs/internal/protocol"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testToken = "test-token-at-least-24-characters"

func startTestNode(t *testing.T, id string) (*Node, *httptest.Server) {
	t.Helper()
	s, err := OpenStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(s, p.Replica{ID: id, URL: "http://" + listener.Addr().String()}, testToken)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(n.Handler())
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(func() { server.Close(); s.Close() })
	return n, server
}
func TestLeaseReplicationAndFailure(t *testing.T) {
	a, _ := startTestNode(t, "a")
	b, bserver := startTestNode(t, "b")
	spec := specFor("hello")
	lease := p.Lease{Chunk: spec, Replicas: []p.Replica{a.Identity, b.Identity}, Primary: "a", ExpiresAt: time.Now().Add(time.Minute).UnixMilli()}
	rpc := p.NewRPC(testToken)
	write := func(target string, lease p.Lease) error {
		response, err := rpc.Do(context.Background(), "PUT", target+"/chunks/"+spec.ID, strings.NewReader("hello"), map[string]string{"X-GFS-Lease": p.Sign(lease, testToken)})
		if response != nil {
			response.Body.Close()
		}
		return err
	}
	if err := write(b.Identity.URL, lease); err == nil {
		t.Fatal("secondary accepted primary write")
	}
	expired := lease
	expired.ExpiresAt = 1
	if err := write(a.Identity.URL, expired); err == nil {
		t.Fatal("expired lease accepted")
	}
	if err := write(a.Identity.URL, lease); err != nil {
		t.Fatal(err)
	}
	if actual, err := b.Storage.Inspect(spec.ID); err != nil || actual != spec {
		t.Fatalf("secondary: %+v %v", actual, err)
	}
	response, err := http.Get(a.Identity.URL + "/chunks/" + spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("unauthenticated read succeeded")
	}
	response, err = rpc.Do(context.Background(), "GET", b.Identity.URL+"/chunks/"+spec.ID, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if string(data) != "hello" {
		t.Fatal("wrong bytes")
	}
	bserver.Close()
	if err = write(a.Identity.URL, lease); err == nil {
		t.Fatal("write acknowledged without a secondary")
	}
}
