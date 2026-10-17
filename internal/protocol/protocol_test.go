package protocol

import (
	"strings"
	"testing"
	"time"
)

func TestValidationAndLeases(t *testing.T) {
	for _, p := range []string{"relative", "/", "/a/../b", "/a//b", "/a/", "/a\x00"} {
		if ValidatePath(p) == nil {
			t.Fatalf("accepted %q", p)
		}
	}
	if err := ValidatePath("/日本語/file"); err != nil {
		t.Fatal(err)
	}
	lease := Lease{Chunk: Spec{NewID(), 5, strings.Repeat("a", 64)}, Replicas: []Replica{{"node-0", "http://localhost:9000"}}, Primary: "node-0", ExpiresAt: 2000}
	signed := Sign(lease, "secret")
	if _, err := Verify(signed, "secret", time.UnixMilli(1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(signed, "wrong", time.UnixMilli(1000)); err == nil {
		t.Fatal("accepted tampered lease")
	}
	if _, err := Verify(signed, "secret", time.UnixMilli(2000)); err == nil {
		t.Fatal("accepted expired lease")
	}
}

func TestBoundedJSON(t *testing.T) {
	for _, body := range []string{`{"unexpected":true}`, `{} {}`, `not-json`} {
		var out Begin
		if err := Decode(strings.NewReader(body), 1024, &out); err == nil {
			t.Fatalf("accepted %q", body)
		}
	}
	var out Begin
	if err := Decode(strings.NewReader(strings.Repeat(" ", 20)), 10, &out); err == nil {
		t.Fatal("oversized JSON accepted")
	}
}
