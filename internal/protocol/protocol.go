// Package protocol defines the wire contract shared by the master, chunkservers and client.
package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
)

const MaxChunkSize int64 = 64 << 20
const MaxFileSize int64 = 1 << 30
const MaxChunks = 4096
const TransferSeconds = 120

type Spec struct {
	ID     string `json:"id"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type Replica struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}
type Chunk struct {
	Spec
	Replicas []Replica `json:"replicas"`
}
type Manifest struct {
	Path       string  `json:"path"`
	Generation string  `json:"generation"`
	Size       int64   `json:"size"`
	Chunks     []Chunk `json:"chunks"`
	CreatedAt  int64   `json:"createdAt"`
}
type Upload struct {
	ID          string  `json:"id"`
	Path        string  `json:"path"`
	Generation  string  `json:"generation"`
	Base        string  `json:"base"`
	Chunks      []Chunk `json:"chunks"`
	ExpiresAt   int64   `json:"expiresAt"`
	Status      string  `json:"status"`
	CommittedAt int64   `json:"committedAt,omitempty"`
}
type Plan struct {
	Upload
	Leases []string `json:"leases"`
}
type Lease struct {
	Chunk     Spec      `json:"chunk"`
	Replicas  []Replica `json:"replicas"`
	Primary   string    `json:"primary"`
	ExpiresAt int64     `json:"expiresAt"`
}
type Inventory struct {
	Replica
	Chunks  []Spec   `json:"chunks"`
	Present []string `json:"present"`
}
type Begin struct {
	Path   string  `json:"path"`
	Size   int64   `json:"size"`
	Chunks []Spec  `json:"chunks"`
	Base   *string `json:"base,omitempty"`
}
type Repair struct {
	Lease  string  `json:"lease"`
	Source Replica `json:"source"`
}
type NodeStatus struct {
	Replica
	Alive  bool  `json:"alive"`
	SeenAt int64 `json:"seenAt"`
	Chunks int   `json:"chunks"`
}
type Status struct {
	Replication     int          `json:"replication"`
	ChunkSize       int64        `json:"chunkSize"`
	Files           int          `json:"files"`
	LiveNodes       int          `json:"liveNodes"`
	Writable        bool         `json:"writable"`
	UnderReplicated int          `json:"underReplicated"`
	Unavailable     int          `json:"unavailable"`
	Nodes           []NodeStatus `json:"nodes"`
	LastRepairError string       `json:"lastRepairError,omitempty"`
}

type Fault struct {
	Status  int
	Message string
}

func (e *Fault) Error() string { return e.Message }
func Error(status int, format string, args ...any) error {
	return &Fault{status, fmt.Sprintf(format, args...)}
}

var idPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var nodePattern = regexp.MustCompile(`^[a-zA-Z0-9-]{1,80}$`)

func NewID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(id[:])
}
func ValidID(id string) bool     { return idPattern.MatchString(id) }
func ValidHash(hash string) bool { return hashPattern.MatchString(hash) }
func ValidatePath(value string) error {
	if value == "/" || !strings.HasPrefix(value, "/") || len(value) > 1024 || strings.ContainsAny(value, "\x00\\") || path.Clean(value) != value {
		return Error(400, "path must be an absolute canonical file path of at most 1024 bytes")
	}
	return nil
}
func (s Spec) Validate() error {
	if !ValidID(s.ID) || s.Size <= 0 || s.Size > MaxChunkSize || !ValidHash(s.SHA256) {
		return Error(400, "invalid chunk descriptor")
	}
	return nil
}
func (r Replica) Validate() error {
	u, err := url.Parse(r.URL)
	if err != nil || !nodePattern.MatchString(r.ID) || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return Error(400, "invalid node identity or HTTP origin")
	}
	return nil
}
func Same(a, b Spec) bool { return a == b }
