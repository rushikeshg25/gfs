package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gfs/internal/chunk"
	"gfs/internal/client"
	"gfs/internal/master"
	"gfs/internal/metadata"
	p "gfs/internal/protocol"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const usage = `GFS learning system (Go)
  gfs master                         Run the metadata master
  gfs chunk                          Run a chunkserver
  gfs put LOCAL_FILE /remote/path     Upload or atomically overwrite a file
  gfs get /remote/path LOCAL_FILE     Download; atomically replace destination after verification
  gfs stat /remote/path               Show manifest and live replicas
  gfs ls [/prefix]                    List files
  gfs rm /remote/path                 Delete the observed file generation
  gfs status                         Show node health and replication
  gfs help                           Show this help

Use gfs master -h and gfs chunk -h for server flags.
Client environment: MASTER_URL (default http://127.0.0.1:8080), GFS_TOKEN or GFS_TOKEN_FILE.
Servers require a token of at least 24 characters. Data is retained when a server stops.`

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func token() (string, error) {
	value := os.Getenv("GFS_TOKEN")
	if value == "" {
		path := os.Getenv("GFS_TOKEN_FILE")
		if path == "" {
			return "", fmt.Errorf("set GFS_TOKEN or GFS_TOKEN_FILE")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		value = string(data)
		for len(value) > 0 && (value[len(value)-1] == '\n' || value[len(value)-1] == '\r') {
			value = value[:len(value)-1]
		}
	}
	if len(value) < 24 {
		return "", fmt.Errorf("token must contain at least 24 characters")
	}
	return value, nil
}
func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}
func intEnv(key string, fallback int64) (int64, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}
func serve(ctx context.Context, listener net.Listener, handler http.Handler, maintenance func(context.Context), interval time.Duration) error {
	background, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if maintenance == nil {
			return
		}
		maintenance(background)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-background.Done():
				return
			case <-ticker.C:
				maintenance(background)
			}
		}
	}()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: p.TransferSeconds * time.Second, WriteTimeout: p.TransferSeconds * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	failed := make(chan error, 1)
	go func() { failed <- server.Serve(listener) }()
	log.Printf("listening on %s", listener.Addr())
	var err error
	select {
	case <-ctx.Done():
	case err = <-failed:
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		server.Close()
		if err == nil {
			err = shutdownErr
		}
	}
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func runMaster(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("master", flag.ContinueOnError)
	listen := flags.String("listen", env("LISTEN_ADDR", ":8080"), "listen address")
	data := flags.String("data", env("DATA_DIR", ".data/master"), "persistent data directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected master arguments")
	}
	secret, err := token()
	if err != nil {
		return err
	}
	replication, err := intEnv("REPLICATION", 3)
	if err != nil {
		return err
	}
	size, err := intEnv("CHUNK_SIZE", p.MaxChunkSize)
	if err != nil {
		return err
	}
	timeout, err := durationEnv("NODE_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	ttl, err := durationEnv("UPLOAD_TTL", 15*time.Minute)
	if err != nil {
		return err
	}
	grace, err := durationEnv("GC_GRACE", time.Hour)
	if err != nil {
		return err
	}
	interval, err := durationEnv("REPAIR_INTERVAL", 3*time.Second)
	if err != nil {
		return err
	}
	store, err := metadata.Open(*data)
	if err != nil {
		return err
	}
	defer store.Close()
	m, err := master.New(store, master.Config{Token: secret, Replication: int(replication), ChunkSize: size, NodeTimeout: timeout, UploadTTL: ttl, GCGrace: grace})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	return serve(ctx, listener, m.Handler(), func(ctx context.Context) {
		if err := m.Reconcile(ctx, time.Now()); err != nil && ctx.Err() == nil {
			log.Printf("reconcile: %v", err)
		}
	}, interval)
}
func runChunk(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("chunk", flag.ContinueOnError)
	listen := flags.String("listen", env("LISTEN_ADDR", ":9000"), "listen address")
	data := flags.String("data", env("DATA_DIR", ".data/chunk"), "persistent data directory")
	id := flags.String("id", os.Getenv("NODE_ID"), "unique node ID")
	advertise := flags.String("advertise", os.Getenv("ADVERTISE_URL"), "HTTP origin reachable by clients and peers")
	masterURL := flags.String("master", env("MASTER_URL", "http://127.0.0.1:8080"), "master URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected chunk arguments")
	}
	secret, err := token()
	if err != nil {
		return err
	}
	interval, err := durationEnv("HEARTBEAT_INTERVAL", 3*time.Second)
	if err != nil {
		return err
	}
	storage, err := chunk.OpenStorage(*data)
	if err != nil {
		return err
	}
	defer storage.Close()
	node, err := chunk.NewNode(storage, p.Replica{ID: *id, URL: *advertise}, secret)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	return serve(ctx, listener, node.Handler(), func(ctx context.Context) {
		if err := node.Heartbeat(ctx, *masterURL); err != nil && ctx.Err() == nil {
			log.Printf("heartbeat: %v", err)
		}
	}, interval)
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Println(usage)
		return nil
	}
	switch args[0] {
	case "master":
		return runMaster(ctx, args[1:])
	case "chunk":
		return runChunk(ctx, args[1:])
	}
	secret, err := token()
	if err != nil {
		return err
	}
	c := client.New(env("MASTER_URL", "http://127.0.0.1:8080"), secret)
	var result any
	switch {
	case args[0] == "put" && len(args) == 3:
		result, err = c.Put(ctx, args[1], args[2])
	case args[0] == "get" && len(args) == 3:
		result, err = c.Get(ctx, args[1], args[2])
	case args[0] == "stat" && len(args) == 2:
		result, err = c.Stat(ctx, args[1])
	case args[0] == "ls" && len(args) <= 2:
		prefix := "/"
		if len(args) == 2 {
			prefix = args[1]
		}
		result, err = c.List(ctx, prefix)
	case args[0] == "rm" && len(args) == 2:
		err = c.Delete(ctx, args[1])
		result = map[string]bool{"deleted": err == nil}
	case args[0] == "status" && len(args) == 1:
		result, err = c.Status(ctx)
	default:
		return fmt.Errorf("invalid command\n%s", usage)
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Print(err)
		os.Exit(1)
	}
}
