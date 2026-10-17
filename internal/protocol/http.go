package protocol

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

func Decode(r io.Reader, limit int64, target any) error {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return Error(400, "read request: %v", err)
	}
	if int64(len(data)) > limit {
		return Error(413, "request too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return Error(400, "invalid JSON: %v", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Error(400, "expected a single JSON object")
	}
	return nil
}
func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("response write: %v", err)
	}
}
func Fail(w http.ResponseWriter, err error) {
	var fault *Fault
	status := 500
	message := "internal server error"
	if errors.As(err, &fault) {
		status, message = fault.Status, fault.Message
	} else {
		log.Printf("request failed: %v", err)
	}
	JSON(w, status, map[string]string{"error": message})
}

type Handler func(http.ResponseWriter, *http.Request) error

func Guard(token string, handler Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/healthz" {
			JSON(w, 200, map[string]bool{"ok": true})
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			Fail(w, Error(401, "unauthorized"))
			return
		}
		if err := handler(w, r); err != nil {
			Fail(w, err)
		}
	})
}

// RPC never follows redirects: credentials and chunk bytes stay at the selected endpoint.
type RPC struct {
	Token string
	HTTP  *http.Client
}

func NewRPC(token string) *RPC {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &RPC{Token: token, HTTP: &http.Client{Transport: transport, Timeout: TransferSeconds * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (c *RPC) Do(ctx context.Context, method, target string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return nil, Error(503, "peer request failed: %v", err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		defer res.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return nil, Error(res.StatusCode, "%s: %s", req.URL.Path, data)
	}
	return res, nil
}
func (c *RPC) JSON(ctx context.Context, method, target string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	res, err := c.Do(ctx, method, target, body, map[string]string{"Content-Type": "application/json"})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if output == nil {
		_, err = io.Copy(io.Discard, res.Body)
		return err
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 16<<20)).Decode(output); err != nil {
		return fmt.Errorf("decode peer response: %w", err)
	}
	return nil
}
