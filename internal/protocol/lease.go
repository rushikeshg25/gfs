package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

func Sign(lease Lease, secret string) string {
	data, _ := json.Marshal(lease)
	body := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func Verify(value, secret string, now time.Time) (Lease, error) {
	var lease Lease
	parts := strings.Split(value, ".")
	if len(parts) != 2 || len(value) > 16000 {
		return lease, Error(403, "invalid lease")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return lease, Error(403, "invalid lease")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(parts[0]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return lease, Error(403, "invalid lease signature")
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return lease, Error(403, "invalid lease")
	}
	if err = json.Unmarshal(data, &lease); err != nil {
		return lease, Error(403, "invalid lease payload")
	}
	if err = lease.Chunk.Validate(); err != nil {
		return lease, err
	}
	if lease.ExpiresAt <= now.UnixMilli() {
		return lease, Error(409, "lease expired")
	}
	if len(lease.Replicas) == 0 || len(lease.Replicas) > 10 {
		return lease, Error(403, "invalid lease replicas")
	}
	found := false
	ids := map[string]bool{}
	for _, r := range lease.Replicas {
		if err = r.Validate(); err != nil {
			return lease, err
		}
		if ids[r.ID] {
			return lease, Error(403, "duplicate replica")
		}
		ids[r.ID] = true
		found = found || r.ID == lease.Primary
	}
	if !found {
		return lease, Error(403, "invalid primary")
	}
	return lease, nil
}
