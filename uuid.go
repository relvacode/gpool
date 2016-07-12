package gpool

import (
	"crypto/rand"
	"encoding/hex"
)

func uuid() string {
	u := make([]byte, 16)
	_, err := rand.Read(u)
	if err != nil {
		return ""
	}
	// Trim bytes range from rand so we get a valid uuid.
	u[8] = (u[8] | 0x80) & 0xBF
	u[6] = (u[6] | 0x40) & 0x4F
	return hex.EncodeToString(u)
}
