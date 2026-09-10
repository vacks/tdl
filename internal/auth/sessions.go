package auth

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"
)

type Sessions struct {
	mu     sync.Mutex
	values map[string]time.Time
}

func NewSessions() *Sessions { return &Sessions{values: make(map[string]time.Time)} }

func (s *Sessions) Create() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(b)
	s.mu.Lock()
	s.values[token] = time.Now().Add(24 * time.Hour)
	s.mu.Unlock()
	return token, nil
}

func (s *Sessions) Valid(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	expires, ok := s.values[token]
	if !ok || time.Now().After(expires) {
		delete(s.values, token)
		return false
	}
	return true
}

func (s *Sessions) Delete(token string) {
	s.mu.Lock()
	delete(s.values, token)
	s.mu.Unlock()
}

// Clear invalidates all administrator sessions, including the session that
// changed the password. This prevents older cookies from remaining usable.
func (s *Sessions) Clear() {
	s.mu.Lock()
	s.values = make(map[string]time.Time)
	s.mu.Unlock()
}
