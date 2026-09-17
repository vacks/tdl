package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/crypto/argon2"
)

const accountFile = "admin.json"

type account struct {
	Username     string `json:"username"`
	PasswordHash string `json:"password_hash"`
}

type Store struct {
	path string
	mu   sync.RWMutex
	data account
}

func Open(dataDir, username, initialPassword string) (*Store, bool, error) {
	s := &Store{path: filepath.Join(dataDir, accountFile)}
	if data, err := os.ReadFile(s.path); err == nil {
		if err := json.Unmarshal(data, &s.data); err != nil {
			return nil, false, fmt.Errorf("read administrator account: %w", err)
		}
		return s, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, err
	}
	if strings.TrimSpace(initialPassword) == "" {
		return nil, false, errors.New("TDL_ADMIN_INITIAL_PASSWORD is required before the administrator is initialized")
	}

	hash, err := hash(initialPassword)
	if err != nil {
		return nil, false, err
	}
	s.data = account{Username: username, PasswordHash: hash}
	if err := s.save(); err != nil {
		return nil, false, err
	}
	return s, true, nil
}

func (s *Store) Verify(username, password string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return subtle.ConstantTimeCompare([]byte(s.data.Username), []byte(strings.TrimSpace(username))) == 1 && verify(s.data.PasswordHash, password)
}

func (s *Store) Username() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Username
}

func (s *Store) ChangePassword(currentPassword, newPassword string) error {
	if strings.TrimSpace(newPassword) == "" {
		return errors.New("新密码不能为空")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !verify(s.data.PasswordHash, currentPassword) {
		return errors.New("当前密码不正确")
	}
	encoded, err := hash(newPassword)
	if err != nil {
		return err
	}
	previous := s.data.PasswordHash
	s.data.PasswordHash = encoded
	if err := s.save(); err != nil {
		s.data.PasswordHash = previous
		return err
	}
	return nil
}

func (s *Store) save() error {
	data, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	return writePrivateFile(s.path, data)
}

func writePrivateFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}

func hash(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, 32)
	return base64.RawStdEncoding.EncodeToString(salt) + ":" + base64.RawStdEncoding.EncodeToString(key), nil
}

func verify(encoded, password string) bool {
	parts := strings.Split(encoded, ":")
	if len(parts) != 2 {
		return false
	}
	salt, saltErr := base64.RawStdEncoding.DecodeString(parts[0])
	want, keyErr := base64.RawStdEncoding.DecodeString(parts[1])
	if saltErr != nil || keyErr != nil {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}
