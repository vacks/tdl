package httpapi

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"

	"github.com/vacks/tdl/internal/config"
)

func TestRequestHTTPS(t *testing.T) {
	trusted := config.Config{TrustedProxies: []string{"127.0.0.1", "10.0.0.0/8"}}
	tests := []struct {
		name    string
		tls     bool
		forward string
		remote  string
		trusted []string
		want    bool
	}{
		{name: "plain HTTP", remote: "203.0.113.9:5555", want: false},
		{name: "direct HTTPS", tls: true, remote: "203.0.113.9:5555", want: true},
		{name: "reverse proxy HTTPS", forward: "https", remote: "127.0.0.1:5555", trusted: trusted.TrustedProxies, want: true},
		{name: "forwarded list", forward: "https, http", remote: "127.0.0.1:5555", trusted: trusted.TrustedProxies, want: true},
		{name: "trusted CIDR", forward: "https", remote: "10.1.2.3:5555", trusted: trusted.TrustedProxies, want: true},
		// The header is forgeable by anyone who can open a connection, so an
		// unconfigured or untrusted peer must never be able to change the cookie
		// transport or the scheme the same-origin check compares against.
		{name: "untrusted peer", forward: "https", remote: "203.0.113.9:5555", trusted: trusted.TrustedProxies, want: false},
		{name: "no trusted proxies configured", forward: "https", remote: "127.0.0.1:5555", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Server{cfg: config.Config{TrustedProxies: test.trusted}}
			r := httptest.NewRequest("GET", "http://tdl.example.com/", nil)
			r.RemoteAddr = test.remote
			if test.tls {
				r.TLS = &tls.ConnectionState{}
			}
			r.Header.Set("X-Forwarded-Proto", test.forward)
			if got := s.requestHTTPS(r); got != test.want {
				t.Fatalf("requestHTTPS() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestValidRequestOriginUsesForwardedHTTPS(t *testing.T) {
	s := &Server{cfg: config.Config{TrustedProxies: []string{"127.0.0.1"}}}
	r := httptest.NewRequest("POST", "http://tdl.example.com/api/test", nil)
	r.RemoteAddr = "127.0.0.1:5555"
	r.Host = "tdl.example.com"
	r.Header.Set("Origin", "https://tdl.example.com")
	r.Header.Set("X-Forwarded-Proto", "https")
	if !s.validRequestOrigin(r) {
		t.Fatal("same HTTPS origin behind reverse proxy was rejected")
	}
}
