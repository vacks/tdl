package httpapi

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"
)

func TestRequestHTTPS(t *testing.T) {
	tests := []struct {
		name    string
		tls     bool
		forward string
		want    bool
	}{
		{name: "plain HTTP", want: false},
		{name: "direct HTTPS", tls: true, want: true},
		{name: "reverse proxy HTTPS", forward: "https", want: true},
		{name: "forwarded list", forward: "https, http", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://tdl.example.com/", nil)
			if test.tls {
				r.TLS = &tls.ConnectionState{}
			}
			r.Header.Set("X-Forwarded-Proto", test.forward)
			if got := requestHTTPS(r); got != test.want {
				t.Fatalf("requestHTTPS() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestValidRequestOriginUsesForwardedHTTPS(t *testing.T) {
	s := &Server{}
	r := httptest.NewRequest("POST", "http://tdl.example.com/api/test", nil)
	r.Host = "tdl.example.com"
	r.Header.Set("Origin", "https://tdl.example.com")
	r.Header.Set("X-Forwarded-Proto", "https")
	if !s.validRequestOrigin(r) {
		t.Fatal("same HTTPS origin behind reverse proxy was rejected")
	}
}
