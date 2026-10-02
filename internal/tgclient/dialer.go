package tgclient

import (
	"net/url"

	"github.com/go-faster/errors"
	"github.com/iyear/connectproxy"
	"golang.org/x/net/proxy"
)

func init() {
	// golang.org/x/net/proxy understands socks5 and direct connections; this
	// registers the http and https schemes, which is what an operator behind a
	// corporate or self-hosted proxy is most likely to have. TLS verification to
	// the proxy itself is skipped, which is the library's own recommendation for
	// this use: the connection is encrypted either way, and a proxy is often
	// reached by an address its certificate does not name.
	connectproxy.Register(&connectproxy.Config{
		InsecureSkipVerify: true,
	})
}

// proxyDialer turns the configured proxy URL into a dialer for the MTProto
// connection.
//
// An unparseable or unsupported URL is an error the caller reports. It used to
// be swallowed at this level, which turned "the proxy is spelled wrong" into a
// connection that quietly went direct.
func proxyDialer(raw string) (proxy.ContextDialer, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, errors.Wrap(err, "parse proxy url")
	}
	dialer, err := proxy.FromURL(parsed, proxy.Direct)
	if err != nil {
		return nil, errors.Wrap(err, "proxy from url")
	}
	if contextual, ok := dialer.(proxy.ContextDialer); ok {
		return contextual, nil
	}
	return nil, errors.New("proxy dialer is not ContextDialer")
}
