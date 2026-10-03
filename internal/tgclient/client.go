package tgclient

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"golang.org/x/net/proxy"

	"github.com/vacks/tdl/internal/kv"
)

// device is how this application introduces itself to Telegram.
//
// It is the official desktop client's own identification, and it is not
// decoration: an account authorized as one client and then claiming to be
// another is exactly the kind of mismatch that draws attention. The values are
// the ones the account was authorized with, so they do not change.
var device = telegram.DeviceConfig{
	DeviceModel:    "Desktop",
	SystemVersion:  "Windows 10",
	AppVersion:     "4.2.4 x64",
	LangCode:       "en",
	SystemLangCode: "en-US",
	LangPack:       "tdesktop",
}

// Options describes one account's client.
type Options struct {
	// KV holds the session and the application identity. It is normally an
	// account's private store, because a session belongs to the account it was
	// authorized for.
	KV kv.Storage
	// Login selects the fresh-login behaviour: the client will not resume an
	// existing session. See kv.NewSession.
	Login bool
	// Proxy is the configured proxy URL, empty for a direct connection.
	Proxy string
	// ReconnectTimeout bounds how long a broken connection is retried.
	ReconnectTimeout time.Duration
	// UpdateHandler receives the account's updates.
	UpdateHandler telegram.UpdateHandler
	// OnSelfSuccess is called every time the connection is established, which
	// includes every reconnect.
	//
	// It is the only signal that a dropped connection came back. The callback
	// passed to Client.Run is not called again - it is invoked once, when the
	// session first becomes ready, and a reconnect inside that same Run neither
	// calls it again nor cancels its context. Anything that has to happen after
	// an outage therefore has to hang off this.
	OnSelfSuccess func(self *tg.User)
	// Middlewares are appended to DefaultMiddlewares, which places them
	// innermost - see DefaultMiddlewares for why that matters.
	Middlewares []telegram.Middleware
}

// New builds the client for one account.
func New(ctx context.Context, o Options) (*telegram.Client, error) {
	identity, err := application(ctx, o.KV)
	if err != nil {
		return nil, err
	}

	// The dialer is chosen once, here: the client reads it when it opens a
	// connection, so a proxy that changes later needs a new client rather than a
	// new setting.
	dialer := proxy.Direct.DialContext
	if o.Proxy != "" {
		proxied, err := proxyDialer(o.Proxy)
		if err != nil {
			return nil, err
		}
		dialer = proxied.DialContext
	}

	return telegram.NewClient(identity.id, identity.hash, telegram.Options{
		Resolver: dcs.Plain(dcs.PlainOptions{Dial: dialer}),
		// A new backoff per reconnection, because a backoff carries its own
		// attempt state and a shared one would keep growing across reconnects.
		ReconnectionBackoff: func() backoff.BackOff { return reconnectBackoff(o.ReconnectTimeout) },
		Logger:              gotdLogger(),
		UpdateHandler:       o.UpdateHandler,
		OnSelfSuccess:       o.OnSelfSuccess,
		Device:              device,
		SessionStorage:      kv.NewSession(o.KV, o.Login),
		RetryInterval:       5 * time.Second,
		MaxRetries:          5,
		DialTimeout:         10 * time.Second,
		// Defaults first, caller's last: the caller's middlewares end up innermost.
		Middlewares: append(DefaultMiddlewares(ctx, o.ReconnectTimeout), o.Middlewares...),
	}), nil
}
