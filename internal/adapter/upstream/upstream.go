// Package upstream is the only layer that may import iyear/tdl packages.
// Web handlers and task services communicate with this adapter through local types.
package upstream

import (
	upstreamLogin "github.com/iyear/tdl/app/login"
	"github.com/iyear/tdl/core/downloader"
)

const Version = "v0.20.4"

// These compile-time references make the dependency boundary explicit. The web
// QR-login and download adapters will be implemented here without exposing
// upstream types to the rest of this application.
var (
	_ = upstreamLogin.QR
	_ downloader.Iter
)
