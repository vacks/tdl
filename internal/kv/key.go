package kv

// The key spellings below are a file format, not a naming choice. An
// installation upgrading into this package reads the files written by the
// previous one, so "app" and "session" have to keep meaning exactly what they
// meant there.

// App is the key holding which Telegram application identity this account
// presents as. See internal/tgclient: the value selects an entry from a small
// table of application ids.
func App() string {
	return New("app")
}

// Session is the key holding the MTProto authorization session.
//
// It is the one key that does not reach the storage backend as written: the
// account store maps it to a file of its own, next to - and separate from - the
// state directory the other keys live in.
func Session() string {
	return New("session")
}
