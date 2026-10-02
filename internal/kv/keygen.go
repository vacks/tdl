package kv

import (
	"bytes"
	"strings"
	"sync"
)

// keyPool exists because keys are built on every peer lookup, and a peer
// lookup happens on every message the listener sees.
var keyPool = sync.Pool{
	New: func() interface{} {
		b := &bytes.Buffer{}
		b.Grow(16)
		return b
	},
}

// New joins key segments with a colon.
//
// The separator and the order are the on-disk format: every key this
// application has ever written was spelled this way, and a key that changes
// spelling is a key that reads as absent. The result is copied before the pool
// takes its buffer back, because callers keep it as a map index and as a file
// name.
func New(indexes ...string) string {
	buf := keyPool.Get().(*bytes.Buffer)
	buf.WriteString(strings.Join(indexes, ":"))

	result := buf.String()
	buf.Reset()
	keyPool.Put(buf)
	return result
}
