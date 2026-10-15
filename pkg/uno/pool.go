package uno

import (
	"bytes"
	"sync"
)

// bufferPool recycles bytes.Buffer instances across serialization cycles.
// Reusing buffers eliminates frequent heap allocations, reducing GC pause
// times and preventing latency spikes during hot replication passes.
var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// getBuffer fetches an empty bytes.Buffer from the pool.
func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// putBuffer returns a used bytes.Buffer back to the pool.
func putBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	// Prevent unbounded memory retention if a buffer grew abnormally large
	if buf.Cap() > 65536 {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}
