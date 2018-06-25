package ioext

import (
	"bytes"
	"sync"
)

const (
	defaultBufferSize    = 4 * 1024
	largeBufferThreshold = 32 * 1024
	maxPoolBufferSize    = 1024 * 1024
)

type bufferPoolT struct {
	regular sync.Pool
	large   sync.Pool
}

var BufferPool = bufferPoolT{
	regular: sync.Pool{
		New: func() interface{} {
			return make([]byte, defaultBufferSize)
		},
	},
	large: sync.Pool{},
}

func (p *bufferPoolT) GetSlice() []byte {
	return p.regular.Get().([]byte)
}

func (p *bufferPoolT) GetLargeSlice() []byte {
	b, ok := p.large.Get().([]byte)
	if !ok {
		b = p.GetSlice()
	}
	return b
}

func (p *bufferPoolT) PutSlice(bs []byte) {
	bs = bs[:cap(bs)]
	if len(bs) < defaultBufferSize || len(bs) > maxPoolBufferSize {
		// ignore too small or too large buffers as it's not worth the re-use
	} else if len(bs) > largeBufferThreshold {
		p.large.Put(bs)
	} else {
		p.regular.Put(bs)
	}
}

func (p *bufferPoolT) GetBuffer() *bytes.Buffer {
	bs := p.GetSlice()
	return bytes.NewBuffer(bs[:0])
}

func (p *bufferPoolT) GetLargeBuffer() *bytes.Buffer {
	bs := p.GetLargeSlice()
	return bytes.NewBuffer(bs[:0])
}

func (p *bufferPoolT) PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	p.PutSlice(buf.Bytes())
}
