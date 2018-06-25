package ioext

import (
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
)

func Tee(in io.Reader) (main, secondary io.ReadCloser) {
	copy, pipe := io.Pipe()
	tee := &teeReader{in, pipe, atomic.Value{}}
	tee.done.Store(false)
	return tee, copy
}

type teeReader struct {
	in   io.Reader
	pipe *io.PipeWriter
	done atomic.Value
}

func (t *teeReader) Read(p []byte) (int, error) {
	n, err := t.in.Read(p)
	if n > 0 {
		if n, err := t.pipe.Write(p[:n]); err != nil && err != io.ErrClosedPipe {
			return n, err
		}
	}
	if err == io.EOF {
		t.done.Store(true)
		t.pipe.Close()
	}
	return n, err
}

func (t *teeReader) Close() (err error) {
	if closer, ok := t.in.(io.ReadCloser); ok {
		err = closer.Close()
	}
	if !t.done.Load().(bool) {
		t.pipe.CloseWithError(errors.New("Reading closed before EOF"))
	}
	return
}
