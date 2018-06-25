package ioext

import (
	"io"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const defaultFlushInterval = 50 * time.Millisecond

type ChunkedData struct {
	contentType string
	data        io.Reader
}

func NewChunkedData(contentType string, data io.Reader) *ChunkedData {
	return &ChunkedData{
		contentType: contentType,
		data:        data,
	}
}

func (c *ChunkedData) Render(w http.ResponseWriter) error {
	if c.contentType != "" {
		w.Header().Set("Content-Type", c.contentType)
	}

	err := copyResponse(w, c.data)
	if err != nil {
		logError("chunked_response_write_error", "Failed writing chunked HTTP response", err, nil)
		if cancelErr := cancelChunkedResponse(w); cancelErr != nil {
			logError("chunked_response_cancel_error", "Failed to cancel chunked HTTP response", cancelErr, err)
		}
	}

	// Errors from Render are panicked upon so we always return nil.
	return nil
}

func copyResponse(dst io.Writer, src io.Reader) error {
	if wf, ok := dst.(WriteFlusher); ok {
		af := NewAutoFlusher(wf, defaultFlushInterval)
		defer af.Stop()
		dst = af
	}

	buf := BufferPool.GetSlice()
	defer BufferPool.PutSlice(buf)

	_, err := io.CopyBuffer(dst, src, buf)
	return err
}

// cancelChunkedResponse is a way of signalling to the client that some error
// occurred while transferring that data and thus they should not use it. After
// having transferred all headers and starting the response streaming, the only
// way of doing this is by abruptly cutting the connection thus making the last
// chunk with zero length not reach the client (which is what marks a successful
// end of the chunked transfer). That's exactly what this does :)
func cancelChunkedResponse(w http.ResponseWriter) error {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return errors.New("Response writer is not hijacker")
	}

	conn, _, err := hj.Hijack()
	if err != nil {
		return errors.Wrap(err, "Failed to hijack HTTP connection")
	}

	err = conn.Close()
	if err != nil {
		return errors.Wrap(err, "Failed to close HTTP connection")
	}

	return nil
}

func logError(code, msg string, err, cause error) {
	fields := logrus.Fields{"code": code}
	if cause != nil {
		fields["cause"] = cause.Error()
	}
	logrus.WithError(err).
		WithFields(fields).
		Error(msg)
}
