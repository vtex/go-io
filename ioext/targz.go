package ioext

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"path"
)

type TarGzWriter struct {
	gzWriter  *gzip.Writer
	tarWriter *tar.Writer
}

func NewTarGzWriter(w io.Writer) *TarGzWriter {
	gzWriter := gzip.NewWriter(w)
	tarWriter := tar.NewWriter(gzWriter)
	return &TarGzWriter{
		gzWriter:  gzWriter,
		tarWriter: tarWriter,
	}
}

func (w *TarGzWriter) Write(filename string, content []byte) error {
	header := &tar.Header{
		Name: normalizePath(filename),
		Size: int64(len(content)),
		Mode: 0777,
	}
	if err := w.tarWriter.WriteHeader(header); err != nil {
		return err
	} else if _, err := w.tarWriter.Write(content); err != nil {
		return err
	}

	return nil
}

func (w *TarGzWriter) Close() error {
	tarErr := w.tarWriter.Close()
	gzErr := w.gzWriter.Close()
	if tarErr != nil {
		return tarErr
	}
	return gzErr
}

func normalizePath(p string) string {
	p = path.Clean(p)
	if path.IsAbs(p) {
		p = p[1:]
	}
	// yarn needs ./ prefix in tarball files
	return "./" + p
}
