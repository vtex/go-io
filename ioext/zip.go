package ioext

import (
	"archive/zip"
	"bytes"
	"io"
)

type ExtractedFiles map[string][]byte

func (files ExtractedFiles) Dispose() {
	for _, b := range files {
		BufferPool.PutSlice(b)
	}
}

func ZipExtract(zippedBytes []byte, wanted ...string) (ExtractedFiles, error) {
	if len(zippedBytes) == 0 {
		return ExtractedFiles{}, nil
	}

	reader, err := zip.NewReader(bytes.NewReader(zippedBytes), int64(len(zippedBytes)))
	if err != nil {
		return nil, err
	}

	var isWanted map[string]bool
	if len(wanted) > 0 {
		isWanted = map[string]bool{}
		for _, w := range wanted {
			isWanted[w] = true
		}
	}

	files := ExtractedFiles{}
	for _, file := range reader.File {
		if isWanted == nil || isWanted[file.Name] {
			content, err := readFile(file)
			if err != nil {
				return nil, err
			}
			files[file.Name] = content
		}
	}
	return files, nil
}

func readFile(f *zip.File) ([]byte, error) {
	fileReader, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	buf := BufferPool.GetBuffer()
	if _, err := buf.ReadFrom(fileReader); err != nil {
		BufferPool.PutBuffer(buf)
		return nil, err
	}
	return buf.Bytes(), nil
}

func ZipCompressTo(w io.Writer, files map[string][]byte) error {
	writer := zip.NewWriter(w)
	for path, content := range files {
		if err := writeFile(writer, path, content); err != nil {
			return err
		}
	}
	return writer.Close()
}

func writeFile(w *zip.Writer, name string, content []byte) error {
	fileWriter, err := w.Create(name)
	if err != nil {
		return err
	}
	_, err = fileWriter.Write(content)
	if err != nil {
		return err
	}
	return nil
}
