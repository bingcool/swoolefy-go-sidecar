package ipc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

func ReadFrame(r io.Reader, max int) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	if n < 1 || n > max {
		return nil, fmt.Errorf("frame length %d", n)
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func WriteFrame(w io.Writer, body []byte) error {
	if len(body) < 1 {
		return fmt.Errorf("frame length 0")
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	_, err := io.Copy(w, io.MultiReader(bytes.NewReader(hdr[:]), bytes.NewReader(body)))
	return err
}

func DecodeRequest(body []byte) (Request, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req Request
	if err := dec.Decode(&req); err != nil {
		return Request{}, err
	}
	if dec.More() {
		return Request{}, fmt.Errorf("trailing json")
	}
	return req, nil
}
