// Package jstream provides an interface for storing a stream of json into a timeseries
package jstream

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
)

type Inbound json.RawMessage

type Extractor interface {
	NanoTime(in Inbound) int64
	Identity(in Inbound) []byte
}

type Stream struct {
	extractor Extractor
	receive   chan Inbound
	log       map[string][]byte
}

func (s *Stream) Close() {
	close(s.receive)
}

func (s *Stream) ingest() {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)

	for j := range s.receive {
		t := s.extractor.NanoTime(j)
		i := s.extractor.Identity(j)

		m := new(map[string]interface{})
		if err := json.Unmarshal(j, m); err == nil {
			buf.Reset()
			if err := encoder.Encode(m); err == nil {
				identifier := fmt.Sprintf("%s:%i", i, t)
				s.log[identifier] = buf.Bytes()
			}
		}
	}
}

func (s *Stream) Run() func() {
	go s.ingest()
	return s.Close
}

func New(extractor Extractor) *Stream {
	s := new(Stream)
	s.extractor = extractor
	s.receive = make(chan Inbound)
	s.log = make(map[string][]byte)
	return s
}
