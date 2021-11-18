package jstream

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

const RecordSeparator rune = '␞'

type Compress struct {
	next uint64
	dict map[string]uint64
}

func RecurseMap(m *map[string]interface{}, cb func(string, string, interface{}), path string) {
	for key, value := range *m {
		switch v := value.(type) {
		case map[string]interface{}:
			RecurseMap(&v, cb, path+"."+key)
		default:
			cb(path, key, v)
		}
	}
}

func (c *Compress) Deflate(j json.RawMessage) ([]byte, error) {
	m := new(map[string]interface{})
	if err := json.Unmarshal(j, m); err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	valbuf := make([]byte, binary.MaxVarintLen64)

	RecurseMap(m, func(path, key string, value interface{}) {
		val, ok := c.dict[path+"."+key]
		if !ok {
			c.dict[path+"."+key] = c.next
			val = c.next
			c.next++
		}

		n := binary.PutUvarint(valbuf, val)
		buf.Write(valbuf[:n])
		buf.WriteRune(RecordSeparator)

	}, "")

	return buf.Bytes(), nil
}
