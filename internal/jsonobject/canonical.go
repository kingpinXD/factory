package jsonobject

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var ErrObject = errors.New("invalid bounded JSON object")

// Canonical preserves JSON number lexemes while removing object key ordering.
func Canonical(data []byte, maxBytes int) (json.RawMessage, error) {
	if len(data) == 0 || maxBytes < 1 || len(data) > maxBytes {
		return nil, ErrObject
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := uniqueValue(d); err != nil {
		return nil, ErrObject
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrObject
	}
	var object map[string]any
	d = json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&object); err != nil || object == nil {
		return nil, ErrObject
	}
	return json.Marshal(object)
}

func uniqueValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil || token == nil {
		return ErrObject
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			token, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok || seen[key] {
				return ErrObject
			}
			seen[key] = true
			if err := uniqueValue(d); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrObject
		}
	case '[':
		for d.More() {
			if err := uniqueValue(d); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrObject
		}
	default:
		return ErrObject
	}
	return nil
}
