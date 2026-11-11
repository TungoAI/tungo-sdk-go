package tungo

import (
	"bytes"
	"encoding/json"
	"reflect"
)

// Nullable holds a property that the API contract allows to be null. It has three states:
//
//   - the zero value: the property is absent (it is omitted from the request);
//   - Null[T](): the property is sent as JSON null;
//   - Value(v): the property is sent with the value v, even when v is a zero value or an
//     empty list.
//
// This is what makes PATCH semantics expressible: "leave it alone", "clear it" and "set it"
// are three different requests.
type Nullable[T any] struct {
	value T
	state uint8 // 0 absent, 1 null, 2 value
}

// Value returns a Nullable that holds v.
func Value[T any](v T) Nullable[T] { return Nullable[T]{value: v, state: 2} }

// Null returns a Nullable that is sent as JSON null.
func Null[T any]() Nullable[T] { return Nullable[T]{state: 1} }

// Get returns the value and true, or the zero value and false when the property is null or absent.
func (n Nullable[T]) Get() (T, bool) { return n.value, n.state == 2 }

// IsNull reports whether the property is an explicit null.
func (n Nullable[T]) IsNull() bool { return n.state == 1 }

// IsZero reports whether the property is absent; encoding/json then omits it (omitzero).
func (n Nullable[T]) IsZero() bool { return n.state == 0 }

// MarshalJSON encodes the value, or null when the property is null. An absent property that is
// required by the contract (and therefore cannot be omitted) is also encoded as null.
func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if n.state != 2 {
		return []byte("null"), nil
	}
	// A value was asked for, so a nil slice or map is an empty list or object, never null.
	switch v := reflect.ValueOf(n.value); v.Kind() {
	case reflect.Slice:
		if v.IsNil() {
			return []byte("[]"), nil
		}
	case reflect.Map:
		if v.IsNil() {
			return []byte("{}"), nil
		}
	}
	return json.Marshal(n.value)
}

// UnmarshalJSON decodes a value or null.
func (n *Nullable[T]) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*n = Null[T]()
		return nil
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*n = Value(v)
	return nil
}

// UnknownVariant is what a tagged union holds when the API sent a tag this SDK version does not
// know. The JSON is kept as it was received and is written back unchanged when encoded.
type UnknownVariant struct {
	// Tag is the value of the discriminator property.
	Tag string
	// Raw is the JSON of the whole object.
	Raw json.RawMessage
}

// MarshalJSON writes the JSON the variant was decoded from.
func (v UnknownVariant) MarshalJSON() ([]byte, error) {
	if len(v.Raw) == 0 {
		return []byte("null"), nil
	}
	return v.Raw, nil
}

// jsonKind reports the shape of a JSON value: "object", "array", "string", "number", "boolean",
// "null", or "" for anything else.
func jsonKind(data []byte) string {
	data = bytes.TrimLeft(data, " \t\r\n")
	if len(data) == 0 {
		return ""
	}
	switch c := data[0]; {
	case c == '{':
		return "object"
	case c == '[':
		return "array"
	case c == '"':
		return "string"
	case c == 't' || c == 'f':
		return "boolean"
	case c == 'n':
		return "null"
	case c == '-' || (c >= '0' && c <= '9'):
		return "number"
	}
	return ""
}

// unionTag reads the discriminator of a tagged union. null reports a JSON null; a missing or
// non-string tag yields "" (the union then holds an *UnknownVariant).
func unionTag(data []byte, property string) (tag string, null bool, err error) {
	switch jsonKind(data) {
	case "null":
		return "", true, nil
	case "object":
	default:
		// Not an object: keep it as an unknown variant instead of failing the whole response.
		return "", false, nil
	}
	var props map[string]json.RawMessage
	if err := json.Unmarshal(data, &props); err != nil {
		return "", false, err
	}
	_ = json.Unmarshal(props[property], &tag)
	return tag, false, nil
}

// unionTagOf reads the discriminator of a variant value by encoding it.
func unionTagOf(variant any, property string) string {
	if u, ok := variant.(*UnknownVariant); ok {
		return u.Tag
	}
	data, err := json.Marshal(variant)
	if err != nil {
		return ""
	}
	tag, _, _ := unionTag(data, property)
	return tag
}

// hasProperties reports whether the JSON object has every one of the named properties.
func hasProperties(data []byte, names ...string) bool {
	var props map[string]json.RawMessage
	if json.Unmarshal(data, &props) != nil {
		return false
	}
	for _, name := range names {
		if _, ok := props[name]; !ok {
			return false
		}
	}
	return true
}

// extraFields returns the properties of the JSON object raw that are not in known.
func extraFields(raw json.RawMessage, known ...string) map[string]json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var props map[string]json.RawMessage
	if json.Unmarshal(raw, &props) != nil {
		return nil
	}
	for _, name := range known {
		delete(props, name)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}
