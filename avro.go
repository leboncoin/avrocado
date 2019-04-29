// Package avro wraps linkedin/goavro to provide serialization and
// deserialization of avro data to go struct with tags.
package avro

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/leboncoin/structs"
	"github.com/linkedin/goavro"
	"github.com/mitchellh/mapstructure"
)

// TypeNameEncoder will transform a Go type name (ex: int, string, ..) into a avro type name
type TypeNameEncoder func(name string) string

// CamelCaseToSnakeCase will transform an input string written in CamelCase into snake_case
func CamelCaseToSnakeCase(name string) string {
	result := camelcase.Split(name)
	for i, s := range result {
		result[i] = strings.ToLower(s)
	}
	return strings.Join(result, "_")
}

var typeConversion = map[string]string{
	"uint8":   "int",
	"uint16":  "int",
	"uint32":  "int",
	"int8":    "int",
	"int16":   "int",
	"int32":   "int",
	"rune":    "int",
	"uint":    "long",
	"int":     "long",
	"uint64":  "long",
	"int64":   "long",
	"float32": "float",
	"float64": "double",
	"bool":    "boolean",
}

var avroBaseTypes = []string{
	"null",
	"boolean",
	"int",
	"long",
	"float",
	"double",
	"bytes",
	"string",
}

// GoToAvroType transforms a Go type name to an Avro type name
// Note that complex types are not handled by this function as there is no
// counterpart
func GoToAvroType(name string) string {
	out, ok := typeConversion[name]
	if !ok {
		return name
	}
	return out
}

// DefaultTypeNameEncoder combines avro type conversion and camel case to snake case
func DefaultTypeNameEncoder(name string) string {
	return CamelCaseToSnakeCase(GoToAvroType(name))
}

// AddNamespace will contact a namespace with the given type name in avro standard way.
func AddNamespace(namespace, typeName string) string {
	if len(namespace) == 0 {
		return typeName
	}
	return namespace + "." + typeName
}

type avroSchemaNamespace struct {
	Namespace string `json:"namespace,omitempty"`
}

// TypeNamer is an interface which will be used during marshalling
// to rename a type into its avro name
type TypeNamer interface {
	AvroName() string
}

// CustomUnmarshaler will allow to define custom unmarshaller.
type CustomUnmarshaler interface {
	UnmarshalAvro(b []byte) error
}

// Marshaler is for types marshaling go types to avro
//
// The Marshaler will understand structure field annotations like
// * "-" to completely omit the field
// * "omitempty" which will not use the field value if it is a zero value and replace it by its default value
//	 if there is one. WARNING: if no default value are provided, the Marshaler will return an error
//
// Marshaler will also handle pointer on values as the default optional value pattern in avro
// (union with null type and null as default value).
type Marshaler interface {
	Marshal(interface{}) ([]byte, error)
}

// Unmarshaler is for types unmarshaling go types to avro
type Unmarshaler interface {
	Unmarshal([]byte, interface{}) error
}

// Codec wraps the goavro.Codec type
type Codec struct {
	goavro.Codec
	// Namespace is the namespace which will be used to encode nested type
	Namespace string
	// TypeNameEncoder will be applied on all type during encoding to transform them from Go name to avro naming convention
	TypeNameEncoder TypeNameEncoder
}

// NewCodec creates a codec from a schema
func NewCodec(schemaSpecification string) (*Codec, error) {
	o, err := goavro.NewCodec(schemaSpecification)
	if err != nil {
		return nil, err
	}
	namespaceStruct := avroSchemaNamespace{}
	err = json.Unmarshal([]byte(schemaSpecification), &namespaceStruct)
	var namespace string
	if err != nil {
		namespace = ""
	} else {
		namespace = namespaceStruct.Namespace
	}
	return &Codec{*o, namespace, DefaultTypeNameEncoder}, nil
}

// Marshal marshals any go type to avro
func (c *Codec) Marshal(st interface{}) ([]byte, error) {
	return c.marshal(&c.Codec, st)
}

// Unmarshal unmarshals any go type from avro
func (c *Codec) Unmarshal(avro []byte, output interface{}) error {
	return c.unmarshal(&c.Codec, avro, output)
}

func (c *Codec) addNamespace(typeName string) string {
	return AddNamespace(c.Namespace, typeName)
}

func (c *Codec) encodeTypeName(name string) string {
	return c.TypeNameEncoder(name)
}

func (c *Codec) encodeUnionHook(kind reflect.Kind, data interface{}) (interface{}, error) {
	value := reflect.ValueOf(data)

	switch kind {
	case reflect.Struct:
		s := structs.New(data)
		s.TagName = "avro"
		s.EncodeHook = c.encodeUnionHook
		data = s.Map()

	case reflect.Slice:
		elemType := getBaseType(reflect.TypeOf(data).Elem())
		newData := reflect.MakeSlice(reflect.SliceOf(elemType), value.Len(), value.Cap())
		for idx := 0; idx < value.Len(); idx++ {
			elem := value.Index(idx)

			val, err := c.encodeUnionHook(elem.Kind(), elem.Interface())
			if err != nil {
				return nil, err
			}

			newData.Index(idx).Set(reflect.ValueOf(val))
		}

		data = newData.Interface()

	case reflect.Ptr:
		if data != nil {
			pointed := value.Elem()
			if pointed.IsValid() {
				val, err := c.encodeUnionHook(pointed.Kind(), pointed.Interface())
				if err != nil {
					return nil, err
				}

				typeName := c.getTypeName(pointed)
				if !isAvroBaseType(typeName) {
					typeName = c.addNamespace(typeName)
				}

				data = map[string]interface{}{
					typeName: val,
				}
			} else {
				data = map[string]interface{}{
					"null": nil,
				}
			}
		}

	default:
		data = convertToBaseType(value).Interface()
	}

	return data, nil
}

func isAvroBaseType(avroType string) bool {
	for _, t := range avroBaseTypes {
		if t == avroType {
			return true
		}
	}
	return false
}

func getBaseType(t reflect.Type) reflect.Type {
	switch t.Kind() {
	// Simple types
	case reflect.String:
		return reflect.TypeOf("")
	case reflect.Uint:
		return reflect.TypeOf(uint(0))
	case reflect.Uint8:
		return reflect.TypeOf(uint8(0))
	case reflect.Uint16:
		return reflect.TypeOf(uint16(0))
	case reflect.Uint32:
		return reflect.TypeOf(uint32(0))
	case reflect.Uint64:
		return reflect.TypeOf(uint64(0))
	case reflect.Int:
		return reflect.TypeOf(int(0))
	case reflect.Int8:
		return reflect.TypeOf(int8(0))
	case reflect.Int16:
		return reflect.TypeOf(int16(0))
	case reflect.Int32:
		return reflect.TypeOf(int32(0))
	case reflect.Int64:
		return reflect.TypeOf(int64(0))
	case reflect.Float32:
		return reflect.TypeOf(float32(0))
	case reflect.Float64:
		return reflect.TypeOf(float64(0))

	// Composed types
	case reflect.Struct, reflect.Ptr:
		return reflect.TypeOf(map[string]interface{}{})
	case reflect.Slice:
		elemType := getBaseType(t.Elem())
		return reflect.SliceOf(elemType)

	default:
		return t
	}
}

// convertToBaseType takes a value which have a custom type (type MyType string) and returns the translation in basic type
func convertToBaseType(value reflect.Value) reflect.Value {
	return value.Convert(getBaseType(value.Type()))
}

// toStructPtr will return a empty interface which is a pointer on the given interface{}
func toStructPtr(obj interface{}) interface{} {
	val := reflect.ValueOf(obj)
	vp := reflect.New(val.Type())
	vp.Elem().Set(val)
	return vp.Interface()
}

func (c *Codec) getTypeName(val reflect.Value) string {
	data := val.Interface()
	// Check if the value implement the interface, with a value as receiver
	if avroNamer, ok := data.(TypeNamer); ok {
		return avroNamer.AvroName()
	}

	ptrData := toStructPtr(data)
	// Check if the pointer on value implement the interface, with a pointer as receiver
	if avroNamer, ok := ptrData.(TypeNamer); ok {
		return avroNamer.AvroName()
	}

	// otherwise return the type name
	return c.encodeTypeName(val.Type().Name())
}

func (c Codec) decodeUnionHook(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
	if to.Kind() == reflect.Ptr && data != nil {
		// Handle optional value as union
		unionData, ok := data.(map[string]interface{})
		// In goavro union types are represented as map[string]interface{} of
		// ONLY ONE VALUE. Hence larger maps do not correspond to union types.
		// We thus keep them unaltered.
		if ok && len(unionData) == 1 {
			for _, val := range unionData {
				return val, nil
			}
		}
	}
	// Lookup for a specific unmarshal method which implements 'CustomUnmarshaler'
	ptrTo := reflect.New(to).Interface()
	if unmarshaler, ok := ptrTo.(CustomUnmarshaler); ok {
		paramVal := []byte(data.(string))
		if err := unmarshaler.UnmarshalAvro(paramVal); err != nil {
			return nil, err
		}
		return unmarshaler, nil
	}
	// Not union or unexpected type, return data unaltered.
	return data, nil
}

func (c *Codec) marshal(codec *goavro.Codec, data interface{}) ([]byte, error) {
	var (
		value = reflect.ValueOf(data)
		kind  = value.Kind()
	)

	switch kind {
	case reflect.Ptr:
		value = value.Elem()
		kind = value.Kind()
		data = value.Interface()
	}

	nativeData, err := c.encodeUnionHook(kind, data)
	if err != nil {
		return nil, err
	}

	return codec.BinaryFromNative(nil, nativeData)
}

func (c *Codec) unmarshal(codec *goavro.Codec, avro []byte, output interface{}) (err error) {
	m, _, err := codec.NativeFromBinary(avro)
	if err != nil {
		return err
	}
	config := mapstructure.DecoderConfig{
		TagName:    "avro",
		DecodeHook: c.decodeUnionHook,
		Result:     output,
	}
	decoder, err := mapstructure.NewDecoder(&config)
	if err != nil {
		return err
	}
	return decoder.Decode(m)
}
