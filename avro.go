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

// Copy from reflect/type.go
var kindNames = []string{
	"invalid",
	"bool",
	"int",
	"int8",
	"int16",
	"int32",
	"int64",
	"uint",
	"uint8",
	"uint16",
	"uint32",
	"uint64",
	"uintptr",
	"float32",
	"float64",
	"complex64",
	"complex128",
	"array",
	"chan",
	"func",
	"interface",
	"map",
	"ptr",
	"slice",
	"string",
	"struct",
	"unsafe.Pointer",
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

func (c *Codec) encodeUnionHook(k reflect.Kind, data interface{}) (interface{}, error) {
	value := reflect.ValueOf(data)
	var valueTypeName string
	if data != nil {
		valueTypeName = value.Type().String()
	} else {
		valueTypeName = "invalid"
	}

	if k == reflect.Ptr && data != nil {
		pointed := value.Elem()

		if !pointed.IsValid() {
			return map[string]interface{}{"null": nil}, nil
		}
		typeName := c.getTypeName(pointed)

		if pointed.Kind() == reflect.Struct {
			typeName = c.addNamespace(typeName)
			s := structs.New(pointed.Interface())
			s.TagName = "avro"
			s.EncodeHook = c.encodeUnionHook
			return map[string]interface{}{typeName: s.Map()}, nil
		}

		if isAvroBaseType(typeName) {
			return map[string]interface{}{typeName: pointed.Interface()}, nil
		}
		typeName = c.addNamespace(typeName)
		pointed = convertToBaseType(pointed)
		return map[string]interface{}{typeName: pointed.Interface()}, nil
	}

	if k == reflect.Struct {
		s := structs.New(data)
		s.TagName = "avro"
		s.EncodeHook = c.encodeUnionHook
		return s.Map(), nil
	}

	if value.Kind() == reflect.Slice && value.Len() > 0 && value.Index(0).Kind() == reflect.Struct {
		var ret []interface{}
		for i := 0; i < value.Len(); i++ {
			s := structs.New(value.Index(i).Interface())
			s.TagName = "avro"
			s.EncodeHook = c.encodeUnionHook
			ret = append(ret, s.Map())
		}
		return ret, nil
	}

	if !isBaseType(valueTypeName) {
		value = convertToBaseType(value)
		return value.Interface(), nil
	}

	return data, nil
}

// isBaseType return is a givem type name is a standard golang type
func isBaseType(typeName string) bool {
	for _, t := range kindNames {
		if typeName == t {
			return true
		}
	}
	return false
}

func isAvroBaseType(avroType string) bool {
	for _, t := range avroBaseTypes {
		if t == avroType {
			return true
		}
	}
	return false
}

// convertToBaseType takes a value which have a custom type (type MyType string) and returns the translation in basic type
func convertToBaseType(value reflect.Value) reflect.Value {
	selectedType := value.Type()
	switch value.Kind() {
	case reflect.String:
		selectedType = reflect.TypeOf("")
	case reflect.Uint:
		selectedType = reflect.TypeOf(uint(0))
	case reflect.Uint8:
		selectedType = reflect.TypeOf(uint8(0))
	case reflect.Uint16:
		selectedType = reflect.TypeOf(uint16(0))
	case reflect.Uint32:
		selectedType = reflect.TypeOf(uint32(0))
	case reflect.Uint64:
		selectedType = reflect.TypeOf(uint64(0))
	case reflect.Int:
		selectedType = reflect.TypeOf(int(0))
	case reflect.Int8:
		selectedType = reflect.TypeOf(int8(0))
	case reflect.Int16:
		selectedType = reflect.TypeOf(int16(0))
	case reflect.Int32:
		selectedType = reflect.TypeOf(int32(0))
	case reflect.Int64:
		selectedType = reflect.TypeOf(int64(0))
	case reflect.Float32:
		selectedType = reflect.TypeOf(float32(0))
	case reflect.Float64:
		selectedType = reflect.TypeOf(float64(0))
	}
	return value.Convert(selectedType)
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
	if avroNamer, ok := (ptrData).(TypeNamer); ok {
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

func (c *Codec) marshal(codec *goavro.Codec, st interface{}) (avro []byte, err error) {
	if !structs.IsStruct(st) {
		return codec.BinaryFromNative(nil, st)
	}
	avroStruct := structs.New(st)
	avroStruct.TagName = "avro"
	avroStruct.EncodeHook = c.encodeUnionHook
	m := avroStruct.Map()
	avro, err = codec.BinaryFromNative(nil, m)
	return avro, err
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
