package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// DefaultEndianness is the endianness used for marshal/unmarshalling.
// The value must be the same between the marshaller and unmarshaller in order to work correctly.
// Note: It is set to BigEndian to match the implementation of the kafka-rest Confluent's project
// (see: https://github.com/confluentinc/kafka-rest)
var DefaultEndianness = binary.BigEndian

// SchemaID is the type of the schema's ID return by the registry
type SchemaID int32

// MagicByte define the first byte of a binary avro payload
const MagicByte = 0x0

// UnknownID is the value of a SchemaRegistry ID if it hasn't be registered
const UnknownID SchemaID = -1

// UnknownVersion is the default value without information from the schema registry
const UnknownVersion = -1

// ErrNoEncodeSchema is the error returned when an encode happens without a schema provided
var ErrNoEncodeSchema = fmt.Errorf("no encoding schema have been initialized")

// Header is the first data of an AvroMessage
type Header struct {
	MagicByte byte
	ID        SchemaID
}

// CodecRegistry is an avro serializer and unserializer which is connected to the schemaregistry
// to dynamically discover and decode schemas
type CodecRegistry struct {
	subject   string
	codecByID map[SchemaID]*Codec
	Registry  SchemaRegistry
	SchemaID  SchemaID
	// TypeNameEncoder is the convertion logic to translate type name from go to avro
	TypeNameEncoder TypeNameEncoder
}

// NewCodecRegistry configures a codec connected to the schema registry.
// - registryURL (required) is the complete URL of the schema registry
// - subject (optional) is the name of the subject under which your schema will be registered (often the kafka topic name)
// - schema (optional) is the schema which will be used for encoding
//
// If an empty schema is provided it would be impossible to encode, but the decoding will auto discover the schema type
//
// Note: the CodecRegistry will take care of registering the schema and dynamic decoding
func NewCodecRegistry(registryURL, subject, schema string) (*CodecRegistry, error) {
	return newRegistry(registryURL, subject, schema, func(r *CodecRegistry, rawSchema string) error {
		return r.init(rawSchema, func() error {
			return fmt.Errorf("the given schema is not registered inside %s", registryURL)
		})
	})
}

// NewCodecRegistryAndRegister does a NewCodecRegistry() and a Register()
func NewCodecRegistryAndRegister(registryURL string, subject string, schema string) (*CodecRegistry, error) {
	return newRegistry(registryURL, subject, schema, func(r *CodecRegistry, rawSchema string) error {
		return r.initAndRegister(rawSchema)
	})
}

// SetTypeNameEncoder will set the TypeNameEncoder of the codec registry and apply it to all previously created codec
func (r *CodecRegistry) SetTypeNameEncoder(typeNameEncoder TypeNameEncoder) {
	r.TypeNameEncoder = typeNameEncoder
	for _, codec := range r.codecByID {
		codec.TypeNameEncoder = r.TypeNameEncoder
	}
}

// Register registers a new schema inside the Schema Registry and sets this schema as the default encode and decode schema
func (r *CodecRegistry) Register(rawSchema string) error {
	_, err := r.Registry.RegisterNewSchema(r.subject, rawSchema)
	if err != nil {
		return fmt.Errorf("RegisterNewSchema error: %w", err)
	}
	isRegistered, schema, err := r.Registry.IsRegistered(r.subject, rawSchema)
	if err != nil {
		return fmt.Errorf("IsRegistered error: %w", err)
	}
	if !isRegistered {
		return fmt.Errorf("can't register schema")
	}
	codec, err := NewCodec(rawSchema)
	if err != nil {
		return fmt.Errorf("NewCodec error: %w", err)
	}
	if r.TypeNameEncoder != nil {
		codec.TypeNameEncoder = r.TypeNameEncoder
	}
	r.codecByID[SchemaID(schema.ID)] = codec
	r.SchemaID = SchemaID(schema.ID)
	return nil
}

// Unmarshal implement Unmarshaller
// Note: the Unmarshalling of older schema can be inefficient.
// nolint
func (r *CodecRegistry) Unmarshal(from []byte, to interface{}) error {
	binBuffer := bytes.NewBuffer(from)

	header := Header{}
	err := binary.Read(binBuffer, DefaultEndianness, &header.MagicByte)
	if err != nil && err != io.EOF {
		return fmt.Errorf("binary.Read magic byte error: %w", err)
	}
	err = binary.Read(binBuffer, DefaultEndianness, &header.ID)
	if err != nil && err != io.EOF {
		return fmt.Errorf("binary.Read ID error: %w", err)
	}
	if header.MagicByte != MagicByte {
		return fmt.Errorf("the parsed magic byte %q is not correct (expected %q)", header.MagicByte, MagicByte)
	}

	codec, err := r.getCodecByID(header.ID)
	if err != nil {
		return fmt.Errorf("error when getting codec for schema id %v: %w", header.ID, err)
	}

	if r.SchemaID != UnknownID && header.ID != r.SchemaID {
		tmpTo := make(map[string]interface{})
		err = codec.Unmarshal(binBuffer.Bytes(), &tmpTo)
		if err != nil {
			return err
		}
		from, err = r.Marshal(tmpTo)
		if err != nil {
			return err
		}
		return r.Unmarshal(from, to)
	}
	return codec.Unmarshal(binBuffer.Bytes(), to)
}

// getCodecByID will retriever a codec and its associated schema and check if the schema is correctly registered under the right topic
func (r *CodecRegistry) getCodecByID(ID SchemaID) (*Codec, error) {
	codec, ok := r.codecByID[ID]
	if !ok {
		rawSchema, err := r.Registry.GetSchemaByID(int(ID))
		if err != nil {
			return nil, err
		}

		codec, err = NewCodec(rawSchema)
		if err != nil {
			return nil, err
		}

		r.codecByID[ID] = codec
	}
	return codec, nil
}

// Marshal implements Marshaller
func (r *CodecRegistry) Marshal(data interface{}) ([]byte, error) {
	var binBuffer bytes.Buffer

	if r.SchemaID == UnknownID {
		return nil, ErrNoEncodeSchema
	}
	header := Header{MagicByte: MagicByte, ID: r.SchemaID}
	err := binary.Write(&binBuffer, DefaultEndianness, header)
	if err != nil {
		return nil, err
	}

	byteData, err := r.codecByID[r.SchemaID].Marshal(data)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&binBuffer, DefaultEndianness, byteData)
	if err != nil {
		return nil, err
	}
	return binBuffer.Bytes(), nil
}

func newRegistry(registryURL string, subject string, schema string, initFunc func(*CodecRegistry, string) error) (*CodecRegistry, error) {
	schemaRegistry, err := NewSchemaRegistry(registryURL)
	if err != nil {
		return nil, err
	}
	CodecRegistry := &CodecRegistry{
		codecByID: make(map[SchemaID]*Codec),
		subject:   subject,
		Registry:  schemaRegistry,
	}

	err = initFunc(CodecRegistry, schema)
	if err != nil {
		return nil, err
	}

	return CodecRegistry, nil
}

// init will configure the Codec and register the schema inside the registry
func (r *CodecRegistry) init(rawSchema string, schemaNotRegisteredFunc func() error) error {
	if len(rawSchema) == 0 {
		r.SchemaID = UnknownID
		return nil
	}
	isRegistered, schema, err := r.Registry.IsRegistered(r.subject, rawSchema)
	if err != nil {
		return fmt.Errorf("Registry.IsRegistered error for %s: %w", r.subject, err)
	}

	if !isRegistered {
		if schemaNotRegisteredFunc != nil {
			return schemaNotRegisteredFunc()
		}

		return fmt.Errorf("schema %s is not registered in the schema registry", r.subject)
	}
	r.SchemaID = SchemaID(schema.ID)
	codec, err := NewCodec(rawSchema)
	if err != nil {
		return fmt.Errorf("NewCodec error: %w", err)
	}
	r.codecByID[SchemaID(schema.ID)] = codec
	r.SchemaID = SchemaID(schema.ID)
	return nil
}

// initAndRegister will call Register ONLY if init returns an ErrSchemaNotRegistered
func (r *CodecRegistry) initAndRegister(rawSchema string) error {
	return r.init(rawSchema, func() error { return r.Register(rawSchema) })
}
