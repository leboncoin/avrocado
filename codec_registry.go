package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
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
var ErrNoEncodeSchema = errors.New("no encoding schema have been initialized")

// Header is the first data of an AvroMessage
type Header struct {
	MagicByte byte
	ID        SchemaID
}

// CodecRegistry is an avro serializer and unserializer which is connected to the schemaregistry
// to dynamically discover and decode schemas
type CodecRegistry struct {
	subject       string
	codecByID     map[SchemaID]*Codec
	schemaByID    map[SchemaID]*Schema
	Registry      SchemaRegistry
	SchemaID      SchemaID
	SchemaVersion int
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
		return errors.Wrap(err, "RegisterNewSchema error")
	}
	isRegistered, schema, err := r.Registry.IsRegistered(r.subject, rawSchema)
	if err != nil {
		return errors.Wrap(err, "IsRegistered error")
	}
	if !isRegistered {
		return errors.Errorf("can't register schema")
	}
	codec, err := NewCodec(rawSchema)
	if err != nil {
		return errors.Wrap(err, "NewCodec error")
	}
	if r.TypeNameEncoder != nil {
		codec.TypeNameEncoder = r.TypeNameEncoder
	}
	r.codecByID[SchemaID(schema.ID)] = codec
	r.schemaByID[SchemaID(schema.ID)] = &schema
	r.SchemaID = SchemaID(schema.ID)
	r.SchemaVersion = schema.Version
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
		return errors.Wrap(err, "binary.Read magic byte error")
	}
	err = binary.Read(binBuffer, DefaultEndianness, &header.ID)
	if err != nil && err != io.EOF {
		return errors.Wrap(err, "binary.Read ID error")
	}
	if header.MagicByte != MagicByte {
		return errors.Errorf("the parsed magic byte %q is not correct (expected %q)", header.MagicByte, MagicByte)
	}

	return r.Unmarshal(binBuffer.Bytes(), to)
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
		codecByID:  make(map[SchemaID]*Codec),
		schemaByID: make(map[SchemaID]*Schema),
		subject:    subject,
		Registry:   schemaRegistry,
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
		r.SchemaVersion = UnknownVersion
		return nil
	}
	isRegistered, schema, err := r.Registry.IsRegistered(r.subject, rawSchema)
	if err != nil {
		return errors.Wrapf(err, "Registry.IsRegistered error for %s", r.subject)
	}

	if !isRegistered {
		if schemaNotRegisteredFunc != nil {
			return schemaNotRegisteredFunc()
		}

		return errors.Errorf("schema %s is not registered in the schema registry", r.subject)
	}
	r.SchemaID = SchemaID(schema.ID)
	codec, err := NewCodec(rawSchema)
	if err != nil {
		return errors.Wrap(err, "NewCodec error")
	}
	r.codecByID[SchemaID(schema.ID)] = codec
	r.schemaByID[SchemaID(schema.ID)] = &schema
	r.SchemaID = SchemaID(schema.ID)
	r.SchemaVersion = schema.Version
	return nil
}

// initAndRegister will call Register ONLY if init returns an ErrSchemaNotRegistered
func (r *CodecRegistry) initAndRegister(rawSchema string) error {
	return r.init(rawSchema, func() error { return r.Register(rawSchema) })
}
