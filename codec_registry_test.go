package avro

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var Registry SchemaRegistry

func NewMockCodecRegistry(subject string) *CodecRegistry {
	if Registry == nil {
		if os.Getenv("GO_INTEGRATION_TESTS") != "1" {
			Registry = NewNOOPClient()
		} else {
			Registry, _ = NewSchemaRegistry(SchemaRegistryURL)
		}
	}
	return &CodecRegistry{
		codecByID: make(map[SchemaID]*Codec),
		Registry:  Registry,
		subject:   subject,
	}
}

func TestNewCodecRegistry(t *testing.T) {
	CodecRegistry := NewMockCodecRegistry("test")
	assert.NotNil(t, CodecRegistry)
	schema := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`
	err := CodecRegistry.initAndRegister(schema)
	assert.NoError(t, err)
	assert.Len(t, CodecRegistry.codecByID, 1)
	assert.NotNil(t, CodecRegistry.codecByID[CodecRegistry.SchemaID])
}

func TestCodecRegistry_TypeNameEncoder(t *testing.T) {
	CodecRegistry := NewMockCodecRegistry("test")
	assert.NotNil(t, CodecRegistry)
	schema := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`
	err := CodecRegistry.initAndRegister(schema)
	assert.NoError(t, err)
	CodecRegistry.TypeNameEncoder = func(name string) string {
		return CamelCaseToSnakeCase(name)
	}
	for _, codec := range CodecRegistry.codecByID {
		assert.NotEqual(t, reflect.ValueOf(codec.TypeNameEncoder).Pointer(), reflect.ValueOf(CodecRegistry.TypeNameEncoder).Pointer())
	}
}

func TestCodecRegistry_SetTypeNameEncoder(t *testing.T) {
	CodecRegistry := NewMockCodecRegistry("test")
	assert.NotNil(t, CodecRegistry)
	schema := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`
	err := CodecRegistry.initAndRegister(schema)
	assert.NoError(t, err)
	CodecRegistry.SetTypeNameEncoder(func(name string) string {
		return CamelCaseToSnakeCase(name)
	})
	for _, codec := range CodecRegistry.codecByID {
		assert.EqualValues(t, reflect.ValueOf(codec.TypeNameEncoder).Pointer(), reflect.ValueOf(CodecRegistry.TypeNameEncoder).Pointer())
	}
}

func TestAvroCodecRegistry(t *testing.T) {
	schema := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
	    }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
	}`

	val := Person{"Nico", 36}
	var decoded Person

	codec := NewMockCodecRegistry("test")
	err := codec.initAndRegister(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&val)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, decoded, val)

	// Test with map[string]interface{}
	decodedMap := make(map[string]interface{})
	err = codec.Unmarshal(avro, &decodedMap)
	assert.NoError(t, err)
	assert.Equal(t, "Nico", decodedMap["name"])
	assert.Equal(t, int32(36), decodedMap["age"])
}

type NestedStructForTest struct {
	S string
}

func (n NestedStructForTest) String() string {
	return n.S
}

func (n *NestedStructForTest) UnmarshalAvro(b []byte) error {
	n.S = string(b)
	return nil
}

type TestTypeWithUnmarshalerAndStringer struct {
	N NestedStructForTest `avro:"nested,string"`
}

func TestAvroCodecRegistrySpecificUnmarshaler(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "TestNested",
		"fields": [
			{
				"name": "nested",
				"type": "string"
			}
		]
	}`

	val := TestTypeWithUnmarshalerAndStringer{
		N: NestedStructForTest{S: "the nested string"},
	}
	var decoded TestTypeWithUnmarshalerAndStringer

	codec := NewMockCodecRegistry("nested")
	err := codec.initAndRegister(schema)
	require.NoError(t, err)

	avro, err := codec.Marshal(&val)
	require.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	require.NoError(t, err)

	assert.Equal(t, val, decoded)
}

func TestAvroHeaderInBytes(t *testing.T) {
	schema := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`

	val := Person{"Nico", 36}

	codec := NewMockCodecRegistry("test")
	err := codec.initAndRegister(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&val)
	assert.NoError(t, err)

	header := avro[:5]
	assert.Equal(t, 5, len(header))

	codec2 := NewMockCodecRegistry("test2")
	codec2.Registry = codec.Registry
	err = codec2.initAndRegister(schema)
	assert.NoError(t, err)

	avro, err = codec2.Marshal(&val)
	assert.NoError(t, err)

	header = avro[:5]
	assert.Equal(t, byte(0x0), header[0])
	assert.Equal(t, byte(0x0), header[1])
	assert.Equal(t, byte(0x0), header[2])
	assert.Equal(t, byte(0x0), header[3])
	assert.True(t, header[4] > 0)
}

func Test_should_be_able_to_unmarshal_payload_sent_by_kafkarest(t *testing.T) {
	registry := NewNOOPClient()
	codec := &CodecRegistry{
		codecByID: make(map[SchemaID]*Codec),
		Registry:  registry,
		subject:   "subject",
	}
	_, _ = registry.RegisterNewSchema("subject", "{}")
	err := codec.Register(`{
		"namespace": "n",
		"type": "record",
		"name": "name",
		"fields": [
			{"name": "event_timestamp", "type": "long"},
			{"name": "event_id", "type": "string"},
			{"name": "store_id", "type": "string"}
		]
	}`) // will get id 1, which is the one requested in the payload
	assert.NoError(t, err)
	result := make(map[string]interface{})

	rawKafkarestPayload := []byte{'\x00', '\x00', '\x00', '\x00', '\x01', '\x02', '\x10', '\x65', '\x76', '\x65', '\x6e', '\x74', '\x2d', '\x69', '\x64', '\x10', '\x73', '\x74', '\x6f', '\x72', '\x65', '\x2d', '\x69', '\x64'}
	err = codec.Unmarshal(rawKafkarestPayload, &result)

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"store_id": "store-id", "event_timestamp": int64(1), "event_id": "event-id"}, result)
}

func TestCodecRegistry_Unmarshal_version_upgrade(t *testing.T) {
	schemaV1 := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`
	schemaV2 := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
		},
		{
	      "name": "age",
	      "type": "int"
	    },
		{
	      "name": "height",
	      "type": "int",
          "default": 150
	    }
	  ]
  	}`

	val := Person{"Nico", 36}

	codecV1 := NewMockCodecRegistry("test")
	err := codecV1.initAndRegister(schemaV1)
	assert.NoError(t, err)

	codecV2 := NewMockCodecRegistry("test")
	codecV2.Registry = codecV1.Registry
	err = codecV2.initAndRegister(schemaV2)
	assert.NoError(t, err)

	avro, err := codecV1.Marshal(&val)
	assert.NoError(t, err)
	data := make(map[string]interface{})
	err = codecV2.Unmarshal(avro, &data)
	assert.NoError(t, err)
	assert.NotNil(t, data["name"])
	assert.NotNil(t, data["age"])
	assert.NotNil(t, data["height"])
	assert.Equal(t, int32(150), data["height"])

	var person Person
	err = codecV1.Unmarshal(avro, &person)
	assert.NoError(t, err)
	assert.Equal(t, "Nico", person.Name)
	assert.Equal(t, int32(36), person.Age)
}

func TestCodecRegistry_Unmarshal_version_downgrade(t *testing.T) {
	schemaV1 := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`
	schemaV2 := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
		},
		{
	      "name": "age",
	      "type": "int"
	    },
		{
	      "name": "height",
	      "type": "int",
          "default": 150
	    }
	  ]
  	}`

	data := map[string]interface{}{"name": "Nico", "age": 36, "height": 180}
	codecV1 := NewMockCodecRegistry("test")
	err := codecV1.initAndRegister(schemaV1)
	assert.NoError(t, err)

	codecV2 := NewMockCodecRegistry("test")
	codecV2.Registry = codecV1.Registry
	err = codecV2.initAndRegister(schemaV2)
	assert.NoError(t, err)

	avro, err := codecV1.Marshal(data)
	assert.NoError(t, err)
	decode := make(map[string]interface{})
	err = codecV2.Unmarshal(avro, &decode)
	assert.NoError(t, err)
	assert.NotNil(t, decode["name"])
	assert.NotNil(t, decode["age"])
	assert.NotNil(t, decode["height"])
	assert.Equal(t, int32(150), decode["height"])

	var person Person
	err = codecV2.Unmarshal(avro, &person)
	assert.NoError(t, err)
	assert.Equal(t, "Nico", person.Name)
	assert.Equal(t, int32(36), person.Age)
}

func TestCodecRegistry_Unmarshal_empty_schema(t *testing.T) {
	schemaV1 := `{
	  "type": "record",
	  "name": "Person",
	  "fields": [
	    {
	      "name": "name",
	      "type": "string"
            }, {
	      "name": "age",
	      "type": "int"
	    }
	  ]
  	}`

	data := map[string]interface{}{"name": "Nico", "age": 36, "height": 180}
	codecV1 := NewMockCodecRegistry("test")
	err := codecV1.initAndRegister(schemaV1)
	assert.NoError(t, err)

	codec := NewMockCodecRegistry("test")
	codec.Registry = codecV1.Registry
	err = codec.init("", nil)
	assert.NoError(t, err)

	avro, err := codecV1.Marshal(data)
	assert.NoError(t, err)
	decode := make(map[string]interface{})
	err = codec.Unmarshal(avro, &decode)
	assert.NoError(t, err)
	assert.NotNil(t, decode["name"])
	assert.NotNil(t, decode["age"])
	assert.Nil(t, decode["height"])
}

func TestCodecRegistry_error_on_encode_with_empty_schema(t *testing.T) {
	data := map[string]interface{}{"name": "Nico", "age": 36, "height": 180}
	codec := NewMockCodecRegistry("test")
	err := codec.init("", nil)
	assert.NoError(t, err)

	_, err = codec.Marshal(data)
	assert.Error(t, err)
}
