package avro

import (
	"reflect"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A simple test of Marshaling/Unmarshaling

type Person struct {
	Name string `avro:"name"`
	Age  int32  `avro:"age"`
}

func TestAddNamespace(t *testing.T) {
	require.Equal(t, "mytype", AddNamespace("", "mytype"))
	require.Equal(t, "test.mytype", AddNamespace("test", "mytype"))
}

func TestCamelCaseToSnakeCase(t *testing.T) {
	require.Equal(t, "int", CamelCaseToSnakeCase("int"))
	require.Equal(t, "my_object", CamelCaseToSnakeCase("myObject"))
	require.Equal(t, "my_object", CamelCaseToSnakeCase("MyObject"))
}

func TestDefaultTypeNameEncoder(t *testing.T) {
	require.Equal(t, "long", DefaultTypeNameEncoder("int"))
	require.Equal(t, "my_object", DefaultTypeNameEncoder("MyObject"))
	require.Equal(t, "my_url", DefaultTypeNameEncoder("MyURL"))
	require.Equal(t, "logo_image_url", DefaultTypeNameEncoder("LogoImageURL"))
	// Yes, be careful !
	require.Equal(t, "logo_image_ur_ls", DefaultTypeNameEncoder("LogoImageURLs"))
}

func TestAvroCodec(t *testing.T) {
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

	codec, err := NewCodec(schema)
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

type MyStruct struct {
	MyNull   interface{}
	MyBool   bool
	MyInt    int
	MyLong   int64
	MyFloat  float32
	MyDouble float64
	MyBytes  []byte
	MyString string
}

func TestStructureWithDifferentTypes(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "MyStruct",
      "fields": [
        {
          "name": "MyNull",
          "type": "null"
        }, {
          "name": "MyBool",
          "type": "boolean"
        }, {
          "name": "MyInt",
          "type": "int"
        }, {
          "name": "MyLong",
          "type": "long"
        }, {
          "name": "MyFloat",
          "type": "float"
        }, {
          "name": "MyDouble",
          "type": "double"
        }, {
          "name": "MyBytes",
          "type": "bytes"
        }, {
          "name": "MyString",
          "type": "string"
        }
      ]
    }`

	expected := MyStruct{
		MyNull:   nil,
		MyBool:   true,
		MyInt:    3,
		MyLong:   int64(4),
		MyFloat:  1.5,
		MyDouble: -2.7,
		MyBytes:  []byte{'a', 'b'},
		MyString: "hello",
	}
	var decoded MyStruct

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)

	val := MyStruct{}
	expected = MyStruct{}
	avro, err = codec.Marshal(&val)
	assert.NoError(t, err)

	var decoded2 MyStruct
	err = codec.Unmarshal(avro, &decoded2)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded2)
}

type Child struct {
	Name string `avro:"name"`
}

type Parent struct {
	Children []Child `avro:"children"`
}

func TestArrayOfRecord(t *testing.T) {
	schema := `{
      "name": "Parent",
      "type": "record",
      "fields": [
        {
          "name": "children",
          "type": {
            "type": "array",
            "items": {
              "name": "Child",
              "type": "record",
              "fields": [
                {
                  "name": "name",
                  "type": "string"
                }
              ]
            }
          }
        }
      ]
    }`

	expected := Parent{[]Child{{"Riri"}, {"Fifi"}, {"Loulou"}}}
	var decoded Parent

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)

}

func TestArrayOfEnum(t *testing.T) {
	schema := `{
    "name": "values",
    "type": {
      "type": "array",
      "items": {
        "name": "enum_value",
        "type": "enum",
        "symbols": ["value1", "value2"]
      }
    }
  }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	type EnumValue string

	expected := []EnumValue{"value1", "value2"}

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	var decoded []EnumValue
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

type Address struct {
	Street  string `avro:"street"`
	City    string `avro:"city"`
	Country string `avro:"country"`
	Zip     string `avro:"zip"`
}

type User struct {
	Username string  `avro:"username"`
	Age      int     `avro:"age"`
	Address  Address `avro:"address"`
}

func TestSubStructures(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "User",
      "namespace": "my.example",
      "fields": [
        {
          "name": "username",
          "type": "string"
        },
        {
          "name": "age",
          "type": "int"
        },
        {
          "name": "address",
          "type": {
            "type": "record",
            "name": "mailing_address",
            "fields": [
              {
                "name": "street",
                "type": "string"},
              {
                "name": "city",
                "type": "string"
              },
              {
                "name": "country",
                "type": "string"
              },
              {
                "name": "zip",
                "type": "string"
              }
            ]
          }
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	assert.Equal(t, codec.Namespace, "my.example")

	expected := User{
		Username: "Joe l'embrouille",
		Age:      735,
		Address: Address{
			Street:  "Sunny",
			City:    "Paris",
			Country: "USA",
			Zip:     "75460",
		},
	}
	var decoded User

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

type UserInfo struct {
	Age int `avro:"age"`
}

func TestDefaultValues(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "userInfo",
      "namespace": "my.example",
      "fields": [
        {
          "name": "age",
          "type": "int",
          "default": -1
        }
      ]
    }`

	expected := UserInfo{Age: 3}
	var decoded UserInfo

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)

	// Marshaling a default value results in a mostly empty string
	expected = UserInfo{Age: -1}

	avro, err = codec.Marshal(&expected)
	assert.NoError(t, err)
	assert.Equal(t, avro, []byte{0x1})
}

// go structure with more fields than avro structure

type UserData struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

func TestGoStructureWithMoreFields(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "userInfo",
      "namespace": "my.example",
      "fields": [
        {
          "name": "age",
          "type": "int"
        }
      ]
    }`

	// The field missing in avro is lost
	expected := UserData{"", 1}
	val := UserData{"Guillaume", 1}
	var decoded UserData

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&val)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)

}

func TestAvroStructureWithMoreFields(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "userData",
      "namespace": "my.example",
      "fields": [
        {
          "name": "age",
          "type": "int"
        },
        {
          "name": "name",
          "type": "string"
        }
      ]
    }`

	expected := UserInfo{1}

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	// It fails since there is no default value for name
	_, err = codec.Marshal(&expected)
	assert.Error(t, err)
}

func TestAvroStructureWithMoreFieldsWithDefaultValue(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "userData",
      "namespace": "my.example",
      "fields": [
        {
          "name": "age",
          "type": "int"
        },
        {
          "name": "name",
          "type": "string",
          "default": ""
        }
      ]
    }`

	expected := UserInfo{1}
	var decoded UserInfo

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

func TestUnion(t *testing.T) {
	schema := `["null","string","int"]`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	expected := goavro.Union("string", "testo")
	var decoded map[string]interface{}

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

type UserDataOptional struct {
	Name    *string
	Age     *int
	Married *bool
}

func TestUnionOptional(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "userDataOptional",
      "namespace": "my.example",
      "fields": [
        {
          "name": "Age",
          "type": ["null", "int", "long"]
        },
        {
          "name": "Name",
          "type": ["null", "string"],
          "default": "null"
        },
        {
          "name": "Married",
          "type": ["null", "boolean"],
          "default": "null"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	name := "MyName"
	age := int(42)
	married := true
	expected := UserDataOptional{Name: &name, Age: &age, Married: &married}

	var decoded UserDataOptional

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

func TestCodecRegistry_Marshal_with_namespace(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "userDataOptional",
      "namespace": "lbc",
      "fields": [
        {
          "name": "Age",
          "type": ["null", "int"]
        },
        {
          "name": "Name",
          "type": ["null", "string"],
          "default": "null"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	name := "MyName"
	codec.Namespace = "lbc"
	expected := UserDataOptional{Name: &name, Age: nil}

	var decoded UserDataOptional

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

func TestUnionOptional_with_nil_value(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "user_optional",
      "namespace": "lbc",
      "fields": [
        {
          "name": "Username",
          "type": "string"
        },
        {
          "name": "Age",
          "type": "int"
        },
        {
          "name": "Address",
          "type": [
            "null",
            {
              "type": "record",
              "name": "address_optional",
              "fields": [
                {
                  "name": "street",
                  "type": ["null", "string"]
                },
                {
                  "name": "city",
                  "type": ["null", "string"]
                },
                {
                  "name": "country",
                  "type": ["null", "string"]
                },
                {
                  "name": "zip",
                  "type": ["null", "string"]
                }
              ]
            }
          ],
          "default": "null"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	expected := UserOptional{Username: "Alan Turing", Age: 45, Address: AddressOptional{}}

	var decoded UserOptional

	codec.Namespace = "lbc"
	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

type AddressOptional struct {
	Street  *string `avro:"street"`
	City    *string `avro:"city"`
	Country *string `avro:"country"`
	Zip     *string `avro:"zip"`
}

type UserOptional struct {
	Username string
	Age      int
	Address  AddressOptional `avro:",omitempty"`
}

func TestUnionOptional_with_recursion(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "user_optional",
      "fields": [
        {
          "name": "Username",
          "type": "string"
        },
        {
          "name": "Age",
          "type": "int"
        },
        {
          "name": "Address",
          "type": [
            "null",
            {
              "type": "record",
              "name": "address_optional",
              "fields": [
                {
                  "name": "street",
                  "type": ["null", "string"]
                },
                {
                  "name": "city",
                  "type": ["null", "string"]
                },
                {
                  "name": "country",
                  "type": ["null", "string"]
                },
                {
                  "name": "zip",
                  "type": ["null", "string"]
                }
              ]
            }
          ],
          "default": "null"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	expected := UserOptional{Username: "Alan Turing", Age: 45, Address: AddressOptional{}}

	var decoded UserOptional

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

func TestUnionOptional_with_recursion_with_namespace(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "UserOptional",
      "namespace": "my.example",
      "fields": [
        {
          "name": "Username",
          "type": "string"
        },
        {
          "name": "Age",
          "type": "int"
        },
        {
          "name": "Address",
          "type": [
            "null",
            {
              "type": "record",
              "name": "address_optional",
              "fields": [
                {
                  "name": "street",
                  "type": ["null", "string"]
                },
                {
                  "name": "city",
                  "type": ["null", "string"]
                },
                {
                  "name": "country",
                  "type": ["null", "string"]
                },
                {
                  "name": "zip",
                  "type": ["null", "string"]
                }
              ]
            }
          ],
          "default": "null"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	//address := AddressOptional{}

	//street := "my street"
	expected := UserOptional{Username: "Alan Turing", Age: 45}

	var decoded UserOptional

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

type order struct {
	Services []service `avro:"services"`
}

type service struct {
	Quantity *int32 `avro:"quantity,optional"`
}

func TestUnionOptional_in_array(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "action",
      "fields": [
        {
          "name": "services",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "name": "service",
              "fields": [
                {
                  "name": "quantity",
                  "type": ["null", "int"]
                }
              ]
            }
          }
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	quantity := int32(1)

	tests := []struct {
		name     string
		expected *order
	}{
		{"value", &order{Services: []service{{Quantity: &quantity}}}},
		{"nil", &order{Services: []service{{Quantity: nil}}}},
	}

	for _, tt := range tests {
		var decoded order

		t.Run(tt.name, func(t *testing.T) {
			avro, err := codec.Marshal(tt.expected)
			require.NoError(t, err)

			err = codec.Unmarshal(avro, &decoded)
			require.NoError(t, err)

			require.Len(t, decoded.Services, 1)
			assert.EqualValues(t, tt.expected.Services[0], decoded.Services[0])
		})
	}
}

func TestOptionalLongMarshaling(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "action",
      "namespace": "bidule",
      "fields": [
        {
          "name": "Elements",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "namespace": "bidule.pouet",
              "name": "Element",
              "fields": [
                {
                  "name": "TestLong",
                  "type": ["null", "long"]
                }
              ]
            }
          }
        }
      ]
    }`

	type Element struct {
		TestLong *int64
	}

	type subarray struct {
		Elements []Element
	}

	a := require.New(t)
	codec, err := NewCodec(schema)
	a.NoError(err)
	a.NotNil(codec)

	ptrField := int64(1234)

	val := subarray{
		Elements: []Element{
			{TestLong: &ptrField},
			{},
		},
	}

	_, err = codec.Marshal(val)
	a.NoError(err)
}

type FakeURLs struct {
	URL string `avro:"url"`
}

func (f *FakeURLs) AvroName() string {
	return "fake_urls"
}

type FakeIMGs struct {
	Img string `avro:"img"`
}

func (FakeIMGs) AvroName() string {
	return "fake_imgs"
}

type FakeData struct {
	Fake         FakeURLs  `avro:"fake"`
	FakeImg      FakeIMGs  `avro:"fake_img"`
	FakeOptional *FakeURLs `avro:"fake_opt"`
}

func TestCodec_Marshal_with_custom_name(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "fake_data",
      "fields": [
        {
          "name": "fake",
          "type": {
            "type": "record",
            "name": "fake_urls",
            "fields": [
              {
                "name": "url",
                "type": "string"
              }
            ]
          }
        },
        {
          "name": "fake_img",
          "type": {
            "type": "record",
              "name": "fake_imgs",
              "fields": [
                {
                  "name": "img",
                  "type": "string"
                }
              ]
            }
        },
        {
          "name": "fake_opt",
          "type": [
            "null",
            {
              "type": "record",
              "name": "fake_urls",
              "fields": [
                {
                  "name": "url",
                  "type": "string"
                }
              ]
            }
          ]
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	expected := FakeData{FakeURLs{"test"}, FakeIMGs{"img"}, &FakeURLs{"test2"}}
	var decoded FakeData

	avro, err := codec.Marshal(expected)
	require.NoError(t, err)

	err = codec.Unmarshal(avro, &decoded)
	require.NoError(t, err)

	require.Equal(t, expected, decoded)
}

type MyEnum string

type UnionAndEnumEvent struct {
	MyOptionalEnum *MyEnum `avro:"my_optional_enum"`
	LongID         int     `avro:"long_id"`
	StringID       string  `avro:"string_id"`
}

func TestCodec_Marshal_union_and_enum(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "parent",
      "fields": [
        {
          "name": "my_optional_enum",
          "type": [
            "null",
            {
              "type": "enum",
              "name": "my_enum",
              "symbols": ["value1", "value2"]
            }
          ]
        },
        {
          "name": "long_id",
          "type": "long"
        },
        {
          "name": "string_id",
          "type": "string"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	expected := UnionAndEnumEvent{
		MyOptionalEnum: func(s string) *MyEnum {
			tmp := MyEnum(s)
			return &tmp
		}("value1"),
		StringID: "user",
		LongID:   64,
	}

	avro, err := codec.Marshal(expected)
	require.NoError(t, err)

	var decoded UnionAndEnumEvent
	err = codec.Unmarshal(avro, &decoded)
	require.NoError(t, err)
	require.Equal(t, expected, decoded)
}

type EnumEvent struct {
	MyRequiredEnum MyEnum `avro:"my_required_enum"`
	LongID         int    `avro:"long_id"`
	StringID       string `avro:"string_id"`
}

func TestCodec_Marshal_enum(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "authentication_event",
      "fields": [
        {
          "name": "my_required_enum",
          "type": {
            "type": "enum",
            "name": "my_enum",
            "symbols": ["value1", "value2"]
          }
        },
        {
          "name": "long_id",
          "type": "long"
        },
        {
          "name": "string_id",
          "type": "string"
        }
      ]
    }`

	codec, err := NewCodec(schema)
	assert.NoError(t, err)

	expected := EnumEvent{
		MyRequiredEnum: MyEnum("value1"),
		StringID:       "user",
		LongID:         64,
	}

	avro, err := codec.Marshal(expected)
	require.NoError(t, err)

	var decoded EnumEvent
	err = codec.Unmarshal(avro, &decoded)
	require.NoError(t, err)
	require.Equal(t, expected, decoded)
}

func TestCodec_Marshal_struct_with_union(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "event",
      "fields": [
        {
          "name": "my_struct",
          "type": {
            "type": "record",
            "name": "my_struct_record",
            "fields": [
              {
                "name": "optional_string_id",
                "type": ["null", "string"],
                "default": null
              }
            ]
          }
        }
      ]
    }`

	codec, err := NewCodec(schema)
	require.NoError(t, err)

	type MyStruct struct {
		OptionalStringID *string `avro:"optional_string_id"`
	}

	type StructEvent struct {
		MyStruct MyStruct `avro:"my_struct"`
	}

	expected := StructEvent{
		MyStruct: MyStruct{
			OptionalStringID: func(s string) *string {
				return &s
			}("foo"),
		},
	}

	avro, err := codec.Marshal(expected)
	if assert.NoError(t, err) {
		var decoded StructEvent

		err = codec.Unmarshal(avro, &decoded)
		assert.NoError(t, err)
		assert.Equal(t, expected, decoded)
	}
}

func TestCodec_Marshal_struct_with_union_different_union(t *testing.T) {
	schema := `{
      "type": "record",
      "name": "event",
      "fields": [
        {
          "name": "my_struct",
          "type": {
            "type": "record",
            "name": "my_struct_record",
            "fields": [
              {
                "name": "optional_string_id",
                "type": ["null", "string"],
                "default": null
              }
            ]
          }
        }
      ]
    }`

	codec, err := NewCodec(schema)
	require.NoError(t, err)

	type MyStruct struct {
		OptionalStringID *string `avro:"optional_string_id"`
	}

	type StructEvent struct {
		MyStruct MyStruct `avro:"my_struct"`
	}

	expected := StructEvent{
		MyStruct: MyStruct{
			OptionalStringID: func(s string) *string {
				return &s
			}("foo"),
		},
	}

	avro, err := codec.Marshal(expected)
	if assert.NoError(t, err) {
		var decoded StructEvent

		err = codec.Unmarshal(avro, &decoded)
		assert.NoError(t, err)
		assert.Equal(t, expected, decoded)
	}
}

func TestComplexRecursion(t *testing.T) {
	schema := `{
    "type": "record",
    "name": "event_record",
    "fields": [
      {
        "name": "field",
        "type": {
          "type": "record",
          "name": "field_record",
          "fields": [
            {
              "name": "values",
              "type": {
                "name": "enum_value_array_array",
                "type": "array",
                "items": {
                  "name": "enum_value_array",
                  "type": "array",
                  "items": [
                    "null",
                    {
                      "name": "enum_value",
                      "type": "enum",
                      "symbols": ["value1", "value2"]
                    }
                  ]
                }
              }
            }
          ]
        }
      }
    ]
  }`

	codec, err := NewCodec(schema)
	require.NoError(t, err)

	type EnumValue string

	type EventField struct {
		Values [][]*EnumValue `avro:"values"`
	}

	type Event struct {
		Field EventField `avro:"field"`
	}

	var (
		value1 = EnumValue("value1")
		value2 = EnumValue("value2")
	)

	expected := Event{
		Field: EventField{
			Values: [][]*EnumValue{
				{
					&value2,
					nil,
					&value1,
				},
				{
					&value2,
				},
			},
		},
	}

	avro, err := codec.Marshal(expected)
	if assert.NoError(t, err) {
		var decoded Event
		err = codec.Unmarshal(avro, &decoded)
		assert.NoError(t, err)
		assert.Equal(t, expected, decoded)
	}
}

func Test_convertToBaseType(t *testing.T) {
	type A string
	type B A
	type C map[string]interface{}
	type args struct {
		value reflect.Value
	}
	tests := []struct {
		name string
		args args
		want reflect.Value
	}{
		{"int", args{reflect.ValueOf(int(42))}, reflect.ValueOf(int(42))},
		{"int8", args{reflect.ValueOf(int8(42))}, reflect.ValueOf(int8(42))},
		{"int16", args{reflect.ValueOf(int16(42))}, reflect.ValueOf(int16(42))},
		{"int32", args{reflect.ValueOf(int32(42))}, reflect.ValueOf(int32(42))},
		{"int64", args{reflect.ValueOf(int64(42))}, reflect.ValueOf(int64(42))},
		{"uint", args{reflect.ValueOf(uint(42))}, reflect.ValueOf(uint(42))},
		{"uint8", args{reflect.ValueOf(uint8(42))}, reflect.ValueOf(uint8(42))},
		{"uint16", args{reflect.ValueOf(uint16(42))}, reflect.ValueOf(uint16(42))},
		{"uint32", args{reflect.ValueOf(uint32(42))}, reflect.ValueOf(uint32(42))},
		{"uint64", args{reflect.ValueOf(uint64(42))}, reflect.ValueOf(uint64(42))},
		{"float32", args{reflect.ValueOf(float32(42))}, reflect.ValueOf(float32(42))},
		{"float64", args{reflect.ValueOf(float64(42))}, reflect.ValueOf(float64(42))},
		{"string", args{reflect.ValueOf("plop")}, reflect.ValueOf("plop")},
		{"map", args{reflect.ValueOf(map[int]string{42: "plop"})}, reflect.ValueOf(map[int]string{42: "plop"})},
		{"no effect on map", args{reflect.ValueOf(C(map[string]interface{}{"plop": 42}))}, reflect.ValueOf(C(map[string]interface{}{"plop": 42}))},
		{"alias on string", args{reflect.ValueOf(A("plop"))}, reflect.ValueOf("plop")},
		{"alias on alias on string", args{reflect.ValueOf(B("plop"))}, reflect.ValueOf("plop")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertToBaseType(tt.args.value); got.Kind() != tt.want.Kind() {
				t.Errorf("convertToBaseType() = %v, want %v", got, tt.want)
			}
		})
	}
}
