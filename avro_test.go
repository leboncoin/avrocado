package avro

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/linkedin/goavro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// testdataDirpath is the full path of the testdata directory
	// inside this Go module directory.
	testdataDirpath string
)

func init() {
	_, modFilePath, _, _ := runtime.Caller(0)
	modDirPath := filepath.Dir(modFilePath)
	testdataDirpath = filepath.Join(modDirPath, "testdata")
}

func readAvroSchemaFromFile(t *testing.T, location string) *Codec {
	t.Helper()

	filePath := filepath.Join(testdataDirpath, location)

	schema, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)

	codec, err := NewCodec(string(schema))
	require.NoError(t, err)

	return codec
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
	codec := readAvroSchemaFromFile(t, "simple-record.json")

	type Person struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}

	val := Person{"Nico", 36}
	avro, err := codec.Marshal(&val)
	assert.NoError(t, err)

	var decoded Person
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

func TestStructureWithDifferentTypes(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "my-struct.json")

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

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	var decoded MyStruct
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)

	// XXX nil bytes are converted to an empty slice
	val := MyStruct{}
	expected = MyStruct{MyBytes: []byte{}}
	avro, err = codec.Marshal(&val)
	assert.NoError(t, err)

	var decoded2 MyStruct
	err = codec.Unmarshal(avro, &decoded2)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded2)
}

func TestArray(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "array.json")

	type Child struct {
		Name string `avro:"name"`
	}

	type Parent struct {
		Children []Child `avro:"children"`
	}

	expected := Parent{
		Children: []Child{
			{Name: "Riri"},
			{Name: "Fifi"},
			{Name: "Loulou"},
		},
	}

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	var decoded Parent
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	assert.Equal(t, expected, decoded)
}

func TestSubStructures(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "sub-structs.json")
	assert.Equal(t, codec.Namespace, "my.example")

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

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	var decoded User
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestDefaultValues(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "simple-record-with-default-age.json")

	type Person struct {
		Age int `avro:"age"`
	}

	expected := Person{
		Age: 3,
	}

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)

	// Marshaling a default value results in a mostly empty string
	avro, err = codec.Marshal(&Person{
		Age: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, avro, []byte{0x1})
}

// go structure with more fields than avro structure

func TestGoStructureWithMoreFields(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "simple-record-without-name.json")

	type Person struct {
		Name string `avro:"name"`
		Age  int    `avro:"age"`
	}

	avro, err := codec.Marshal(&Person{
		Name: "Guillaume",
		Age:  1,
	})
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)

	// The field missing in avro is lost
	assert.Equal(t, Person{
		Name: "",
		Age:  1,
	}, decoded)
}

func TestAvroStructureWithMoreFields(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "simple-record.json")

	type Person struct {
		Age int `avro:"age"`
	}

	// It fails since there is no default value for name
	_, err := codec.Marshal(&Person{
		Age: 1,
	})
	assert.Error(t, err)
}

func TestAvroStructureWithMoreFieldsWithDefaultValue(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "simple-record-with-default-name.json")

	type Person struct {
		Age int `avro:"age"`
	}

	expected := Person{
		Age: 1,
	}

	avro, err := codec.Marshal(&expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestUnion(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "union.json")

	expected := goavro.Union("string", "testo")

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	var decoded map[string]interface{}
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestUnionOptional(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "record-with-optional-fields.json")

	type Person struct {
		Name    *string
		Age     *int
		Married *bool
	}

	name := "MyName"
	age := int(42)
	married := true
	expected := Person{
		Name:    &name,
		Age:     &age,
		Married: &married,
	}

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestRecordWithNamespace(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "record-with-namespace.json")
	assert.Equal(t, codec.Namespace, "lbc")

	type Person struct {
		Name *string
		Age  *int
	}

	name := "MyName"
	expected := Person{
		Name: &name,
		Age:  nil,
	}

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestUnionOptional_with_nil_value(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "union-with-record.json")

	type AddressOptional struct {
		Street  *string `avro:"street"`
		City    *string `avro:"city"`
		Country *string `avro:"country"`
		Zip     *string `avro:"zip"`
	}

	type Person struct {
		Name    string
		Age     int
		Address AddressOptional `avro:",omitempty"`
	}

	expected := Person{
		Name:    "Alan Turing",
		Age:     45,
		Address: AddressOptional{},
	}

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestUnionOptional_with_recursion(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "union-with-record.json")

	type AddressOptional struct {
		Street  *string `avro:"street"`
		City    *string `avro:"city"`
		Country *string `avro:"country"`
		Zip     *string `avro:"zip"`
	}

	type Person struct {
		Name    string
		Age     int
		Address AddressOptional `avro:",omitempty"`
	}

	expected := Person{
		Name:    "Alan Turing",
		Age:     45,
		Address: AddressOptional{},
	}

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestUnionOptional_with_recursion_with_namespace(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "union-with-record.json")

	type AddressOptional struct {
		Street  *string `avro:"street"`
		City    *string `avro:"city"`
		Country *string `avro:"country"`
		Zip     *string `avro:"zip"`
	}

	type Person struct {
		Name    string
		Age     int
		Address AddressOptional `avro:",omitempty"`
	}

	expected := Person{
		Name: "Alan Turing",
		Age:  45,
	}

	avro, err := codec.Marshal(expected)
	assert.NoError(t, err)

	var decoded Person
	err = codec.Unmarshal(avro, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestUnionOptional_in_array(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "union-with-array.json")

	quantity := int32(1)

	type service struct {
		Quantity *int32 `avro:"quantity,optional"`
	}

	type order struct {
		Services []service `avro:"services"`
	}

	for testName, expected := range map[string]order{
		"value": {
			Services: []service{
				{
					Quantity: &quantity,
				},
			},
		},
		"nil": {
			Services: []service{
				{
					Quantity: nil,
				},
			},
		},
	} {
		t.Run(testName, func(t *testing.T) {
			avro, err := codec.Marshal(&expected)
			require.NoError(t, err)

			var decoded order
			err = codec.Unmarshal(avro, &decoded)
			require.NoError(t, err)

			require.Len(t, decoded.Services, 1)
			assert.EqualValues(t, expected.Services[0], decoded.Services[0])
		})
	}
}

func TestOptionalLongMarshaling(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "record-with-array.json")

	type element struct {
		TestLong *int64
	}

	type subarray struct {
		Elements []element
	}

	ptrField := int64(1234)

	val := subarray{
		Elements: []element{
			{
				TestLong: &ptrField,
			},
			{},
		},
	}

	_, err := codec.Marshal(val)
	assert.NoError(t, err)
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
	codec := readAvroSchemaFromFile(t, "record-with-different-field-type.json")

	expected := FakeData{
		Fake: FakeURLs{
			URL: "test",
		},
		FakeImg: FakeIMGs{
			Img: "img",
		},
		FakeOptional: &FakeURLs{
			URL: "test2",
		},
	}

	avro, err := codec.Marshal(expected)
	require.NoError(t, err)

	var decoded FakeData
	err = codec.Unmarshal(avro, &decoded)
	require.NoError(t, err)
	require.Equal(t, expected, decoded)
}

func TestCodec_Marshal_union_and_enum(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "union-with-enum.json")

	type MyEnum string

	type UnionAndEnumEvent struct {
		MyOptionalEnum *MyEnum `avro:"my_optional_enum"`
		LongID         int     `avro:"long_id"`
		StringID       string  `avro:"string_id"`
	}

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

func TestCodec_Marshal_enum(t *testing.T) {
	codec := readAvroSchemaFromFile(t, "record-with-enum.json")

	type MyEnum string

	type EnumEvent struct {
		MyRequiredEnum MyEnum `avro:"my_required_enum"`
		LongID         int    `avro:"long_id"`
		StringID       string `avro:"string_id"`
	}

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
	codec := readAvroSchemaFromFile(t, "subrecord-with-union.json")

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
