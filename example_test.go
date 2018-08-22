package avro

import (
	"fmt"
)

type Someone struct {
	Name string `avro:"name"`
	Age  int32  `avro:"age"`
}

func ExampleCodec() {
	val := Someone{"MyName", 3}
	var decoded Someone

	schema := `{
	  "type": "record",
	  "name": "Someone",
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

	codec, err := NewCodec(schema)
	if err != nil {
		panic(fmt.Sprintf("wrong schema: %s", err))
	}

	avro, err := codec.Marshal(&val)
	if err != nil {
		panic(fmt.Sprintf("unable to serialize to avro: %s", err))
	}

	err = codec.Unmarshal(avro, &decoded)
	if err != nil {
		panic(fmt.Sprintf("unable to deserialize from avro: %s", err))
	}
}
