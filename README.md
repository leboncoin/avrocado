[![Build Status](https://travis-ci.org/leboncoin/avrocado.svg?branch=master)](https://travis-ci.org/leboncoin/avrocado)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/leboncoin/avrocado)](https://pkg.go.dev/github.com/leboncoin/avrocado)

# Avrocado

Avrocado is a convenience library to handle Avro in golang, built on top of [linkedin/goavro](https://github.com/linkedin/goavro).
It is split into three parts:
* Avro marshalling/unmarshalling using structure fields annotations inspired by the JSON standard library.
* A [confluentinc/schema-registry](https://github.com/confluentinc/schema-registry) client.
* A codec registry which handles marshalling/unmarshalling schemas from the schema-registry.

## Getting Started

You can start using the library after installing by importing it in your go code.
You need to annotate the types you want to marshal with the avro tag.
Finally you will have to instantiate a codec with the corresponding Avro schema:

```
import "github.com/leboncoin/avrocado"

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

```

The example can also be found [here](example_test.go).

## Installing

Just run `go get github.com/leboncoin/avrocado`.

## Examples

See the test files for examples on how to use the library.

## Running tests
Just run `go test` at the root directory of this repository.
