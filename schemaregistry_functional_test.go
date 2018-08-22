package avro

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
)

var SchemaRegistryURL string

func WipeRegistry() {
	c, err := NewSchemaRegistry(SchemaRegistryURL)
	if err != nil {
		panic(fmt.Sprintf("unable to teardown: %s", err))
	}

	subjects, err := c.Subjects()
	if err != nil {
		panic(fmt.Sprintf("unable to teardown: %s", err))
	}

	for _, subject := range subjects {
		_, err := c.DeleteSubject(subject)
		if err != nil {
			fmt.Printf("unable to delete subject `%s`: %s", subject, err)
		}
	}
}

func stripSpaces(s string) string {
	f := func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}
	return strings.Map(f, s)
}

func Setup() {
	SchemaRegistryURL = os.Getenv("SCHEMA_REGISTRY_URL")
	if SchemaRegistryURL == "" {
		SchemaRegistryURL = "http://schemaregistry:8081"
	}
}

func TearDown() {
	if os.Getenv("GO_INTEGRATION_TESTS") != "1" {
		return
	}

	WipeRegistry()
}

func TestMain(m *testing.M) {
	Setup()
	ret := m.Run()
	TearDown()
	os.Exit(ret)
}

func containsValueString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func containsSliceString(a []string, b []string) bool {
	for _, v := range a {
		if !containsValueString(b, v) {
			return false
		}
	}
	return true
}

func TestFunctionalSubjects(t *testing.T) {
	if os.Getenv("GO_INTEGRATION_TESTS") != "1" {
		return
	}

	schema := `
	{
	    "type" : "record",
	    "name" : "userInfo",
	    "namespace" : "my.example",
	    "fields" : [{"name" : "age", "type" : "int", "default" : -1}]
	}`

	c, err := NewSchemaRegistry(SchemaRegistryURL)
	assert.NoError(t, err)

	subsIn := []string{"rollulus", "hello-subject"}
	for _, s := range subsIn {
		_, err = c.RegisterNewSchema(s, schema)
		assert.NoError(t, err)
	}

	subs, err := c.Subjects()
	assert.NoError(t, err)

	sort.Strings(subs)
	sort.Strings(subsIn)
	assert.True(t, containsSliceString(subsIn, subs))

	WipeRegistry()
}

func TestFunctionalVersions(t *testing.T) {
	if os.Getenv("GO_INTEGRATION_TESTS") != "1" {
		return
	}

	schemas := []string{`
	{
	    "type" : "record",
	    "name" : "userInfo",
	    "namespace" : "my.example",
	    "fields" : [{"name" : "age", "type" : "int"}]
	}`, `
	{
	    "type" : "record",
	    "name" : "userInfo",
	    "namespace" : "my.example",
	    "fields" : [{"name" : "age", "type" : "int", "default" : -1}]
	}`, `
	{
	    "type" : "record",
	    "name" : "userInfo",
	    "namespace" : "my.example",
	    "fields" : [
	        {"name": "age", "type": "int", "default": -1},
		{"name": "city", "type": "string", "default": ""}
	    ]
	}`,
	}

	c, err := NewSchemaRegistry(SchemaRegistryURL)
	assert.NoError(t, err)

	subject := "mysubject"

	for _, schema := range schemas {
		_, err = c.RegisterNewSchema(subject, schema)
		assert.NoError(t, err)
	}

	versions, err := c.Versions(subject)
	assert.NoError(t, err)
	assert.Equal(t, len(schemas), len(versions))

	WipeRegistry()
}

func TestFunctionalIsRegistered_yes(t *testing.T) {
	if os.Getenv("GO_INTEGRATION_TESTS") != "1" {
		return
	}

	schema := `
	{
	    "type" : "record",
	    "name" : "userInfo",
	    "namespace" : "my.example",
	    "fields" : [{"name" : "age", "type" : "int", "default" : -1}]
	}`
	schema = stripSpaces(schema)

	subject := "newsubject"

	sIn := Schema{schema, subject, 1, 7}

	c, err := NewSchemaRegistry(SchemaRegistryURL)
	assert.NoError(t, err)

	id, err := c.RegisterNewSchema(subject, schema)
	assert.NoError(t, err)
	sIn.ID = id

	isreg, sOut, err := c.IsRegistered(subject, schema)
	assert.NoError(t, err)
	assert.True(t, isreg)
	// We cannot and do not want to guarantee the version
	assert.Equal(t, sIn.Schema, sOut.Schema)
	assert.Equal(t, sIn.Subject, sOut.Subject)
	assert.Equal(t, sIn.ID, sOut.ID)

	WipeRegistry()
}

func TestFunctionalIsRegistered_not(t *testing.T) {
	if os.Getenv("GO_INTEGRATION_TESTS") != "1" {
		return
	}

	c, err := NewSchemaRegistry(SchemaRegistryURL)
	assert.NoError(t, err)

	isreg, _, err := c.IsRegistered("mysubject", "{}")
	assert.NoError(t, err)
	assert.False(t, isreg)

	WipeRegistry()
}
