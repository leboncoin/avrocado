package avro

// Retrieved from https://github.com/Landoop/schema-registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
)

// DefaultURL is the address where a local schema registry listens by default.
var DefaultURL = "http://localhost:8081"

// These numbers are used by the schema registry to communicate errors.
const (
	schemaNotFound  = 40403
	subjectNotFound = 40401
)

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	ID      int    `json:"id"`      // Registry's unique id
}

type simpleSchema struct {
	Schema string `json:"schema"`
}

// A ConfluentError is an error as communicated by the schema registry.
// Some day this type might be exposed so that callers can do type assertions on it.
type confluentError struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// Error makes confluentError implement the error interface.
func (ce confluentError) Error() string {
	return fmt.Sprintf("%s (%d)", ce.Message, ce.ErrorCode)
}

type httpDoer interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

// SchemaRegistry is a client for the schema registry.
type SchemaRegistry interface {
	Subjects() (subjects []string, err error)
	Versions(subject string) (versions []int, err error)
	RegisterNewSchema(subject, schema string) (int, error)
	IsRegistered(subject, schema string) (bool, Schema, error)
	GetSchemaByID(id int) (string, error)
	GetSchemaBySubject(subject string, ver int) (s Schema, err error)
	GetLatestSchema(subject string) (s Schema, err error)
	DeleteSubject(subject string) (versions []int, err error)
}

// ConfluentSchemaRegistry defines a schema registry managed by Confluent
type ConfluentSchemaRegistry struct {
	url    url.URL
	client httpDoer
}

func parseSchemaRegistryError(resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ioutil.ReadAll error while reading error body: %w", err)
	}

	var ce confluentError
	if err := json.Unmarshal(body, &ce); err != nil {
		return fmt.Errorf("json.Unmarshal error while reading error body: %q: %w", body, err)
	}
	return ce
}

// do performs http requests and json (de)serialization.
func (c *ConfluentSchemaRegistry) do(method, urlPath string, in interface{}, out interface{}) error {
	u := c.url
	u.Path = path.Join(u.Path, urlPath)
	var rdp io.Reader
	if in != nil {
		body, err := json.Marshal(in)
		if err != nil {
			return err
		}
		rdp = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u.String(), rdp)
	if err != nil {
		return fmt.Errorf("http.NewRequest error: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("ConfluentSchemaRegistry.Do error; %w", err)
	}
	defer func() {
		if resp.Body != nil {
			_, _ = io.Copy(ioutil.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseSchemaRegistryError(resp)
	}
	err = json.NewDecoder(resp.Body).Decode(out)
	if err != nil {
		return fmt.Errorf("json.Decode error: %w", err)
	}

	return nil
}

// Subjects returns all registered subjects.
func (c *ConfluentSchemaRegistry) Subjects() (subjects []string, err error) {
	err = c.do("GET", "subjects", nil, &subjects)
	return
}

// Versions returns all schema version numbers registered for this subject.
func (c *ConfluentSchemaRegistry) Versions(subject string) (versions []int, err error) {
	err = c.do("GET", fmt.Sprintf("subjects/%s/versions", subject), nil, &versions)
	return
}

// RegisterNewSchema registers the given schema for this subject.
func (c *ConfluentSchemaRegistry) RegisterNewSchema(subject, schema string) (int, error) {
	var resp struct {
		ID int `json:"id"`
	}
	err := c.do("POST", fmt.Sprintf("/subjects/%s/versions", subject), simpleSchema{schema}, &resp)
	return resp.ID, err
}

// IsRegistered tells if the given schema is registred for this subject.
func (c *ConfluentSchemaRegistry) IsRegistered(subject, schema string) (bool, Schema, error) {
	var fs Schema
	err := c.do("POST", fmt.Sprintf("/subjects/%s", subject), simpleSchema{schema}, &fs)
	// subject not found?
	if ce, confluentErr := err.(confluentError); confluentErr && ce.ErrorCode == subjectNotFound {
		return false, fs, nil
	}
	// schema not found?
	if ce, confluentErr := err.(confluentError); confluentErr && ce.ErrorCode == schemaNotFound {
		return false, fs, nil
	}
	// error?
	if err != nil {
		return false, fs, err
	}
	// so we have a schema then
	return true, fs, nil
}

// GetSchemaByID returns the schema for some id.
// The schema registry only provides the schema itself, not the id, subject or version.
func (c *ConfluentSchemaRegistry) GetSchemaByID(id int) (string, error) {
	var s Schema
	err := c.do("GET", fmt.Sprintf("/schemas/ids/%d", id), nil, &s)
	return s.Schema, err
}

// GetSchemaBySubject returns the schema for a particular subject and version.
func (c *ConfluentSchemaRegistry) GetSchemaBySubject(subject string, ver int) (s Schema, err error) {
	err = c.do("GET", fmt.Sprintf("/subjects/%s/versions/%d", subject, ver), nil, &s)
	return
}

// GetLatestSchema returns the latest version of the subject's schema.
func (c *ConfluentSchemaRegistry) GetLatestSchema(subject string) (s Schema, err error) {
	err = c.do("GET", fmt.Sprintf("/subjects/%s/versions/latest", subject), nil, &s)
	return
}

// DeleteSubject removes a list of schema under the given subject
func (c *ConfluentSchemaRegistry) DeleteSubject(subject string) (versions []int, err error) {
	err = c.do("DELETE", fmt.Sprintf("/subjects/%s", subject), nil, &versions)
	return
}

// NewSchemaRegistry returns a new SchemaRegistry that connects to baseurl.
func NewSchemaRegistry(baseurl string) (SchemaRegistry, error) {
	u, err := url.Parse(baseurl)
	if err != nil {
		return nil, err
	}
	return &ConfluentSchemaRegistry{*u, http.DefaultClient}, nil
}
