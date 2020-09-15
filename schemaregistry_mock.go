package avro

import "fmt"

// NewNOOPCodecRegistry returns a CodecRegistry that uses the NOOP
// schema registry
func NewNOOPCodecRegistry(subject string) *CodecRegistry {
	return &CodecRegistry{
		codecByID: make(map[SchemaID]*Codec),
		Registry:  NewNOOPClient(),
		subject:   subject,
	}
}

// Retrieved from https://github.com/Landoop/schema-registry

type mockSchemaRegistry struct {
	SubjectsFn           func() (subjects []string, err error)
	VersionsFn           func(subject string) (versions []int, err error)
	RegisterNewSchemaFn  func(subject, schema string) (int, error)
	IsRegisteredFn       func(subject, schema string) (bool, Schema, error)
	GetSchemaByIDFn      func(id int) (string, error)
	GetSchemaBySubjectFn func(subject string, ver int) (Schema, error)
	GetLatestSchemaFn    func(subject string) (Schema, error)
	DeleteSubjectFn      func(subject string) (versions []int, err error)
}

func (c *mockSchemaRegistry) Subjects() (subjects []string, err error) {
	return c.SubjectsFn()
}

func (c *mockSchemaRegistry) Versions(subject string) (versions []int, err error) {
	return c.VersionsFn(subject)
}

func (c *mockSchemaRegistry) RegisterNewSchema(subject, schema string) (int, error) {
	return c.RegisterNewSchemaFn(subject, schema)
}

func (c *mockSchemaRegistry) IsRegistered(subject, schema string) (bool, Schema, error) {
	return c.IsRegisteredFn(subject, schema)
}

func (c *mockSchemaRegistry) GetSchemaByID(id int) (string, error) {
	return c.GetSchemaByIDFn(id)
}

func (c *mockSchemaRegistry) GetSchemaBySubject(subject string, ver int) (Schema, error) {
	return c.GetSchemaBySubjectFn(subject, ver)
}

func (c *mockSchemaRegistry) GetLatestSchema(subject string) (Schema, error) {
	return c.GetLatestSchemaFn(subject)
}

func (c *mockSchemaRegistry) DeleteSubject(subject string) ([]int, error) {
	return c.DeleteSubjectFn(subject)
}

// NewNOOPClient is a mock schema registry which can be used for testing purposes
// nolint
func NewNOOPClient() SchemaRegistry {
	var newID int
	ptrNewID := &newID
	store := make(map[string][]Schema)
	return &mockSchemaRegistry{
		SubjectsFn: func() (subjects []string, err error) {
			var keys []string
			for key := range store {
				keys = append(keys, key)
			}
			return keys, nil
		},
		VersionsFn: func(subject string) (versions []int, err error) {
			for _, schemas := range store {
				for i := range schemas {
					versions = append(versions, i)
				}
			}
			return versions, nil
		},
		RegisterNewSchemaFn: func(subject, rawSchema string) (int, error) {
			var schema Schema
			schema.Schema = rawSchema
			schema.Subject = subject
			schemas, ok := store[subject]
			if !ok {
				schemas = []Schema{}
			}
			schema.ID = *ptrNewID
			*ptrNewID++
			schema.Version = len(schemas)
			schemas = append(schemas, schema)

			store[subject] = schemas

			return schema.ID, nil
		},
		IsRegisteredFn: func(subject, schema string) (bool, Schema, error) {
			schemas, ok := store[subject]
			if !ok {
				return false, Schema{}, nil
			}
			for _, s := range schemas {
				if schema == s.Schema {
					return true, s, nil
				}
			}
			return false, Schema{}, nil
		},
		GetSchemaByIDFn: func(id int) (string, error) {
			for _, schemas := range store {
				for _, schema := range schemas {
					if schema.ID == id {
						return schema.Schema, nil
					}
				}
			}
			return "", fmt.Errorf("schema not found")
		},
		GetSchemaBySubjectFn: func(subject string, ver int) (Schema, error) {
			return Schema{}, nil
		},
		GetLatestSchemaFn: func(subject string) (Schema, error) {
			schemas, ok := store[subject]
			if !ok {
				return Schema{}, fmt.Errorf("subject not found")
			}
			if len(schemas) == 0 {
				return Schema{}, fmt.Errorf("no schema found in this subject")
			}
			return schemas[len(schemas)-1], nil
		},
		DeleteSubjectFn: func(subject string) (versions []int, err error) {
			for _, schemas := range store {
				for i := range schemas {
					versions = append(versions, i)
				}
			}
			delete(store, subject)
			return versions, nil
		},
	}
}
