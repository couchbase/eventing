package eventing

import (
	"bytes"
	"strings"

	"github.com/couchbase/eventing/gen/parser"
	"github.com/santhosh-tekuri/jsonschema"
)

var handlerSchema, settingsSchema, listSchema *jsonschema.Schema

func init() {
	compiler := jsonschema.NewCompiler()
	addResource(compiler, "listSchema", parser.ListSchema)
	addResource(compiler, "handlerSchema", parser.HandlerSchema)
	addResource(compiler, "settingsSchema", parser.SettingsSchema)
	addResource(compiler, "depcfgSchema", parser.DepcfgSchema)
	listSchema = compiler.MustCompile("listSchema")
	handlerSchema = compiler.MustCompile("handlerSchema")
	settingsSchema = compiler.MustCompile("settingsSchema")
}

func addResource(compiler *jsonschema.Compiler, name, schema string) {
	err := compiler.AddResource(name, strings.NewReader(schema))
	if err != nil {
		panic(err)
	}
}

func ValidateHandlerSchema(handler []byte) error {
	reader := bytes.NewReader(handler)
	err := handlerSchema.Validate(reader)
	return err
}

func ValidateSettingsSchema(settings []byte) error {
	reader := bytes.NewReader(settings)
	err := settingsSchema.Validate(reader)
	return err
}

func ValidateHandlerListSchema(list []byte) error {
	reader := bytes.NewReader(list)
	err := listSchema.Validate(reader)
	return err
}
