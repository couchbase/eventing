package eventing

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/couchbase/eventing/gen/parser"
	"github.com/santhosh-tekuri/jsonschema"
)

var handlerSchema, settingsSchema, listSchema *jsonschema.Schema

func init() {
	compiler := jsonschema.NewCompiler()
	addResource(compiler, "list_schema.json", parser.ListSchema)
	addResource(compiler, "handler_schema.json", parser.HandlerSchema)
	addResource(compiler, "settings_schema.json", parser.SettingsSchema)
	addResource(compiler, "depcfg_schema.json", parser.DepcfgSchema)
	listSchema = compiler.MustCompile("list_schema.json")
	handlerSchema = compiler.MustCompile("handler_schema.json")
	settingsSchema = compiler.MustCompile("settings_schema.json")
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
	if err != nil {
		return fmt.Errorf("Validation error: %v\nWhile validating JSON: %s\n", err, handler)
	}
	return nil
}

func ValidateSettingsSchema(settings []byte) error {
	reader := bytes.NewReader(settings)
	err := settingsSchema.Validate(reader)
	if err != nil {
		return fmt.Errorf("Validation error: %v\nWhile validating JSON: %s\n", err, settings)
	}
	return nil
}

func ValidateHandlerListSchema(list []byte) error {
	reader := bytes.NewReader(list)
	err := listSchema.Validate(reader)
	if err != nil {
		return fmt.Errorf("Validation error: %v\nWhile validating JSON: %s\n", err, list)
	}
	return nil
}
