package parser

import (
	"bytes"
	"strings"

	"github.com/couchbase/eventing/gen/parser"
	"github.com/couchbase/eventing/logging"
	"github.com/santhosh-tekuri/jsonschema"
)

const (
	LIST_SCHEMA     = "list_schema.json"
	HANDLER_SCHEMA  = "handler_schema.json"
	SETTINGS_SCHEMA = "settings_schema.json"
	DEPCFG_SCHEMA   = "depcfg_schema.json"
	FUNCTION_SCOPE_SCHEMA = "function_scope_schema.json"
)

var handlerSchema, settingsSchema, listSchema *jsonschema.Schema
var compiler *jsonschema.Compiler

func InitCompiler() {
	compiler = jsonschema.NewCompiler()
	addResource(LIST_SCHEMA, parser.ListSchema)
	addResource(HANDLER_SCHEMA, parser.HandlerSchema)
	addResource(SETTINGS_SCHEMA, parser.SettingsSchema)
	addResource(DEPCFG_SCHEMA, parser.DepcfgSchema)
	addResource(FUNCTION_SCOPE_SCHEMA, parser.FunctionScopeSchema)
	listSchema = compiler.MustCompile(LIST_SCHEMA)
	handlerSchema = compiler.MustCompile(HANDLER_SCHEMA)
	settingsSchema = compiler.MustCompile(SETTINGS_SCHEMA)
}

func addResource(name, schema string) {
	err := compiler.AddResource(name, strings.NewReader(schema))
	if err != nil {
		logging.Errorf("Error adding %s schema to the compiler.", name)
	}
}

func ValidateHandlerSchema(handler []byte) error {
	if compiler == nil {
		InitCompiler()
	}
	reader := bytes.NewReader(handler)
	err := handlerSchema.Validate(reader)
	return err
}

func ValidateSettingsSchema(settings []byte) error {
	if compiler == nil {
		InitCompiler()
	}
	reader := bytes.NewReader(settings)
	err := settingsSchema.Validate(reader)
	return err
}

func ValidateHandlerListSchema(list []byte) error {
	if compiler == nil {
		InitCompiler()
	}
	reader := bytes.NewReader(list)
	err := listSchema.Validate(reader)
	return err
}
