# Copyright (c) 2017 Couchbase, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS"
# BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.

INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseFlatbuffers)

GoInstall(
  TARGET convertjs
  PACKAGE github.com/couchbase/eventing/gen/convertjs
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.."
  GOVERSION "1.8.3"
)

ADD_CUSTOM_COMMAND(
        OUTPUT js/builtin.h
        COMMAND ${PROJECT_SOURCE_DIR}/../../../../bin/convertjs
        ARGS ../v8_consumer/src/builtin.js js_builtin js/builtin.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../v8_consumer/src/builtin.js to js/builtin.h with variable js_builtin"
)
ADD_CUSTOM_COMMAND(
        OUTPUT js/transpiler.h
        COMMAND ${PROJECT_SOURCE_DIR}/../../../../bin/convertjs
        ARGS ../v8_consumer/src/transpiler.js js_transpiler js/transpiler.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../v8_consumer/src/transpiler.js to js/transpiler.h with variable js_transpiler"
)
ADD_CUSTOM_COMMAND(
        OUTPUT js/esprima.h
        COMMAND ${PROJECT_SOURCE_DIR}/../../../../bin/convertjs
        ARGS ../third_party/estools/esprima.js js_esprima js/esprima.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../third_party/estools/esprima.js to js/esprima.h with variable js_esprima"
)
ADD_CUSTOM_COMMAND(
        OUTPUT js/escodegen.h
        COMMAND ${PROJECT_SOURCE_DIR}/../../../../bin/convertjs
        ARGS ../third_party/estools/escodegen.js js_escodegen js/escodegen.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../third_party/estools/escodegen.js to js/escodegen.h with variable js_escodegen"
)
ADD_CUSTOM_COMMAND(
        OUTPUT js/estraverse.h
        COMMAND ${PROJECT_SOURCE_DIR}/../../../../bin/convertjs
        ARGS ../third_party/estools/estraverse.js js_estraverse js/estraverse.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../third_party/estools/estraverse.js to js/estraverse.h with variable js_estraverse"
)
ADD_CUSTOM_COMMAND(
        OUTPUT js/source-map.h
        COMMAND ${PROJECT_SOURCE_DIR}/../../../../bin/convertjs
        ARGS ../third_party/source-map/source-map.js js_source_map js/source-map.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../third_party/source-map/source-map.js to js/source-map.h with variable js_source_map"
)

ADD_CUSTOM_COMMAND(
        OUTPUT inspector/v8_inspector_protocol_json.h
        COMMAND python
	ARGS 
	  ../third_party/inspector/compress_json.py
	  ../third_party/inspector/js_protocol.json
          inspector/v8_inspector_protocol_json.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../third_party/inspector/js_protocol.json to inspector/v8_inspector_protocol_json.h"
)

ADD_CUSTOM_COMMAND(
	OUTPUT flatbuf/header_generated.h flatbuf/header/Header.go
	COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf header.fbs
	COMMAND ${FLATC} -g ARGS -o ../gen header.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
	COMMENT "Converting ../flatbuf/header.fbs to flatbuf/header_generated.h and flatbuf/header/Header.go"
)
ADD_CUSTOM_COMMAND(
	OUTPUT flatbuf/payload_generated.h flatbuf/payload/Payload.go
	COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf payload.fbs
	COMMAND ${FLATC} -g ARGS -o ../gen payload.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
	COMMENT "Converting ../flatbuf/payload.fbs to flatbuf/payload_generated.h and flatbuf/payload/Payload.go"
)
ADD_CUSTOM_COMMAND(
	OUTPUT flatbuf/cfg_schema_generated.h flatbuf/cfg_schema/Cfg_schema.go
	COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf cfg_schema.fbs
	COMMAND ${FLATC} -g ARGS -o ../gen cfg_schema.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
	COMMENT "Converting ../flatbuf/cfg_schema.fbs to flatbuf/cfg_schema_generated.h and flatbuf/cfg_schema/Cfg_schema.go"
)
ADD_CUSTOM_COMMAND(
	OUTPUT flatbuf/response_generated.h flatbuf/response/Response.go
	COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf response.fbs
	COMMAND ${FLATC} -g ARGS -o ../gen response.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
	COMMENT "Converting ../flatbuf/response.fbs to flatbuf/response_generated.h and flatbuf/response/Response.go"
)

ADD_CUSTOM_TARGET(generated DEPENDS
        js/builtin.h js/transpiler.h js/esprima.h js/escodegen.h js/estraverse.h js/source-map.h
        inspector/v8_inspector_protocol_json.h
	flatbuf/header_generated.h flatbuf/header/Header.go
	flatbuf/payload_generated.h flatbuf/payload/Payload.go
	flatbuf/response_generated.h flatbuf/response/Response.go
	flatbuf/cfg_schema_generated.h flatbuf/cfg_schema/Cfg_schema.go
)

ADD_DEPENDENCIES(generated convertjs)
