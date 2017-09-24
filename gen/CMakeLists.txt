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
GET_GOROOT ("1.8.3" GOROOT _ver)

MACRO(convertjs src var out)
  IF(NOT WIN32)
    ADD_CUSTOM_COMMAND(
      OUTPUT ${out}
      COMMAND GOROOT=${GOROOT} ${GOROOT}/bin/go
      ARGS run generate.go ${src} ${var} ${out}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Converting ${src} to ${out} with variable ${var}")
   ELSE()
    ADD_CUSTOM_COMMAND(
      OUTPUT ${out}
      COMMAND set GOROOT=${GOROOT} & ${GOROOT}/bin/go
      ARGS run generate.go ${src} ${var} ${out}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Converting ${src} to ${out} with variable ${var}")
   ENDIF()
ENDMACRO()

MACRO(flatbuf src outc outg)
  ADD_CUSTOM_COMMAND(
    OUTPUT ${outc} ${outg}
    COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf ${src}
    COMMAND ${FLATC} -g ARGS -o ../gen ${src}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
    COMMENT "Converting ${src} to {$outc} and ${outg}")
ENDMACRO()

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

convertjs(../v8_consumer/src/builtin.js js_builtin js/builtin.h)
convertjs(../v8_consumer/src/transpiler.js js_transpiler js/transpiler.h)
convertjs(../third_party/estools/esprima.js js_esprima js/esprima.h)
convertjs(../third_party/estools/escodegen.js js_escodegen js/escodegen.h)
convertjs(../third_party/estools/estraverse.js js_estraverse js/estraverse.h)
convertjs(../third_party/source-map/source-map.js js_source_map js/source-map.h)

flatbuf(header.fbs flatbuf/header_generated.h flatbuf/header/Header.go)
flatbuf(payload.fbs flatbuf/payload_generated.h flatbuf/payload/Payload.go)
flatbuf(cfg_schema.fbs flatbuf/cfg_schema_generated.h flatbuf/cfg_schema/Cfg_schema.go)
flatbuf(response.fbs flatbuf/response_generated.h flatbuf/response/Response.go)

ADD_CUSTOM_TARGET(generated DEPENDS
        js/builtin.h js/transpiler.h js/esprima.h js/escodegen.h js/estraverse.h js/source-map.h
        inspector/v8_inspector_protocol_json.h
	flatbuf/header_generated.h flatbuf/header/Header.go
	flatbuf/payload_generated.h flatbuf/payload/Payload.go
	flatbuf/response_generated.h flatbuf/response/Response.go
	flatbuf/cfg_schema_generated.h flatbuf/cfg_schema/Cfg_schema.go
)

