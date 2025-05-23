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

SET(LDFLAGS)
IF(WIN32)
   IF("${MSVC_VERSION}" LESS 1910)
      SET (LDFLAGS "${LDFLAGS} -linkmode internal")
   ENDIF()
ENDIF()

IF(NOT DEFINED GOVERSION)
  MESSAGE(FATAL_ERROR "GOVERSION must be defined")
ENDIF()

SET(TAGS "")
IF(BUILD_ENTERPRISE)
  SET(TAGS "${TAGS} enterprise")
ENDIF()

MESSAGE(STATUS "Eventing version is ${EVENTING_VER}")
CONFIGURE_FILE(
  "${PROJECT_SOURCE_DIR}/util/version.in"
  "${PROJECT_SOURCE_DIR}/gen/version/version.go"
)
CONFIGURE_FILE(
  "${PROJECT_SOURCE_DIR}/v8_consumer/src/version.in"
  "${PROJECT_SOURCE_DIR}/gen/version/version.cc"
)
CONFIGURE_FILE(
  "${PROJECT_SOURCE_DIR}/ui/eventing-ui/version.in"
  "${PROJECT_SOURCE_DIR}/gen/version/version.js"
)

GoModBuild(
  TARGET convertgo
  PACKAGE github.com/couchbase/eventing/gen/convertgo
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION}
)

GoModBuild(
  TARGET convertaudit
  PACKAGE github.com/couchbase/eventing/gen/convertaudit
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION}
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/auditevent/events.go
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/convertaudit
        ARGS ../audit/audit_descriptor.json auditevent/events.go
        DEPENDS ../audit/audit_descriptor.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Coverting ../audit/audit_descriptor.json to auditevent/events.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/handler_schema.go
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/convertgo
        ARGS ../parser/handler_schema.json HandlerSchema parser/handler_schema.go
        DEPENDS ../parser/handler_schema.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Embedding ../parser/handler_schema.json to parser/handler_schema.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/depcfg_schema.go
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/convertgo
        ARGS ../parser/depcfg_schema.json DepcfgSchema parser/depcfg_schema.go
        DEPENDS ../parser/depcfg_schema.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Embedding ../parser/depcfg_schema.json to parser/depcfg_schema.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/settings_schema.go
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/convertgo
        ARGS ../parser/settings_schema.json SettingsSchema parser/settings_schema.go
        DEPENDS ../parser/settings_schema.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Embedding ../parser/settings_schema.json to parser/settings_schema.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/list_schema.go
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/convertgo
        ARGS ../parser/list_schema.json ListSchema parser/list_schema.go
        DEPENDS ../parser/list_schema.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Embedding ../parser/list_schema.json to parser/list_schema.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/parser/function_scope_schema.go
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/convertgo
        ARGS ../parser/function_scope_schema.json FunctionScopeSchema parser/function_scope_schema.go
        DEPENDS ../parser/function_scope_schema.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Embedding ../parser/function_scope_schema.json to parser/function_scope_schema.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/inspector/v8_inspector_protocol_json.h
        COMMAND ${PYTHON_EXE}
        ARGS
          ../third_party/inspector/compress_json.py
          ../third_party/inspector/js_protocol.json
          inspector/v8_inspector_protocol_json.h
        DEPENDS
          local-cbpy
          ../third_party/inspector/compress_json.py
          ../third_party/inspector/js_protocol.json
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../third_party/inspector/js_protocol.json to inspector/v8_inspector_protocol_json.h"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header_generated.h
               ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header/Header.go
        COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf header.fbs
        COMMAND ${FLATC} -g ARGS -o ../gen header.fbs
        DEPENDS ../flatbuf/header.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
        COMMENT "Converting ../flatbuf/header.fbs to flatbuf/header_generated.h and flatbuf/header/Header.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header_v2_generated.h
               ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header_v2/HeaderV2.go
        COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf header_v2.fbs
        COMMAND ${FLATC} -g ARGS -o ../gen header_v2.fbs
        DEPENDS ../flatbuf/header_v2.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
        COMMENT "Converting ../flatbuf/header_v2.fbs to flatbuf/header_v2_generated.h and flatbuf/header_v2/HeaderV2.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/payload_generated.h
               ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/payload/Payload.go
        COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf payload.fbs
        COMMAND ${FLATC} -g ARGS -o ../gen payload.fbs
        DEPENDS ../flatbuf/payload.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
        COMMENT "Converting ../flatbuf/payload.fbs to flatbuf/payload_generated.h and flatbuf/payload/Payload.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/cfg_schema_generated.h
        COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf cfg_schema.fbs
        COMMAND ${FLATC} -g ARGS -o ../gen cfg_schema.fbs
        DEPENDS ../flatbuf/cfg_schema.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
        COMMENT "Converting ../flatbuf/cfg_schema.fbs to flatbuf/cfg_schema_generated.h"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/cfg_schema_v2_generated.h
               ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/cfg_schema_v2/Cfg_schema_v2.go
        COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf cfg_schema_v2.fbs
        COMMAND ${FLATC} -g ARGS -o ../gen cfg_schema_v2.fbs
        DEPENDS ../flatbuf/cfg_schema_v2.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
        COMMENT "Converting ../flatbuf/cfg_schema_v2.fbs to flatbuf/cfg_schema_v2_generated.h and flatbuf/cfg_schema_v2/Cfg_schema_v2.go"
)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/response_generated.h
               ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/response/Response.go
        COMMAND ${FLATC} -c ARGS -o ../gen/flatbuf response.fbs
        COMMAND ${FLATC} -g ARGS -o ../gen response.fbs
        DEPENDS ../flatbuf/response.fbs
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../flatbuf
        COMMENT "Converting ../flatbuf/response.fbs to flatbuf/response_generated.h and flatbuf/response/Response.go"
)

ADD_CUSTOM_TARGET(eventing-generated DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/inspector/v8_inspector_protocol_json.h
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header_generated.h
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header/Header.go
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header_v2_generated.h
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/header_v2/HeaderV2.go
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/payload_generated.h
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/payload/Payload.go
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/cfg_schema_v2_generated.h
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/cfg_schema_v2/Cfg_schema_v2.go
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/response_generated.h
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/response/Response.go
        ${CMAKE_CURRENT_SOURCE_DIR}/flatbuf/cfg_schema_generated.h
        ${CMAKE_CURRENT_SOURCE_DIR}/auditevent/events.go
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/handler_schema.go
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/depcfg_schema.go
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/settings_schema.go
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/list_schema.go
        ${CMAKE_CURRENT_SOURCE_DIR}/parser/function_scope_schema.go
        ${CMAKE_CURRENT_SOURCE_DIR}/version/version.go
        ${CMAKE_CURRENT_SOURCE_DIR}/version/version.cc
)

ADD_DEPENDENCIES(eventing-generated convertgo convertaudit)

AddMetricsMetadata (JSON ../parser/metrics_metadata.json COMPONENT eventing)
