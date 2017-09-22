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
GET_GOROOT ("1.8.3" GOROOT _ver)
SET (ENV{GOROOT} "${GOROOT}")

ADD_CUSTOM_COMMAND(
        OUTPUT builtin.h
        COMMAND ${GOROOT}/bin/go
        ARGS run generate.go ../src/builtin.js js_builtin builtin.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../src/builtin.js to builtin.h with variable js_builtin"
)
ADD_CUSTOM_COMMAND(
        OUTPUT transpiler.h
        COMMAND ${GOROOT}/bin/go
        ARGS run generate.go ../src/transpiler.js js_transpiler transpiler.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../src/transpiler.js to transpiler.h with variable js_transpiler"
)
ADD_CUSTOM_COMMAND(
        OUTPUT esprima.h
        COMMAND ${GOROOT}/bin/go
        ARGS run generate.go ../../third_party/estools/esprima.js js_esprima esprima.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../../third_party/estools/esprima.js to esprima.h with variable js_esprima"
)
ADD_CUSTOM_COMMAND(
        OUTPUT escodegen.h
        COMMAND ${GOROOT}/bin/go
        ARGS run generate.go ../../third_party/estools/escodegen.js js_escodegen escodegen.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../../third_party/estools/escodegen.js to escodegen.h with variable js_escodegen"
)
ADD_CUSTOM_COMMAND(
        OUTPUT estraverse.h
        COMMAND ${GOROOT}/bin/go
        ARGS run generate.go ../../third_party/estools/estraverse.js js_estraverse estraverse.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../../third_party/estools/estraverse.js to estraverse.h with variable js_estraverse"
)
ADD_CUSTOM_COMMAND(
        OUTPUT source-map.h
        COMMAND ${GOROOT}/bin/go
        ARGS run generate.go ../../third_party/source-map/source-map.js js_source_map source-map.h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Converting ../../third_party/source-map/source-map.js to source-map.h with variable js_source_map"
)

ADD_CUSTOM_TARGET(generated ALL DEPENDS
  builtin.h transpiler.h esprima.h
  escodegen.h estraverse.h source-map.h
)

