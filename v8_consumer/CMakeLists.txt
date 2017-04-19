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

INCLUDE_DIRECTORIES(BEFORE ${LIBCOUCHBASE_INCLUDE_DIR} ${LIBUV_INCLUDE_DIR})

FIND_PACKAGE(FLEX REQUIRED)

INCLUDE_DIRECTORIES(AFTER  ${ICU_INCLUDE_DIR}
                           ${V8_INCLUDE_DIR}
                           ${CURL_INCLUDE_DIR}
                           ${CMAKE_CURRENT_BINARY_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/include
                           ${FLATBUFFERS_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${CMAKE_INSTALL_PREFIX}/include)

FLEX_TARGET(jsify src/jsify.lex ${CMAKE_CURRENT_BINARY_DIR}/jsify.cc)

SET(EVENTING_SOURCES src/client.cc src/commands.cc
    src/message.cc src/v8worker.cc src/assert.cc
    src/bucket.cc src/n1ql.cc src/parse_deployment.cc
    src/log.cc ${CMAKE_CURRENT_BINARY_DIR}/jsify.cc src/transpiler.cc)

SET(EVENTING_LIBRARIES ${V8_LIBRARIES} ${ICU_LIBRARIES} -ll ${JEMALLOC_LIBRARIES} ${CURL_LIBRARIES} ${LIBCOUCHBASE_LIBRARIES} ${LIBUV_LIBRARIES} platform)
ADD_EXECUTABLE(client.bin ${EVENTING_SOURCES})
TARGET_LINK_LIBRARIES(client.bin ${EVENTING_LIBRARIES})
INSTALL(TARGETS client.bin
        RUNTIME DESTINATION bin)
FILE(COPY src/transpiler.js DESTINATION ${CMAKE_SOURCE_DIR}/ns_server/)
FILE(COPY src/transpiler.js DESTINATION ${CMAKE_INSTALL_PREFIX}/var/lib/couchbase)
FILE(COPY src/builtin.js DESTINATION ${CMAKE_SOURCE_DIR}/ns_server/)
FILE(COPY src/builtin.js DESTINATION ${CMAKE_INSTALL_PREFIX}/var/lib/couchbase)
FILE(COPY third_party/esprima.js DESTINATION ${CMAKE_SOURCE_DIR}/ns_server/)
FILE(COPY third_party/esprima.js DESTINATION ${CMAKE_INSTALL_PREFIX}/var/lib/couchbase)
FILE(COPY third_party/escodegen.js DESTINATION ${CMAKE_SOURCE_DIR}/ns_server/)
FILE(COPY third_party/escodegen.js DESTINATION ${CMAKE_INSTALL_PREFIX}/var/lib/couchbase)
FILE(COPY third_party/estraverse.js DESTINATION ${CMAKE_SOURCE_DIR}/ns_server/)
FILE(COPY third_party/estraverse.js DESTINATION ${CMAKE_INSTALL_PREFIX}/var/lib/couchbase)
