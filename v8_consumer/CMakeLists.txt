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

PROJECT (eventing-consumer)

INCLUDE (FindCouchbaseJemalloc)
INCLUDE (FindCouchbaseLibuv)
INCLUDE (FindCouchbaseLibCouchbase)

INCLUDE_DIRECTORIES(BEFORE ${LIBCOUCHBASE_INCLUDE_DIR} ${LIBUV_INCLUDE_DIR})

INCLUDE_DIRECTORIES(AFTER  ${ICU_INCLUDE_DIR}
                           ${V8_INCLUDE_DIR}
                           ${CURL_INCLUDE_DIR}
                           ${CMAKE_CURRENT_BINARY_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/include
                           ${FLATBUFFERS_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${CMAKE_INSTALL_PREFIX}/include)



SET(SSE42_COMPILE_FLAG "-msse4.2")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SSE42_COMPILE_FLAG}")

SET(EVENTING_SOURCES
    src/client.cc src/commands.cc src/message.cc
    src/v8worker.cc src/assert.cc src/bucket.cc
    src/n1ql.cc src/parse_deployment.cc src/crc32c.cc
    src/log.cc src/transpiler.cc src/js_exception.cc
    src/utils.cc src/function_templates.cc)

SET(EVENTING_LIBRARIES
      ${V8_LIBRARIES} ${ICU_LIBRARIES} ${JEMALLOC_LIBRARIES}
      ${CURL_LIBRARIES} ${LIBCOUCHBASE_LIBRARIES} ${LIBUV_LIBRARIES}
      platform)

# Below is temporary. Flex will be required
FIND_PACKAGE(FLEX)
IF(FLEX_FOUND)
  ADD_CUSTOM_COMMAND(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/../gen/jsify.cc
    COMMAND ${FLEX_EXECUTABLE}
    ARGS -B -o../gen/jsify.cc src/jsify.lex
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/src/jsify.lex
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Converting src/jsify.lex to ../gen/jsify.cc"
  )
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DFLEX_FOUND -DYY_NEVER_INTERACTIVE")
  SET(EVENTING_SOURCES ${EVENTING_SOURCES} ${CMAKE_CURRENT_SOURCE_DIR}/../gen/jsify.cc)
  IF(APPLE)
    SET(EVENTING_LIBRARIES ${EVENTING_LIBRARIES} "-ll")
  ENDIF()
ENDIF()

# Below is temporary. Zlib will be required
FIND_PACKAGE(ZLIB)
IF(ZLIB_FOUND)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DZLIB_FOUND")
  INCLUDE_DIRECTORIES(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/inspector)
  SET(EVENTING_LIBRARIES ${EVENTING_LIBRARIES} eventing-inspector)
ENDIF()

ADD_EXECUTABLE(eventing-consumer ${EVENTING_SOURCES})
ADD_DEPENDENCIES(eventing-consumer generated)

TARGET_LINK_LIBRARIES(eventing-consumer ${EVENTING_LIBRARIES})
INSTALL(TARGETS eventing-consumer RUNTIME DESTINATION bin)
