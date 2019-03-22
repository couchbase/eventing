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

PROJECT(eventing-consumer)

INCLUDE (FindCouchbaseLibuv)
INCLUDE (FindCouchbaseV8)
INCLUDE (FindCouchbaseBreakpad)

SET(LIBS_HEADERS
        ../libs/include
        ../third_party/crc64)

SET(LIBS_SRC
        ../../eventing-ee/curl.cc
        ../libs/src/bucket.cc
        ../libs/src/comm.cc
        ../libs/src/n1ql.cc
        ../libs/src/log.cc
        ../libs/src/transpiler.cc
        ../libs/src/js_exception.cc
        ../libs/src/v8log.cc
        ../libs/src/utils.cc
        ../libs/src/base64.cc
        ../third_party/crc64/crc64.cc)

INCLUDE_DIRECTORIES(BEFORE
                     "${libcouchbase_SOURCE_DIR}/include"
                     "${libcouchbase_BINARY_DIR}/generated"
                     "${LIBUV_INCLUDE_DIR}")

INCLUDE_DIRECTORIES(AFTER
                     ${ICU_INCLUDE_DIR}
                     ${V8_INCLUDE_DIR}
                     ${CURL_INCLUDE_DIR}
                     ${CMAKE_CURRENT_BINARY_DIR}
                     ${CMAKE_CURRENT_SOURCE_DIR}
                     ${CMAKE_CURRENT_SOURCE_DIR}/include
                     ${FLATBUFFERS_INCLUDE_DIR}
                     ${CMAKE_INSTALL_PREFIX}/include
                     ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/inspector
                     ${LIBS_HEADERS})

IF (NOT WIN32)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -DYY_NEVER_INTERACTIVE -DYY_NO_UNPUT -DYY_NO_INPUT")
ELSE ()
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DYY_NEVER_INTERACTIVE -DYY_NO_UNPUT -DYY_NO_INPUT")
ENDIF ()

IF(BUILD_ENTERPRISE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENTERPRISE")
ENDIF()

SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/../gen/parser/jsify.cc PROPERTIES GENERATED TRUE)

SET(EVENTING_SOURCES
    src/client.cc
    src/commands.cc
    src/v8worker.cc
    src/parse_deployment.cc
    src/breakpad.cc
    src/timer.cc
    src/histogram.cc
    ${LIBS_SRC}
    ${CMAKE_CURRENT_SOURCE_DIR}/../gen/parser/jsify.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../gen/version/version.cc)

SET(EVENTING_LIBRARIES
    ${V8_LIBRARIES}
    ${ICU_LIBRARIES}
    ${CURL_LIBRARIES}
    ${LIBUV_LIBRARIES}
    ${ZLIB_LIBRARIES}
    eventing-inspector)

IF(APPLE)
  SET(EVENTING_LIBRARIES ${EVENTING_LIBRARIES} ${FLEX_LIBRARIES})
ENDIF()

IF(BREAKPAD_FOUND)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBREAKPAD_FOUND")
  SET(EVENTING_LIBRARIES ${EVENTING_LIBRARIES} ${BREAKPAD_LIBRARIES})
  INCLUDE_DIRECTORIES(AFTER ${BREAKPAD_INCLUDE_DIR})
ENDIF()

ADD_EXECUTABLE(eventing-consumer ${EVENTING_SOURCES})
ADD_DEPENDENCIES(eventing-consumer generated  couchbase)

TARGET_LINK_LIBRARIES(eventing-consumer ${EVENTING_LIBRARIES} couchbase)
INSTALL(TARGETS eventing-consumer RUNTIME DESTINATION bin)
