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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 99)

INCLUDE (FindCouchbaseLibuv)
INCLUDE (FindCouchbaseV8)
INCLUDE (FindCouchbaseBreakpad)
INCLUDE (../features/query/FindEventingQuery.cmake)
INCLUDE (FindCouchbaseNlohmannJson)
INCLUDE (FindCouchbaseOpenSSL)

SET(FEATURES_HEADERS
        ../features/include
        ../third_party/crc64
        ../third_party/crc32)

SET(FEATURES_SRC
        ../../eventing-ee/features/src/curl.cc
        ../../eventing-ee/features/src/timer_defs.cc
        ../../eventing-ee/features/src/timer_store.cc
        ../../eventing-ee/features/src/timer_iterator.cc
        ../features/src/lcb_util.cc
        ../features/src/lang_compat.cc
        ../features/src/bucket.cc
        ../features/src/comm.cc
        ../features/src/log.cc
        ../features/src/js_exception.cc
        ../features/src/v8log.cc
        ../features/src/utils.cc
        ../features/src/base64.cc
        ../features/src/insight.cc
        ../features/src/bucket_ops.cc
        ../third_party/crc64/crc64.cc
        ../third_party/crc32/crc32.cc)

INCLUDE_DIRECTORIES(BEFORE
                     "${libcouchbase_SOURCE_DIR}/include"
                     "${libcouchbase_BINARY_DIR}/generated"
                     "${LIBUV_INCLUDE_DIR}")

INCLUDE_DIRECTORIES(AFTER
                     ${ICU_INCLUDE_DIR}
                     ${EVENTING_QUERY_INCLUDE_DIR}
                     ${V8_INCLUDE_DIR}
                     ${CURL_INCLUDE_DIR}
                     ${OPENSSL_INCLUDE_DIR}
                     ${CMAKE_CURRENT_BINARY_DIR}
                     ${CMAKE_CURRENT_SOURCE_DIR}
                     ${CMAKE_CURRENT_SOURCE_DIR}/include
                     ${FLATBUFFERS_INCLUDE_DIR}
                     ${CMAKE_INSTALL_PREFIX}/include
                     ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/inspector
                     ${FEATURES_HEADERS}
                     ${NLOHMANN_JSON_INCLUDE_DIR})

IF (NOT WIN32)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -DYY_NEVER_INTERACTIVE -DYY_NO_UNPUT -DYY_NO_INPUT")
ELSE ()
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DYY_NEVER_INTERACTIVE -DYY_NO_UNPUT -DYY_NO_INPUT")
ENDIF ()

IF(BUILD_ENTERPRISE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENTERPRISE")
ENDIF()


SET(EVENTING_SOURCES
    src/client.cc
    src/commands.cc
    src/v8worker.cc
    src/parse_deployment.cc
    src/breakpad.cc
    src/timer.cc
    src/histogram.cc
    ${FEATURES_SRC}
    ${EVENTING_QUERY_SRC}
    ${CMAKE_CURRENT_SOURCE_DIR}/../gen/version/version.cc)

SET(EVENTING_LIBRARIES
    ${V8_LIBRARIES}
    ${ICU_LIBRARIES}
    ${CURL_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${LIBUV_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${EVENTING_QUERY_LIBRARIES}
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
ADD_DEPENDENCIES(eventing-consumer eventing-generated couchbase)

TARGET_LINK_LIBRARIES(eventing-consumer ${EVENTING_LIBRARIES} couchbase)
INSTALL(TARGETS eventing-consumer RUNTIME DESTINATION bin)
