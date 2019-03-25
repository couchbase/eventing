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

PROJECT (eventing-inspector)

INCLUDE (FindCouchbaseLibuv)
INCLUDE (FindCouchbaseOpenSSL)

FIND_PACKAGE(ZLIB REQUIRED)

INCLUDE_DIRECTORIES(BEFORE ${LIBUV_INCLUDE_DIR})

INCLUDE_DIRECTORIES(AFTER  ${ICU_INCLUDE_DIR}
                           ${V8_INCLUDE_DIR}
                           ${OPENSSL_INCLUDE_DIR}
                           ${V8_INCLUDE_DIR}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_INSTALL_PREFIX}/include
                           ${ZLIB_INCLUDE_DIR})

SET(V8INSPECTOR_SOURCES validate.cc http_parser.cc inspector_agent.cc
    inspector_io.cc inspector_socket.cc inspector_socket_server.cc)

SET(V8INSPECTOR_LIBRARIES
   ${V8_LIBRARIES}
   ${ICU_LIBRARIES}
   ${LIBUV_LIBRARIES}
   ${OPENSSL_LIBRARIES}
   ${ZLIB_LIBRARIES})

ADD_LIBRARY(eventing-inspector STATIC ${V8INSPECTOR_SOURCES})
TARGET_LINK_LIBRARIES(eventing-inspector ${V8INSPECTOR_LIBRARIES})
IF (NOT WIN32)
  TARGET_COMPILE_OPTIONS(eventing-inspector PRIVATE -Wno-unused-variable -Wno-unused-function)
ENDIF ()
ADD_DEPENDENCIES(eventing-inspector generated)
