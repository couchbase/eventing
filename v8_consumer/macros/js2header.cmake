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
SET(THIS_DIR ${CMAKE_CURRENT_LIST_DIR})

macro(js2header)
    MESSAGE (STATUS "Converting ${ARGV0} to ${ARGV1} header as variable ${ARGV2}")
    SET (ENV{GOROOT} "${GOROOT}")
    EXEC_PROGRAM(
        ${GOROOT}/bin/go ${CMAKE_CURRENT_SOURCE_DIR}
	ARGS run ${THIS_DIR}/generate.go ${ARGV0} ${ARGV1} ${ARGV2}
        RETURN_VALUE rval)
    IF (NOT "${rval}" STREQUAL "0")
        MESSAGE(FATAL_ERROR "Error converting ${ARGV0} to header")
    endif()
endmacro(js2header)

