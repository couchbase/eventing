#
#     Copyright 2019 Couchbase, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Locate Eventing Query
# This module defines
#  EVENTING_QUERY_FOUND - Boolean to suggest if the necessary sources were found
#  EVENTING_QUERY_INCLUDE_DIR - Directory to be #includ-ed
#  EVENTING_QUERY_SRC - Sources that need to be compiled
#  EVENTING_QUERY_LIBRARIES - Libraries that Eventing Query depends on which need to be linked against

if (NOT EVENTING_QUERY_FOUND)
    include(FindCouchbaseFolly)
    include(FindCouchbaseBoost)
    include(FindCouchbaseLibevent)

    set(QUERY_DIR ${CMAKE_SOURCE_DIR}/goproj/src/github.com/couchbase/eventing/libs/query)

    set(EVENTING_QUERY_INCLUDE_DIR
            ${QUERY_DIR}/include
            ${FOLLY_INCLUDE_DIR}
            ${BOOST_INCLUDE_DIR}
            ${LIBEVENT_INCLUDE_DIR})

    set(EVENTING_QUERY_SRC
            ${QUERY_DIR}/src/iterator.cc
            ${QUERY_DIR}/src/iterable.cc
            ${QUERY_DIR}/src/conn-pool.cc
            ${QUERY_DIR}/src/manager.cc
            ${QUERY_DIR}/src/helper.cc
            ${QUERY_DIR}/src/builder.cc)

    set(EVENTING_QUERY_LIBRARIES
            ${FOLLY_LIBRARIES}
            ${Boost_CONTEXT_LIBRARY})

    if (WIN32)
        set(NT_REQUIRED_VERSION 0x0601)
        add_definitions(-D_WIN32_WINNT=${NT_REQUIRED_VERSION})
        set(EVENTING_QUERY_LIBRARIES
                ${EVENTING_QUERY_LIBRARIES}
                ws2_32.lib
                dnsapi.lib)
    endif ()
    mark_as_advanced(EVENTING_QUERY_FOUND EVENTING_QUERY_INCLUDE_DIR EVENTING_QUERY_SRC EVENTING_QUERY_LIBRARIES)
endif ()
