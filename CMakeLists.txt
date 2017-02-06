CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
IF (${CMAKE_MAJOR_VERSION} GREATER 2)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF (${CMAKE_MAJOR_VERSION} GREATER 2)


PROJECT (eventing)


INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseJemalloc)
INCLUDE (FindCouchbaseFlatbuffers)
INCLUDE (FindCouchbaseEventingDep)

ADD_SUBDIRECTORY(patches)
ADD_SUBDIRECTORY(flatbuf/schema)
ADD_SUBDIRECTORY(v8_consumer)
ADD_SUBDIRECTORY(ui)

SET (_eventing_ldflags "${_ldflags}")
IF (WIN32)
  INCLUDE (PlatformIntrospection)
  _DETERMINE_ARCH (_arch)
  IF (NOT _arch STREQUAL x86)
    SET (_eventing_ldflags "${_eventing_ldflags} -linkmode internal")
  ENDIF ()
ENDIF ()

GoInstall (TARGET go_eventing PACKAGE github.com/couchbase/eventing/cmd/producer/
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  LDFLAGS "${_eventing_ldflags}"
  INSTALL_PATH bin OUTPUT go_eventing
  GOVERSION 1.6)
IF (APPLE)
    SET_TARGET_PROPERTIES(go_eventing
                          PROPERTIES
                          INSTALL_RPATH "@rpath")
ENDIF (APPLE)


