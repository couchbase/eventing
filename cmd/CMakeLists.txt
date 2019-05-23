PROJECT (eventing-producer)

INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseCurl)

SET(LDFLAGS)
IF(APPLE)
  SET (LDFLAGS "-s -extldflags '-Wl,-rpath,@executable_path/../lib'")
ENDIF()
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
IF("${PRODUCT_VERSION}" VERSION_LESS "6.5.0" AND
   NOT "${PRODUCT_VERSION}" VERSION_EQUAL "0.0.0")
  SET(TAGS "${TAGS} alice")
ENDIF()

GET_FILENAME_COMPONENT (CURL_LIB_DIR "${CURL_LIBRARIES}" DIRECTORY)
SET(CGO_INCLUDE_DIRS "${sigar_SOURCE_DIR}/include;${CURL_INCLUDE_DIR}")
SET(CGO_LIBRARY_DIRS "${sigar_BINARY_DIR}/src;${CURL_LIB_DIR}")

GoInstall(TARGET eventing-producer PACKAGE github.com/couchbase/eventing/cmd/producer
  GOPATH "${PROJECT_SOURCE_DIR}/../../../../.." "${GODEPSDIR}"
  INSTALL_PATH bin OUTPUT eventing-producer
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION})

GoInstall(TARGET cbevent PACKAGE github.com/couchbase/eventing/cmd/cbevent
  GOPATH "${PROJECT_SOURCE_DIR}/../../../../.." "${GODEPSDIR}"
  INSTALL_PATH bin OUTPUT cbevent
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION})

ADD_DEPENDENCIES(eventing-producer generated cbq-engine indexer)
ADD_DEPENDENCIES(cbevent generated)
