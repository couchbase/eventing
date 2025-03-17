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
IF("${PRODUCT_VERSION}" VERSION_GREATER "6.5.0" AND
   "${PRODUCT_VERSION}" VERSION_LESS "7.1.0" AND
   NOT "${PRODUCT_VERSION}" VERSION_EQUAL "0.0.0")
  SET(TAGS "${TAGS} mh")
ENDIF()
IF("${PRODUCT_VERSION}" VERSION_GREATER_EQUAL "7.1.0" OR
   "${PRODUCT_VERSION}" VERSION_EQUAL "0.0.0")
  SET(TAGS "${TAGS} neo")
ENDIF()

IF(BUILD_ENTERPRISE)
    SET(NEWEVAL_DIR ../../eventing-ee/evaluator)
    find_path(NEWEVAL_INCLUDE_DIR worker.h
        HINTS ${NEWEVAL_DIR}/worker/include)
    LIST(APPEND CGO_INCLUDE_DIRS ${NEWEVAL_INCLUDE_DIR})
    LIST(APPEND CGO_LIBRARY_DIRS ${evaluator_BINARY_DIR})
ENDIF()

GoModBuild(TARGET eventing-producer PACKAGE github.com/couchbase/eventing/cmd/producer
  INSTALL_PATH bin OUTPUT eventing-producer
  DEPENDS sigar Curl::libcurl eventing-generated
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION})

GoModBuild(TARGET cbevent PACKAGE github.com/couchbase/eventing/cmd/cbevent
  INSTALL_PATH bin
  DEPENDS sigar Curl::libcurl eventing-generated
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION})

ADD_DEPENDENCIES(eventing-producer cbq-engine indexer jseval)
