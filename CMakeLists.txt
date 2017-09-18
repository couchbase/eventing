PROJECT (eventing)

INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseJemalloc)
INCLUDE (FindCouchbaseFlatbuffers)
INCLUDE (FindCouchbaseLibuv)
INCLUDE (FindCouchbaseLibCouchbase)

ADD_SUBDIRECTORY(flatbuf/schema)
ADD_SUBDIRECTORY(v8_consumer)
ADD_SUBDIRECTORY(ui)

FIND_PACKAGE(ZLIB)
IF(ZLIB_FOUND)
  ADD_SUBDIRECTORY(third_party/inspector)
ENDIF()

SET (LDFLAGS)
IF (APPLE)
  # On macOS, we must use -rpath to locate libraries
  SET (LDFLAGS "-extldflags '-Wl,-rpath,@executable_path/../lib'")
ENDIF ()
IF (WIN32)
   SET (LDFLAGS "${LDFLAGS} -linkmode internal")
ENDIF ()

SET(GOVERSION 1.8.3)
SET(TAGS "jemalloc")

GET_FILENAME_COMPONENT (JEMALLOC_LIB_DIR ${JEMALLOC_LIBRARIES} DIRECTORY)
SET (ENV{CGO_CFLAGS} "$ENV{CGO_CFLAGS} -DJEMALLOC=1")
SET(CGO_INCLUDE_DIRS "${sigar_SOURCE_DIR}/include;${Platform_SOURCE_DIR}/include;${JEMALLOC_INCLUDE_DIR}")
SET(CGO_LIBRARY_DIRS "${sigar_BINARY_DIR}/src;${Platform_BINARY_DIR};${JEMALLOC_LIB_DIR}")

GoInstall (TARGET eventing-producer PACKAGE github.com/couchbase/eventing/cmd/producer
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  INSTALL_PATH bin OUTPUT eventing-producer
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${TAGS}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION ${GOVERSION})

