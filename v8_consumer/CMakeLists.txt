INCLUDE_DIRECTORIES(BEFORE ${LIBCOUCHBASE_INCLUDE_DIR} ${LIBUV_INCLUDE_DIR})

INCLUDE_DIRECTORIES(AFTER  ${ICU_INCLUDE_DIR}
                           ${V8_INCLUDE_DIR}
                           ${CURL_INCLUDE_DIR}
                           ${CMAKE_CURRENT_BINARY_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${FLATBUFFERS_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${CMAKE_INSTALL_PREFIX}/include)

SET(EVENTING_SOURCES src/client.cc src/commands.cc
    src/message.cc src/v8worker.cc src/assert.cc
    src/bucket.cc src/n1ql.cc src/parse_deployment.cc)

SET(EVENTING_LIBRARIES ${V8_LIBRARIES} ${ICU_LIBRARIES} ${JEMALLOC_LIBRARIES} ${CURL_LIBRARIES} ${LIBCOUCHBASE_LIBRARIES} ${LIBUV_LIBRARIES} platform)
ADD_EXECUTABLE(client ${EVENTING_SOURCES})
TARGET_LINK_LIBRARIES(client ${EVENTING_LIBRARIES})
INSTALL(TARGETS client
        RUNTIME DESTINATION bin)

