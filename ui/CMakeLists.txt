# for actual install
SET (BIN_PREFIX "")
configure_file (pluggable-ui-eventing.json.in pluggable-ui-eventing.json)
install(FILES "${PROJECT_BINARY_DIR}/ui/pluggable-ui-eventing.json"
        DESTINATION etc/couchbase)

# for cluster_run
SET (BIN_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/")
configure_file (pluggable-ui-eventing.json.in pluggable-ui-eventing.cluster_run.json)
file(COPY "${PROJECT_BINARY_DIR}/ui/pluggable-ui-eventing.cluster_run.json"
        DESTINATION  "${PROJECT_BINARY_DIR}/../../../../../cluster_run_ui_plugins")

# third party files
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/estools/escodegen.min.js
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/estools/esprima.min.js
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/ace/
        DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/eventing-ui/lib)

# install all files including third party
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/eventing-ui
        DESTINATION lib)

