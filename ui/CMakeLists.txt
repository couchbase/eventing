#
# CMakeLists.txt for eventing-ui
#
# We need to copy the pluggable-ui-query.json to install/etc/couchbase,
# and copy everything else to install/lib/eventing-ui
# 
CMAKE_MINIMUM_REQUIRED (VERSION 2.8.10)


# installed system now uses relative path, so no prefix
SET (_instdir "${PROJECT_BINARY_DIR}")
SET (BIN_PREFIX "")

# rewrite the config file for installation
configure_file (pluggable-ui-eventing.json.in pluggable-ui-eventing.json)

# copy rewritten config file and code to install directory
install(FILES "${_instdir}/ui/pluggable-ui-eventing.json"
        DESTINATION etc/couchbase)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/eventing-ui
        DESTINATION lib)

install(FILES
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/estools/escodegen.min.js
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/estools/esprima.min.js
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/ace/ace.js
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/ace/ui-ace.min.js
           ${CMAKE_CURRENT_SOURCE_DIR}/../third_party/ace/ext-language_tools.js
        DESTINATION lib/eventing-ui/lib)

# rewrite the config file for running locally (using cluster-run)
SET (BIN_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/")
configure_file (pluggable-ui-eventing.json.in ${_instdir}/../../../../../cluster_run_ui_plugins/pluggable-ui-eventing.cluster_run.json)


