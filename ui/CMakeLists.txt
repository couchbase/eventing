PROJECT (eventing-ui)
INCLUDE (CouchbasePlugInUI)

if ("${PRODUCT_VERSION}" VERSION_LESS "6.5.0" AND
    NOT "${PRODUCT_VERSION}" VERSION_EQUAL "0.0.0")
  SET (PROXY_STRATEGY "local")
else()
  SET (PROXY_STRATEGY "sticky")
endif()

# generated files
FILE (COPY "../gen/version/version.js" DESTINATION "${NS_UI_PUB_DIR}")

#Configure build and install
PLUG_IN_UI ("pluggable-ui-eventing")
