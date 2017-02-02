macro(Flatbuffers)
    add_custom_command(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/../include/${ARGV0}_generated.h
                    COMMAND
                         ${FLATC} -c
                            -o ${CMAKE_CURRENT_SOURCE_DIR}/../include/
                                  ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                    COMMAND
                         ${FLATC} -g
                            -o ${CMAKE_CURRENT_SOURCE_DIR}/../../
                            ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                      DEPENDS
                         ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                         ${FLATC}
                      COMMENT "Generating code for ${ARGV0}")
endmacro(Flatbuffers)

Flatbuffers(header)
Flatbuffers(payload)
