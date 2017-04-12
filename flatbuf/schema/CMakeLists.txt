function(Flatbuffers)
    execute_process(
                    COMMAND
                         ${FLATC} -c
                            -o ${CMAKE_CURRENT_SOURCE_DIR}/../include/
                                  ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                    COMMAND
                         ${FLATC} -g
                            -o ${CMAKE_CURRENT_SOURCE_DIR}/../../
                            ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                            )
endfunction(Flatbuffers)

Flatbuffers(header)
Flatbuffers(payload)
Flatbuffers(cfg_schema)
