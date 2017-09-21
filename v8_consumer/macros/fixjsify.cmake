# This is a temporary script. Flex will be switched to c++ mode soon
FILE(READ gen/jsify.c JSIFY)
STRING(REPLACE "extern int yylex" "extern \"C\" int yylex" JSIFY "${JSIFY}")
FILE(WRITE gen/jsify.cc "${JSIFY}")
