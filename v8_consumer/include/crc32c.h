#ifndef CRC32C_H
#define CRC32C_H

#include <iostream>
#include <nmmintrin.h>
#include <stdint.h>
#include <stdlib.h>

bool isSSE42Supported();
void initCrcTable();
uint32_t crc32c(uint32_t initialCrc, const char *buf, size_t len);

#endif
