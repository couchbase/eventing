// Attributed to https://golang.org/src/hash/crc64/crc64.go

#include "crc64.h"

static const uint64_t* makeTable(uint64_t poly) {
    auto table = new uint64_t[256];
    for (auto i = 0; i < 256; i++) {
        auto crc = uint64_t(i);
        for (auto j = 0; j < 8; j++) {
            if ((crc & 1) == 1) {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    return table;
}

CRC64::CRC64(uint64_t poly) : table_(makeTable(poly)) {
}

CRC64::~CRC64() {
    delete[] table_;
}

uint64_t CRC64::Checksum(const uint8_t* data, uint64_t len) const {
    auto crc = uint64_t(0);
    crc = ~crc;
    for (auto i = 0; i < len; i++) {
        auto v = data[i];
        crc = table_[uint8_t(crc) ^ v] ^ (crc >> 8);
    }
    return ~crc;
}

const CRC64 crc64_iso(0xD800000000000000);
const CRC64 crc64_ecma(0xC96C5795D7870F42);
