// Attributed to https://golang.org/src/hash/crc64/crc64.go

#ifndef _EVENTING_CRC64
#define _EVENTING_CRC64

#include <stdint.h>

class CRC64 {
public:
    CRC64(uint64_t poly);
    ~CRC64();
    uint64_t Checksum(const uint8_t* data, uint64_t sz) const;

private:
    const uint64_t* const table_;
};

extern const CRC64 crc64_iso;  // polynomial 0xD800000000000000
extern const CRC64 crc64_ecma; // polynomial 0xC96C5795D7870F42

#endif // _EVENTING_CRC64

