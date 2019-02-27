/*
 * CRC64 Specification:
 * Width: 64 bits
 * Poly: ISO/ECMA
 * Reflected Input: True
 * Xor Input: 0xffffffffffffffff
 * Reflected Output: True
 * Xor Output: 0x0
 */

#ifndef CRC_H
#define CRC_H

#include <array>
#include <stdint.h>

template <uint64_t polynomial> class Crc64Generator {
public:
  static uint64_t crc64(uint64_t seed, const uint8_t *data, uint64_t len);

private:
  static std::array<uint64_t, 256> MakeCrc64Table();
  static const std::array<uint64_t, 256> crc64_table;
};

template <uint64_t polynomial>
const std::array<uint64_t, 256> Crc64Generator<polynomial>::crc64_table =
    Crc64Generator<polynomial>::MakeCrc64Table();

template <uint64_t polynomial>
uint64_t Crc64Generator<polynomial>::crc64(uint64_t crc, const uint8_t *data,
                                           uint64_t len) {
  for (uint64_t idx = 0; idx < len; idx++) {
    crc = crc64_table[static_cast<uint8_t>(crc >> 56) ^ data[idx]] ^ (crc >> 8);
  }
  return crc;
}

template <uint64_t polynomial>
std::array<uint64_t, 256> Crc64Generator<polynomial>::MakeCrc64Table() {
  std::array<uint64_t, 256> crc64_table{};
  for (size_t idx = 0; idx < 256; ++idx) {
    uint64_t crc = idx;
    for (uint8_t bit_pos = 0; bit_pos < 8; ++bit_pos) {
      if ((crc & 1) == 1) {
        crc = (crc >> 1) ^ polynomial;
      } else {
        crc >>= 1;
      }
    }
    crc64_table[idx] = crc;
  }
  return crc64_table;
}

// ISO polynomial=0xD800000000000000, reflection of ISO polynomial=0x1B
using crc64_iso = Crc64Generator<0x1B>;

// ECMA polynomial=0xC96C5795D7870F42, reflection of ECMA polynomial= 0x42F0E1EBA9EA3693
using crc64_ecma = Crc64Generator<0x42F0E1EBA9EA3693>;

#endif