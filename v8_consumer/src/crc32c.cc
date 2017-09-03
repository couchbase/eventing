#include "crc32c.h"

// Castagnoli's polynomial, used in iSCSI.
// Has better error detection characteristics than IEEE.
#define CASTAGNOLI_POLYNOMIAL 0x82f63b78

#define ALIGN_SIZE 0x08UL
#define ALIGN_MASK (ALIGN_SIZE - 1)

#define CRC(op, crc, type, buf, len)                                           \
  do {                                                                         \
    for (; (len) >= sizeof(type);                                              \
         (len) -= sizeof(type), buf += sizeof(type)) {                         \
      (crc) = op((crc), *(type *)(buf));                                       \
    }                                                                          \
  } while (0)

// Software CRC32 lookup table
static uint32_t crc32c_table[8][256];

void initCrcTable() {
  uint32_t i, j, crc;

  for (i = 0; i < 256; i++) {
    crc = i;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc = crc & 1 ? (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >> 1;
    crc32c_table[0][i] = crc;
  }

  for (i = 0; i < 256; i++) {
    crc = crc32c_table[0][i];
    for (j = 1; j < 8; j++) {
      crc = crc32c_table[0][crc & 0xff] ^ (crc >> 8);
      crc32c_table[j][i] = crc;
    }
  }
}

// Check for availability of hardware-based CRC-32C
void cpuid(uint32_t op, uint32_t reg[4]) {
#if defined(_WIN64) || defined(_WIN32)
#include <intrin.h>
  __cpuid((int *)reg, 1);
#elif defined(__x86_64__)
  __asm__ volatile("pushq %%rbx       \n\t"
                   "cpuid             \n\t"
                   "movl  %%ebx, %1   \n\t"
                   "popq  %%rbx       \n\t"
                   : "=a"(reg[0]), "=r"(reg[1]), "=c"(reg[2]), "=d"(reg[3])
                   : "a"(op)
                   : "cc");
#else
  reg[0] = reg[1] = reg[2] = reg[3] = 0;
#endif
}

bool isSSE42Supported() {
  uint32_t reg[4];

  cpuid(1, reg);
  return ((reg[2] >> 20) & 1) == 1;
}

uint32_t swCRC32c(uint32_t initialCrc, const char *buf, size_t len) {
  const char *next = buf;
  uint64_t crc = initialCrc;

  if (len == 0)
    return (uint32_t)crc;

  crc ^= 0xFFFFFFFF;

  while (len && ((uintptr_t)next & 7) != 0) {
    crc = crc32c_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }

  while (len >= 8) {
    crc ^= *(uint64_t *)next;
    crc = crc32c_table[7][(crc >> 0) & 0xff] ^
          crc32c_table[6][(crc >> 8) & 0xff] ^
          crc32c_table[5][(crc >> 16) & 0xff] ^
          crc32c_table[4][(crc >> 24) & 0xff] ^
          crc32c_table[3][(crc >> 32) & 0xff] ^
          crc32c_table[2][(crc >> 40) & 0xff] ^
          crc32c_table[1][(crc >> 48) & 0xff] ^ crc32c_table[0][(crc >> 56)];
    next += 8;
    len -= 8;
  }

  while (len) {
    crc = crc32c_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }

  return (uint32_t)(crc ^= 0xFFFFFFFF);
}

uint32_t hwCRC32c(uint32_t initialCrc, const char *buf, size_t len) {
  uint32_t crc = initialCrc;

  if (len == 0)
    return crc;

  crc ^= 0xFFFFFFFF;

  for (; (len > 0) && ((size_t)buf & ALIGN_MASK); len--, buf++) {
    crc = _mm_crc32_u8(crc, *buf);
  }

#if defined(__x86_64__) || defined(_M_X64)
  CRC(_mm_crc32_u64, crc, uint64_t, buf, len);
#endif
  CRC(_mm_crc32_u32, crc, uint32_t, buf, len);
  CRC(_mm_crc32_u16, crc, uint16_t, buf, len);
  CRC(_mm_crc32_u8, crc, uint8_t, buf, len);

  return (crc ^= 0xFFFFFFFF);
}

uint32_t crc32c(uint32_t initialCrc, const char *buf, size_t len) {
  if (isSSE42Supported()) {
    return hwCRC32c(initialCrc, buf, len);
  } else {
    return swCRC32c(initialCrc, buf, len);
  }
}
