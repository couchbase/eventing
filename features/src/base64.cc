// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static const uint8_t code[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void encode_rest(const uint8_t *s, std::string &result, size_t num) {
  uint32_t val = 0;

  switch (num) {
  case 2:
    val = (uint32_t)((*s << 16) | (*(s + 1) << 8));
    break;
  case 1:
    val = (uint32_t)((*s << 16));
    break;
  default:
    throw std::invalid_argument("encode_rest num may be 1 or 2");
  }

  result.push_back((char)code[(val >> 18) & 63]);
  result.push_back((char)code[(val >> 12) & 63]);
  if (num == 2) {
    result.push_back((char)code[(val >> 6) & 63]);
  } else {
    result.push_back('=');
  }
  result.push_back('=');
}

static void encode_triplet(const uint8_t *s, std::string &str) {
  uint32_t val = (uint32_t)((*s << 16) | (*(s + 1) << 8) | (*(s + 2)));
  str.push_back((char)code[(val >> 18) & 63]);
  str.push_back((char)code[(val >> 12) & 63]);
  str.push_back((char)code[(val >> 6) & 63]);
  str.push_back((char)code[val & 63]);
}

std::string base64Encode(const std::string &blob) {
  auto triplets = blob.size() / 3;
  auto rest = blob.size() % 3;
  auto chunks = triplets;
  if (rest != 0) {
    ++chunks;
  }

  std::string result;
  result.reserve(chunks * 4);

  const uint8_t *in = reinterpret_cast<const uint8_t *>(blob.data());

  for (size_t ii = 0; ii < triplets; ++ii) {
    encode_triplet(in, result);
    in += 3;
  }

  if (rest > 0) {
    encode_rest(in, result, rest);
  }
  return result;
}
