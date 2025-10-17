#include "execution/smallbank/metadata_initializer.h"

#include <glog/logging.h>

#include <functional>  // For std::hash
namespace slog {
namespace smallbank {

uint32_t murmurhash3(const std::string& str) {
  uint32_t seed = 42;  // You can use any seed
  uint32_t hash = seed;
  const char* data = str.c_str();
  int len = str.length();

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  int i = 0;
  while (len >= 4) {
    uint32_t k = *(reinterpret_cast<const uint32_t*>(data + i));
    k *= c1;
    k = (k << 15) | (k >> 17);  // ROTL32(k, 15)
    k *= c2;

    hash ^= k;
    hash = (hash << 13) | (hash >> 19);  // ROTL32(hash, 13)
    hash = hash * 5 + 0xe6546b64;

    len -= 4;
    i += 4;
  }

  uint32_t tail = 0;
  switch (len) {
    case 3:
      tail ^= data[i + 2] << 16;
      break;
    case 2:
      tail ^= data[i + 1] << 8;
      break;
    case 1:
      tail ^= data[i];
      break;
  }

  tail *= c1;
  tail = (tail << 15) | (tail >> 17);
  tail *= c2;
  hash ^= tail;

  hash ^= str.length();
  hash ^= (hash >> 16);
  hash *= 0x85ebca6b;
  hash ^= (hash >> 13);
  hash *= 0xc2b2ae35;
  hash ^= (hash >> 16);

  return hash;
}

SmallBankMetadataInitializer::SmallBankMetadataInitializer(uint32_t num_regions, uint32_t num_partitions)
    : num_regions_(num_regions), num_partitions_(num_partitions) {}

Metadata SmallBankMetadataInitializer::Compute(const Key& key) {
  if (key.size() == 26) {
    std::string client_name(reinterpret_cast<const char*>(key.data()), 24);
    return Metadata((murmurhash3(client_name) / num_partitions_) % num_regions_);
  } else {
    uint32_t client_id = *reinterpret_cast<const uint32_t*>(key.data());
    return Metadata(((client_id) / num_partitions_) % num_regions_);
  }
}

}  // namespace smallbank
}  // namespace slog