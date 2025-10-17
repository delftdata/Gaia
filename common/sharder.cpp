#include "common/sharder.h"

#include <glog/logging.h>

namespace slog {

namespace {

template <class It>
uint32_t FNVHash(It begin, It end) {
  uint64_t hash = 0x811c9dc5;
  for (auto it = begin; it != end; it++) {
    hash = (hash * 0x01000193) % (1LL << 32);
    hash ^= *it;
  }
  return hash;
}

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

}  // namespace

std::shared_ptr<Sharder> Sharder::MakeSharder(const ConfigurationPtr& config) {
  if (config->proto_config().has_simple_partitioning()) {
    return std::make_shared<SimpleSharder>(config);
  } else if (config->proto_config().has_simple_partitioning2()) {
    return std::make_shared<SimpleSharder2>(config);
  } else if (config->proto_config().has_tpcc_partitioning()) {
    return std::make_shared<TPCCSharder>(config);
  } else if (config->proto_config().has_dsh_partitioning()) {
    return std::make_shared<DSHSharder>(config);
  } else if (config->proto_config().has_movr_partitioning()) {
    return std::make_shared<MovrSharder>(config);
  } else if (config->proto_config().has_pps_partitioning()) {
    return std::make_shared<PPSSharder>(config);
  } else if (config->proto_config().has_movie_partitioning()) {
    return std::make_shared<MovieSharder>(config);
  } else if (config->proto_config().has_smallbank_partitioning()) {
    return std::make_shared<SmallBankSharder>(config);
  }
  return std::make_shared<HashSharder>(config);
}

Sharder::Sharder(const ConfigurationPtr& config)
    : local_partition_(config->local_partition()), num_partitions_(config->num_partitions()) {}

bool Sharder::is_local_key(const Key& key) const { return compute_partition(key) == local_partition_; }

uint32_t Sharder::num_partitions() const { return num_partitions_; }

uint32_t Sharder::local_partition() const { return local_partition_; }

/**
 * Hash Sharder
 */
HashSharder::HashSharder(const ConfigurationPtr& config)
    : Sharder(config), partition_key_num_bytes_(config->proto_config().hash_partitioning().partition_key_num_bytes()) {}
uint32_t HashSharder::compute_partition(const Key& key) const {
  auto end = partition_key_num_bytes_ >= key.length() ? key.end() : key.begin() + partition_key_num_bytes_;
  return FNVHash(key.begin(), end) % num_partitions_;
}

/**
 * Simple Sharder
 *
 * This sharder assumes the following home/partition assignment
 *
 *        home | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 * partition 0 | 0  3  6  9  12 15 18 21 24 ...
 * partition 1 | 1  4  7  10 13 16 19 22 25 ...
 * partition 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * Taking the modulo of the key by the number of partitions gives the partition of the key
 */
SimpleSharder::SimpleSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t SimpleSharder::compute_partition(const Key& key) const { return std::stoll(key) % num_partitions_; }

/**
 * Simple Sharder 2
 *
 * This sharder assumes the following home/partition assignment
 *
 *   partition | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 *      home 0 | 0  3  6  9  12 15 18 21 24 ...
 *      home 1 | 1  4  7  10 13 16 19 22 25 ...
 *      home 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * We divide the key by the number of regions to get the "column number" of the key.
 * Then, taking the modulo of the column number by the number of partitions gives the partition
 * of the key.
 */
SimpleSharder2::SimpleSharder2(const ConfigurationPtr& config) : Sharder(config), num_regions_(config->num_regions()) {}
uint32_t SimpleSharder2::compute_partition(const Key& key) const {
  return (std::stoll(key) / num_regions_) % num_partitions_;
}

/**
 * TPC-C Sharder
 */
TPCCSharder::TPCCSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t TPCCSharder::compute_partition(const Key& key) const {
  int w_id = *reinterpret_cast<const int*>(key.data());
  return (w_id - 1) % num_partitions_;
}

/**
 * DeathStar Hotels Sharder
 */

DSHSharder::DSHSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t DSHSharder::compute_partition(const Key& key) const {
  uint32_t id;
  // partitioning for usernames which string (actually just user ID formatted but not relevant)
  if (key.length() == 22) {
    uint32_t res;
		std::from_chars(key.data(), key.data() + 2, res);
		std::from_chars(key.data() + 20 - res, key.data() + 20, id);
  // everything else uses an integer as their key on which to shard
  } else {
    id = *reinterpret_cast<const uint32_t*>(key.data());
  }
  return id % num_partitions_;
}

/**
 * MovR Sharder
 */
MovrSharder::MovrSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t MovrSharder::compute_partition(const Key& key) const {
  uint64_t global_id = *reinterpret_cast<const uint64_t*>(key.data());
  constexpr int kPartitionBits = 16;
  uint32_t city_index = static_cast<uint32_t>(global_id >> (64 - kPartitionBits));

  return city_index % num_partitions_;
}

/**
 * PPS Sharder
 */
PPSSharder::PPSSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t PPSSharder::compute_partition(const Key& key) const {
  int id = *reinterpret_cast<const int*>(key.data());
  return (id - 1) % num_partitions_;
}

/**
 * Movie Sharder
 *
 * This sharder assumes the following home/partition assignment
 *
 *        home | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 * partition 0 | 0  3  6  9  12 15 18 21 24 ...
 * partition 1 | 1  4  7  10 13 16 19 22 25 ...
 * partition 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * Taking the modulo of the key by the number of partitions gives the partition of the key
 */

MovieSharder::MovieSharder(const ConfigurationPtr& config) : Sharder(config), num_regions_(config->num_regions()) {}
uint32_t MovieSharder::compute_partition(const Key& key) const {
  std::string id = key.substr(0, 12);
  int intid = std::stoll(id);
  return intid % num_partitions_;
}

/**
 * SmallBank Sharder
 */
SmallBankSharder::SmallBankSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t SmallBankSharder::compute_partition(const Key& key) const {
  if (key.size() == 26) {
    std::string client_name(reinterpret_cast<const char*>(key.data()), 24);
    return murmurhash3(client_name) % num_partitions_;
  } else {
    int client_id = *reinterpret_cast<const int*>(key.data());
    return (client_id) % num_partitions_;
    return 0;
  }
}

}  // namespace slog