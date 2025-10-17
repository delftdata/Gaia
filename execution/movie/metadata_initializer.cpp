#include "execution/movie/metadata_initializer.h"

#include <glog/logging.h>

namespace slog {
namespace movie {


MovieMetadataInitializer::MovieMetadataInitializer(uint32_t num_regions, uint32_t num_partitions, size_t partition_key_num_bytes)
    : num_regions_(num_regions), num_partitions_(num_partitions), partition_key_num_bytes_(partition_key_num_bytes) {}
/**
 * This initializer assumes the following home/partition assignment
 *
 *        home | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 * partition 0 | 0  3  6  9  12 15 18 21 24 ...
 * partition 1 | 1  4  7  10 13 16 19 22 25 ...
 * partition 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * We divide the key by the number of parititons to get the "column number" of the key.
 * Then, taking the modulo of the column number by the number of regions gives the home
 * of the key.
 */
Metadata MovieMetadataInitializer::Compute(const Key& key) {
  std::string id = key.substr(0, 12);
  int intid = std::stoll(id);
  return Metadata((intid / num_partitions_) % num_regions_);
}

}  // namespace movie
}  // namespace slog