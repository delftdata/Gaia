#include "execution/movr/metadata_initializer.h"

#include <glog/logging.h>

namespace slog {
namespace movr {

MovrMetadataInitializer::MovrMetadataInitializer(uint32_t num_regions, uint32_t num_partitions)
    : num_regions_(num_regions), num_partitions_(num_partitions) {}

Metadata MovrMetadataInitializer::Compute(const Key& key) {
  CHECK_GE(key.size(), 8) << "Invalid key - MovR keys should be at least 8 bytes";
  
  uint64_t global_id = *reinterpret_cast<const uint64_t*>(key.data());
  
  constexpr int kPartitionBits = 16;
  uint32_t city_index = static_cast<uint32_t>(global_id >> (64 - kPartitionBits));
  
  return Metadata((city_index / num_partitions_) % num_regions_);
}

}  // namespace movr
}  // namespace slog