#pragma once

#include "storage/metadata_initializer.h"

namespace slog {
namespace pps {

class PPSMetadataInitializer : public MetadataInitializer {
 public:
  PPSMetadataInitializer(uint32_t num_regions, uint32_t num_partitions);
  virtual Metadata Compute(const Key& key);

 private:
  uint32_t num_regions_;
  uint32_t num_partitions_;
};

}  // namespace pps
}  // namespace slog