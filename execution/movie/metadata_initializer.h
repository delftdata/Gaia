#pragma once

#include "storage/metadata_initializer.h"

namespace slog {
namespace movie {

class MovieMetadataInitializer : public MetadataInitializer {
 public:
  MovieMetadataInitializer(uint32_t num_regions, uint32_t num_partitions, size_t ppartition_key_num_bytes);
  virtual Metadata Compute(const Key& key);

 private:
  uint32_t num_regions_;
  uint32_t num_partitions_;
  size_t partition_key_num_bytes_;
};

}  // namespace movie
}  // namespace slog