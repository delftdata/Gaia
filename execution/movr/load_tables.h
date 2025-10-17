#pragma once

#include "execution/movr/constants.h"
#include "execution/movr/storage_adapter.h"

namespace slog {
namespace movr {

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_regions, int num_partitions, int partition,
                int num_threads = 3);

}  // namespace movr
}  // namespace slog