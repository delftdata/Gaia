#pragma once

#include "execution/smallbank/storage_adapter.h"

namespace slog {
namespace smallbank {

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_regions, int num_partitions, int partition,
                int num_threads = 3);

}  // namespace smallbank
}  // namespace slog