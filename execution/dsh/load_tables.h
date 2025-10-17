#pragma once

#include "execution/dsh/storage_adapter.h"

namespace slog {
namespace dsh {

void LoadTables(const StorageAdapterPtr& storage_adapter, int num_partitions, int partition, int num_regions, int num_users, int num_hotels,
    double coord_range, int num_threads = 3);

}  // namespace tpcc
}  // namespace slog