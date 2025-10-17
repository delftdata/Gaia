#pragma once

#include "execution/pps/constants.h"
#include "execution/pps/storage_adapter.h"

namespace slog {
namespace pps {

void LoadTables(const StorageAdapterPtr& storage_adapter, int num_products, int num_parts, int num_suppliers, 
    int num_regions, int num_partitions, int local_partition, 
    int max_regions, int max_partitions, int num_threads = 3);

}  // namespace pps
}  // namespace slog