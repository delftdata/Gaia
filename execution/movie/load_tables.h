#pragma once

#include "execution/movie/storage_adapter.h"

namespace slog {
namespace movie {

void LoadTables(const StorageAdapterPtr& storage_adapter, int W, int num_regions, int num_partitions, int partition,
                int num_threads = 3);
inline void addLeadingZeros(int totallength, std::string& original) {
    int zeroes = totallength - original.length();
    original.insert(0, zeroes, '0');
}
inline void addTrailingSpaces(int totallength, std::string& original) {
    int spaces = totallength - original.length();
    original.insert(original.length(), spaces, ' ');
}

}  // namespace movie
}  // namespace slog