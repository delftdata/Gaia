#include "execution/pps/load_tables.h"

#include <algorithm>
#include <random>
#include <thread>

#include "common/string_utils.h"
#include "execution/pps/table.h"

namespace slog {
namespace pps {

class PartitionedPPSDataLoader {
 public:
 PartitionedPPSDataLoader(const StorageAdapterPtr& storage_adapter, int num_products, int num_parts, int num_suppliers, 
  int num_regions, int num_partitions, int local_partition, int max_regions, int max_partitions, int num_threads, int seed)
      : num_products_(num_products),
        num_parts_(num_parts),
        num_suppliers_(num_suppliers),
        num_partitions_(num_partitions),
        num_regions_(num_regions),
        local_partition_(local_partition),
        max_regions_(max_regions),
        max_partitions_(max_partitions),
        num_threads_(num_threads),
        rg_(seed),
        storage_adapter_(storage_adapter),
        num_parts_per_class_(num_parts / (num_partitions * num_regions)) {
          remote_regions_ = std::vector<std::vector<int>>(num_regions_);
          for (int i = 0; i < num_regions_; i++) {
            remote_regions_[i].reserve(num_regions_ - 1);
            for (int j = 0; j < num_regions_; j++) {
              if (i != j) {
                remote_regions_[i].push_back(j);
              }
            }
          }
          remote_partitions_.reserve(num_partitions_ - 1);
          for (int i = 0; i < num_partitions_; i++) {
            if (i != local_partition_) {
              remote_partitions_.push_back(i);
            }
          }
        }

  void Load(int thread_index) {
    // Add the tables for the products
    LOG(INFO) << "Generating ~" << num_products_ / num_partitions_ << " products for thread " << thread_index << "/" << num_threads_;
    Table<ProductSchema> productTable(storage_adapter_); 
    for (uint32_t product_id = 1; product_id <= num_products_; product_id++) {
      if (computePartition(product_id) == local_partition_) {
        LOG(INFO) << "Load the product with id " << product_id;
        productTable.Insert({
            MakeInt32Scalar(product_id),
            MakeFixedTextScalar<10>(str_gen_(10))
        });
      }
    }

    // Add the tables for the parts
    LOG(INFO) << "Generating ~" << num_parts_ / num_partitions_ << " parts for thread " << thread_index << "/" << num_threads_;
    Table<PartSchema> partTable(storage_adapter_);
    for (uint32_t part_id = 1; part_id <= num_parts_; part_id++) {
      if (computePartition(part_id) == local_partition_) {
        LOG(INFO) << "Load the part with id " << part_id;
        partTable.Insert({
            MakeInt32Scalar(part_id),
            MakeInt64Scalar(1000 + (part_id % 100)),
            MakeFixedTextScalar<10>(str_gen_(10))
        });
      }
    }

    // Add the tables for the suppliers
    LOG(INFO) << "Generating ~" << num_suppliers_ / num_partitions_ << " suppliers for thread " << thread_index << "/" << num_threads_;
    Table<SupplierSchema> supplierTable(storage_adapter_);
    for (uint32_t supplier_id = 1; supplier_id <= num_suppliers_; supplier_id++) {
      if (computePartition(supplier_id) == local_partition_) {
        LOG(INFO) << "Load the supplier with id " << supplier_id;
        supplierTable.Insert({
            MakeInt32Scalar(supplier_id),
            MakeFixedTextScalar<10>(str_gen_(10))
        });
      }
    }

    // Add the product-to-parts mapping
    LOG(INFO) << "Generating ~" << num_products_ * pps::kPartsPerProduct / num_partitions_ << " product-to-parts for thread " << thread_index << "/" << num_threads_;
    CHECK(num_parts_per_class_ >= 4) << "Not enough parts per class for each category: " << num_parts_per_class_;
    Table<ProductPartsSchema> productPartsTable(storage_adapter_);
    int count_product_parts = 0;
    for (uint32_t product_id = 1; product_id <= num_products_; product_id++) {      
      if (computePartition(product_id) == local_partition_) {
        int product_region = computeRegion(product_id);
        std::vector<int> selected_parts;
        switch ((count_product_parts / num_regions_) % 4) {
          case 0:
            // Category 1: same region, same partition
            for (int i = 1; i <= pps::kPartsPerProduct; i++) {
              int part_id = chooseRandomPart(product_region, local_partition_);
              CHECK(part_id > 0 && part_id <= num_parts_) << "Invalid part id: " << part_id;
              selected_parts.push_back(part_id);
              productPartsTable.Insert({
                MakeInt32Scalar(product_id),
                MakeInt32Scalar(i),
                MakeInt32Scalar(part_id)
              });
            }
            break;
            
          case 1:
            // Category 2: same region, different partitions
            std::shuffle(remote_partitions_.begin(), remote_partitions_.end(), rg_);
            for (int i = 1; i <= pps::kPartsPerProduct; i++) {
              int chosen_partition_index = std::uniform_int_distribution<int>(0, max_partitions_ - 1)(rg_);
              int chosen_partition = (chosen_partition_index == max_partitions_ - 1 ?
                local_partition_ : remote_partitions_[chosen_partition_index]);
              int part_id = chooseRandomPart(product_region, chosen_partition);
              CHECK(part_id > 0 && part_id <= num_parts_) << "Invalid part id: " << part_id;
              selected_parts.push_back(part_id);
              productPartsTable.Insert({
                MakeInt32Scalar(product_id),
                MakeInt32Scalar(i),
                MakeInt32Scalar(part_id)
              });
            }
            break;

          case 2:
            // Category 3: different regions, same partition
            std::shuffle(remote_regions_[product_region].begin(), remote_regions_[product_region].end(), rg_);
            for (int i = 1; i <= pps::kPartsPerProduct; i++) {
              int chosen_region_index = std::uniform_int_distribution<int>(0, max_regions_ - 1)(rg_);
              int chosen_region = (chosen_region_index == max_regions_ - 1 ?
                product_region : remote_regions_[product_region][chosen_region_index]);
              int part_id = chooseRandomPart(chosen_region, local_partition_);
              CHECK(part_id > 0 && part_id <= num_parts_) << "Invalid part id: " << part_id;
              selected_parts.push_back(part_id);
              productPartsTable.Insert({
                MakeInt32Scalar(product_id),
                MakeInt32Scalar(i),
                MakeInt32Scalar(part_id)
              });
            }
            break;

          case 3:
            // Category 4: different regions, different partitions
            std::shuffle(remote_partitions_.begin(), remote_partitions_.end(), rg_);
            std::shuffle(remote_regions_[product_region].begin(), remote_regions_[product_region].end(), rg_);
            for (int i = 1; i <= pps::kPartsPerProduct; i++) {
              int chosen_partition_index = std::uniform_int_distribution<int>(0, max_partitions_ - 1)(rg_);
              int chosen_partition = (chosen_partition_index == max_partitions_ - 1 ?
                local_partition_ : remote_partitions_[chosen_partition_index]);
              int chosen_region_index = std::uniform_int_distribution<int>(0, max_regions_ - 1)(rg_);
              int chosen_region = (chosen_region_index == max_regions_ - 1 ?
                product_region : remote_regions_[product_region][chosen_region_index]);
              int part_id = chooseRandomPart(chosen_region, chosen_partition);
              CHECK(part_id > 0 && part_id <= num_parts_) << "Invalid part id: " << part_id;
              selected_parts.push_back(part_id);
              productPartsTable.Insert({
                MakeInt32Scalar(product_id),
                MakeInt32Scalar(i),
                MakeInt32Scalar(part_id)
              });
            }
            break;
        }
        LOG(INFO) << "The product " << showIdWithPartitionAndRegion(product_id) << " has the parts " << showChosenParts(selected_parts);
        count_product_parts++;
      }
    }

    // Add the supplier-to-parts mapping
    LOG(INFO) << "Generating ~" << num_suppliers_ * pps::kPartsPerSupplier / num_partitions_ << " supplier-to-parts for the thread " << thread_index << "/" << num_threads_;
    Table<SupplierPartsSchema> supplierPartsTable(storage_adapter_);
    std::vector <int> part_ids(num_parts_);
    std::iota(part_ids.begin(), part_ids.end(), 1);
    for (uint32_t supplier_id = 1; supplier_id <= num_suppliers_; supplier_id++) {
      if (computePartition(supplier_id) == local_partition_) {
        LOG(INFO) << "Load the supplier-to-parts with id " << supplier_id;
        std::shuffle(part_ids.begin(), part_ids.end(), rg_);
        for (int i = 1; i <= pps::kPartsPerSupplier; i++) {
          supplierPartsTable.Insert({
              MakeInt32Scalar(supplier_id),
              MakeInt32Scalar(i),
              MakeInt32Scalar(part_ids[i])
          });
        }
      }
    }
  }

 private:
  int chooseRandomPart(int chosen_region, int chosen_partition) {
    int normalized_part_id = num_partitions_ * chosen_region + chosen_partition + 1;
    int part_index_within_class = std::uniform_int_distribution<int>(1, num_parts_per_class_)(rg_);
    return (part_index_within_class - 1) * num_partitions_ * num_regions_ + normalized_part_id;
  };

  int computePartition(int id) const {
    return (id - 1) % num_partitions_;
  }

  int computeRegion(int id) const {
    return (id - 1) / num_partitions_ % num_regions_;
  }

  std::string showIdWithPartitionAndRegion(int id) const {
    return std::to_string(id) + "(" + std::to_string(computeRegion(id)) + "," + std::to_string(computePartition(id)) + ")";
  }

  std::string showChosenParts(const std::vector<int>& chosen_parts) const {
    if (chosen_parts.empty()) {
      return "[]";
    }
    std::ostringstream oss;
    oss << "[" << showIdWithPartitionAndRegion(chosen_parts[0]);
    for (size_t i = 1; i < chosen_parts.size(); i++) {
      oss << ", " << showIdWithPartitionAndRegion(chosen_parts[i]);
    }
    oss << "]";
    return oss.str();
  }

  int num_products_;
  int num_parts_;
  int num_suppliers_;
  int num_partitions_;
  int num_regions_;

  // The table maintained by a database node is dependent only on the partition, not on the region.
  int local_partition_;
  std::vector<std::vector<int>> remote_regions_;
  std::vector<int> remote_partitions_;

  int max_regions_;
  int max_partitions_;

  int num_threads_;

  /*
   * We define as class the combination of a region and a partition. So, the number of classes is num_partitions * num_regions.
   * The number of parts per class is the number of parts divided by the number of classes (we round for simplicity).
   * For example, if we have 3 regions and 4 partitions, the class (0, 2) will contain the parts from
   * the region 0 and the partition 2 => [3, 15, 27, ...]
   * 
   * partition / region |  0  |  1  |  2  |  0  |  1  |  2  |  0  |  1  |  2
   * -------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----
   *           0        |  1  |  5  |  9  | 13  | 17  | 21  | 25  | 29  | 33
   *           1        |  2  |  6  | 10  | 14  | 18  | 22  | 26  | 30  | 34
   *           2        |  3  |  7  | 11  | 15  | 19  | 23  | 27  | 31  | 35
   *           3        |  4  |  8  | 12  | 16  | 20  | 24  | 28  | 32  | 36
   * ---------------------------------- parts ids ----------------------------
   */
  int num_parts_per_class_;

  std::mt19937 rg_;
  RandomStringGenerator str_gen_;

  StorageAdapterPtr storage_adapter_;
};

void LoadTables(const StorageAdapterPtr& storage_adapter, int num_products, int num_parts, int num_suppliers, 
  int num_regions, int num_partitions, int local_partition, int max_regions, int max_partitions, int num_threads) { 
    // [pps] TODO: make it parallel by using multiple PartitionedPPSDataLoaders
    PartitionedPPSDataLoader data_loader(storage_adapter, num_products, num_parts, num_suppliers, 
        num_regions, num_partitions, local_partition, max_regions, max_partitions, num_threads, local_partition);
    data_loader.Load(1);
}

}  // namespace pps
}  // namespace slog