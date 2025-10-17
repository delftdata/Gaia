#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "execution/pps/constants.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

class PPSWorkload : public Workload {
public:
  PPSWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& params_str,
               std::pair<int, int> id_slot, const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();
  void printStatistics() const;
  void updateForSunflowerScenario(int64_t duration, int64_t elapsed_time);

  // For dependent transactions, we need to results of the first phase to be used in the second phase.
  Transaction* prev_txn_;
  vector<int> parts_to_retrieve_;
  private:
  int local_region() { return config_->num_regions() == 1 ? local_replica_ : local_region_; }
  
  void orderProductTransaction(Transaction& txn, TransactionProfile& pro, int product_id);
  void getPartsByProductTransaction(Transaction& txn, TransactionProfile& pro, bool is_part_of_order_product = false);
  void updateProductPartTable(Transaction& txn, TransactionProfile& pro);
  void getProductTransaction(Transaction& txn, TransactionProfile& pro);
  void getPartTransaction(Transaction& txn, TransactionProfile& pro);
  
  ConfigurationPtr config_;
  std::pair<int, int> id_slot_;
  int num_regions_;
  int num_partitions_;
  RegionId local_region_;
  ReplicaId local_replica_;
  int sunflower_redirect_pct_;
  int sunflower_target_region_;

  int num_products_;
  int num_parts;
  int num_suppliers_;
  
  std::mt19937 rg_;
  TxnId client_txn_id_counter_;

private:
  int selectProduct();

  // Statistics for the PPS workload that will be printed at the end of the benchmark.
  int txn_total_ = 0;
  int order_product_1st_phase_total_ = 0;
  std::array<int, 4> order_product_2nd_phase_cateogry_total_ = {0, 0, 0, 0};
  int get_parts_by_product_total_ = 0;
  int update_product_part_total_ = 0;
  int get_product_total_ = 0;
  int get_part_total_ = 0;
};

}  // namespace slog