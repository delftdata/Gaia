#pragma once

#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

class SmallBankWorkload : public Workload {
 public:
  SmallBankWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& params_str,
                    std::pair<int, int> id_slot, const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();

  void GetCustomerIdByName(Transaction& txn, TransactionProfile& pro, int choice,
                           const std::string& override_account_name = "");

  bool IsSunflowerEnabled() const;
  std::string PickAccountName(int choice);

  void Balance(Transaction& txn, TransactionProfile& pro, int phase);
  void DepositChecking(Transaction& txn, TransactionProfile& pro, int phase);
  void TransactionSaving(Transaction& txn, TransactionProfile& pro, int phase);
  void Amalgamate(Transaction& txn, TransactionProfile& pro, int phase);
  void Writecheck(Transaction& txn, TransactionProfile& pro, int phase);

  void refreshSunflowerRegions(int64_t duration, int64_t elapsed_time) override;

  int returned_first_customer_id;

  int am_returned_first_customer_id;
  int am_returned_second_customer_id;
  int sunflower_current_region_index_;

  std::string amalgamate_src_;
  std::string amalgamate_dst_;

  Transaction* pending_balance_txn_;
  Transaction* pending_deposit_txn_;
  Transaction* pending_saving_txn_;
  Transaction* pending_writecheck_txn_;

  Transaction* pending_amalgamate_txn_;
  Transaction* previous_amalgamate_txn_;

 private:
  int local_region() { return config_->num_regions() == 1 ? local_replica_ : local_region_; }

  ConfigurationPtr config_;
  RegionId local_region_;
  ReplicaId local_replica_;
  std::vector<int> distance_ranking_;
  int zipf_coef_;
  // _warehouse vector has dimensions: partition (currently 2), home/home (2?, i.e., number of 'regions' blocks in the
  // .conf file), and then a list of warehouses that are based there
  vector<vector<vector<std::string>>> account_names_;
  std::unordered_map<int, std::string> client_names_by_id_;

  vector<vector<std::string>> sh_sp_accounts_by_region_;
  vector<vector<std::string>> sh_mp_accounts_by_region_;

  vector<std::string> mh_mp_account_names_;
  vector<std::string> mh_sp_account_names_;

  vector<vector<vector<int>>> client_partition_map_;
  std::mt19937 rg_;
  TxnId client_txn_id_counter_;
  std::vector<int> txn_mix_;
  std::vector<int> region_mix_;
  std::vector<int> probability_mix_;
  std::vector<int> sunflower_sent_regions;
  int phase_ = 0;
};

}  // namespace slog