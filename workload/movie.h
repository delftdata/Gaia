#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "execution/tpcc/constants.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog{
class MovieWorkload : public Workload {
 public:
  MovieWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& params_str,
               std::pair<int, int> id_slot, const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();
 private:
  int local_region() { return config_->num_regions() == 1 ? local_replica_ : local_region_; }
  void NewReview(Transaction& txn, TransactionProfile& pro, bool sunflower, int sunflowerhome, bool multihome, bool multipart);
  ConfigurationPtr config_;
  RegionId local_region_;
  ReplicaId local_replica_;
  std::mt19937 rg_;
  TxnId client_txn_id_counter_;
  double skew_;
  int calculatehome(int id);
  int calculatepart(int id);
  long random_id_for_home(int h_req, long MAX_ID);
  long same_home_same_part(long id, long MAX_ID);
  long same_home_diff_part(long id, long MAX_ID);
  long diff_home_same_part(long id, long MAX_ID);
  long diff_home_diff_part(long id, long MAX_ID);
};
}