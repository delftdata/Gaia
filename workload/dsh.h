# pragma once

#include <vector>
#include <cstdint>
#include <chrono>

#include "common/configuration.h"
#include "common/types.h"
#include "execution/tpcc/constants.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

class DeathStarHotelWorkload: public Workload {
public:
    DeathStarHotelWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& params_str,
        std::pair<int, int> id_slot, const uint32_t seed = std::random_device()());
    std::pair<Transaction*, TransactionProfile> NextTransaction();

private:
    int local_region() { 
        if (sunflower_active_) {
            return std::discrete_distribution<int>(sf_weights_[current_sf_index_].second.begin(), sf_weights_[current_sf_index_].second.end())(rg_);
        }
        return config_->num_regions() == 1 ? local_replica_ : local_region_; 
    }

    void user_login(Transaction& txn);
    void search_hotel(Transaction& txn, TransactionProfile& pro);
    void get_recommendation(Transaction& txn, TransactionProfile& pro);
    void reserve_hotel(Transaction& txn, TransactionProfile& pro);

    template <typename T>
    std::vector<T> sample_(std::vector<std::vector<std::vector<T>>> source, size_t hot_cnt, size_t cnt, TransactionProfile& pro);
    template <typename T>
    T sample_once(std::vector<T> source, size_t hot_cnt);

    void load_sunflower();
    void load_skew();

    ConfigurationPtr config_;
    RegionId local_region_;
    ReplicaId local_replica_;
    std::vector<int> distance_ranking_;
    std::mt19937 rg_;
    TxnId client_txn_id_counter_;
    std::vector<int> txn_mix_;
    internal::DSHPartitioning dsh_config_;

    double mh_chance_, mp_chance_, hot_chance_;
    bool sunflower_active_, hot_active_;
  
    std::vector<std::vector<std::vector<uint32_t>>> u_index_;
    size_t num_hot_users_ = 0, num_hot_hotels_ = 0;
    std::vector<std::vector<std::vector<uint32_t>>> h_index_;

    std::vector<double> region_uniform_weights_;
    std::vector<std::pair<double, std::vector<double>>> sf_weights_;
    size_t current_sf_index_ = 0;
    std::chrono::system_clock::time_point start_time_;

};

}