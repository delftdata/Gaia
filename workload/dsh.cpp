#include "workload/dsh.h"

#include <glog/logging.h>

#include <random>
#include <set>
#include <fstream>
#include <iostream>
#include <variant>
#include <filesystem>

#include "common/proto_utils.h"
#include "execution/dsh/utils.h"
#include "execution/dsh/transaction.h"
#include "execution/dsh/storage_adapter.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::to_string;
using std::unordered_set;


namespace slog {

namespace {
constexpr char TXN_MIX[] = "mix";               // txn mix (Search, recommend, login, reserve)
constexpr char HOT[] = "hot";                   // size of hot record set as a fraction of record set -> 0.01 means 1% of records are in the hot set
constexpr char HOT_CHANCE[] = "hot_chance";     // chance of a record being from the hot record set (skew is disabled if this is 0)
constexpr char MH_CHANCE[] = "mh";              // chance of MH txn
constexpr char MP_CHANCE[] = "mp";              // chance of MP txn
constexpr char SUNFLOWER_FILE[] = "sf";         // filepath to sunflower file
constexpr char DURATION[] = "duration";         // total # of txns pre-generated

const RawParamMap DEFAULT_PARAMS = {{TXN_MIX, "120:68:1:1"}, {HOT, "-1.0"}, {HOT_CHANCE, "0.0"}, {MH_CHANCE, ".25"}, {MP_CHANCE, ".25"}, {SUNFLOWER_FILE, ""}, {DURATION, "60"}};

int GetNumRegions(const ConfigurationPtr& config) {
    return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

struct date {
    uint32_t d, m ,y;
};

/// @brief Generates a random date range between the start an end dates
/// @param start first day of range
/// @param end last day of range
/// @param rg random generator
/// @return a pair of strings which hold the start and end days of the generated range in dd-mm-yyyy format
std::pair<std::string, std::string> rand_date_range_from_range(date start, date end, std::mt19937 rg) {
    // generate year
    auto y = std::uniform_int_distribution<uint32_t>(start.y, end.y)(rg);
    uint32_t start_m = 1, end_m = 12;
    // make sure if we are in a start or end year the month range is correct, otherwise can do any month from 1-12
    if (y == start.y) {
        start_m = start.m;
    }
    if (y == end.y) {
        end_m = end.m;
    }
    // generate month
    auto m = std::uniform_int_distribution<uint32_t>(start_m, end_m)(rg);
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int start_d = 1, end_d = days_in_month[m - 1];
    // make sure if we are in the starting month or ending month of the range we get the right day selection
    if (y == start.y && m == start.m) {
        start_d = start.d;
    }
    if (y == end.y && m == end.m) {
        end_d = end.d;
    }
    // generate day
    auto d = std::uniform_int_distribution<uint32_t>(start_d, end_d)(rg);
    // generate length of stay
    auto l = std::uniform_int_distribution<uint32_t>(1, static_cast<int>(dsh::kMaxStay))(rg);
    // math to calculate correct ending date
    auto d_l = d + l;
    int m_l = m, y_l = y;
    if (d_l > days_in_month[m - 1]) {
        d_l -= days_in_month[m - 1];
        m_l++;
        if(m_l > 12) {
            m_l = 1;
            y_l++;
        }
    }
    return {dsh::format_date(d, m, y), dsh::format_date(d_l, m_l, y_l)};
}




uint32_t login_cnt = 0;
uint32_t search_cnt = 0;
uint32_t recommendation_cnt = 0;
uint32_t reservation_cnt = 0;

uint32_t total_txn_count = 0;
uint32_t mh_cnt = 0;


} //namespace


DeathStarHotelWorkload::DeathStarHotelWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& params_str,
    std::pair<int, int> id_slot, const uint32_t seed) 
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      distance_ranking_(config->distance_ranking_from(region)),
      rg_(seed),
      client_txn_id_counter_(0),
      dsh_config_(config->proto_config().dsh_partitioning()) {

    auto num_regions = GetNumRegions(config_);
    auto num_partitions = config_->num_partitions();

    // load sunflower parameters if we got a file
    if (sunflower_active_ = (params_.GetString(SUNFLOWER_FILE) != "")) {
        load_sunflower();
    }

    // load skew parameters if there is a chance of accessing a hot record
    hot_chance_ = params_.GetDouble(HOT_CHANCE);
    if (hot_active_ = (hot_chance_ > 0.0)) {
        load_skew();
    }

    // allocate space for all users/hotels
    for (int i = 0; i < num_partitions; i++) {
        vector<vector<uint32_t>> partitions(num_regions);
        u_index_.push_back(partitions);
        h_index_.push_back(partitions);
    }

    // add all users to the index according to calculated home + partition
    auto num_users = dsh_config_.num_users();
    for (uint32_t i = 0; i < num_users; i++) {
        int partition = i % num_partitions;
        int home = (i - 1) / num_partitions % num_regions;
        u_index_[partition][home].push_back(i);
    }

    // add all hotels to the index according to calculated home + partition
    auto num_hotels = dsh_config_.num_hotels();
    for (uint32_t i = 0; i < num_hotels; i++) {
        int partition = i % num_partitions;
        int home = (i - 1) / num_partitions % num_regions;
        h_index_[partition][home].push_back(i);
    }

    // parse the txn mix
    auto txn_mix_str = Split(params_.GetString(TXN_MIX), ":");
    CHECK_EQ(txn_mix_str.size(), 4u) << "There must be exactly 4 values for txn mix";
    for (const auto& t : txn_mix_str) {
      txn_mix_.push_back(std::stoi(t));
    }

    // save mh/mp chance for quick access
    mh_chance_ = params_.GetDouble(MH_CHANCE);
    mp_chance_ = params_.GetDouble(MP_CHANCE);

    // in the sunflower scenario, we need to measure time passed. This is super good enough.
    if (sunflower_active_) {
        start_time_ = std::chrono::system_clock::now();
    }
}


std::pair <Transaction*, TransactionProfile> DeathStarHotelWorkload::NextTransaction() {
    TransactionProfile pro;
    pro.client_txn_id = client_txn_id_counter_;
    pro.is_multi_partition = false;
    pro.is_multi_home = false;
    pro.is_foreign_single_home = false;

    Transaction* txn = new Transaction();
    std::discrete_distribution<> select_next_txn(txn_mix_.begin(), txn_mix_.end());

    switch (select_next_txn(rg_)) {
        case 0:
            search_hotel(*txn, pro);
            search_cnt++;
            break;
        case 1:
            get_recommendation(*txn, pro);
            recommendation_cnt++;
            break;
        case 2:
            user_login(*txn);
            login_cnt++;
            break;
        case 3:
            reserve_hotel(*txn, pro);
            reservation_cnt++;
            break;
        default:
            LOG(FATAL) << "Invalid txn choice";
    }

    if (pro.is_multi_home) {
        mh_cnt++;
    }
    txn->mutable_internal()->set_id(client_txn_id_counter_);
    client_txn_id_counter_++;


    if (++total_txn_count % 100000 == 0) {
        LOG(INFO) << "total txn count: " << total_txn_count;
        // LOG(INFO) << "number of user login txns: " << login_cnt;
        // LOG(INFO) << "number of search txns: " << search_cnt;
        // LOG(INFO) << "number of recommendation txns: " << recommendation_cnt;
        // LOG(INFO) << "number of reservation txns: " << reservation_cnt; 
        // LOG(INFO) << "number of mh txns: " << mh_cnt;
    }

    return {txn, pro};
}


void DeathStarHotelWorkload::user_login(Transaction& txn) {
    auto txn_adapter = std::make_shared<dsh::TxnKeyGenStorageAdapter>(txn);
    
    auto partition = std::uniform_int_distribution(0, config_->num_partitions() - 1)(rg_);
    // select a region which is not the local region
    auto region = std::uniform_int_distribution(0, GetNumRegions(config_) - 2)(rg_);
    region = (region >= local_region()) ? region : (region + 1);
    // if MH we make this txn FSH
    const auto& selectable_u = h_index_[partition][std::bernoulli_distribution(mh_chance_)(rg_) ? region : local_region()];
    CHECK(!selectable_u.empty()) << "Not enough users";
    
    std::string uname = std::to_string(sample_once(selectable_u, num_hot_users_));

    dsh::UserLoginTxn login_txn(txn_adapter, uname, uname);
    login_txn.Execute();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("user login");
    procedure->add_args(dsh::format_uname(uname));
    procedure->add_args(uname);

}

void DeathStarHotelWorkload::search_hotel(Transaction& txn, TransactionProfile& pro) {
    auto txn_adapter = std::make_shared<dsh::TxnKeyGenStorageAdapter>(txn);

    std::vector<uint32_t> hotel_sample = sample_(h_index_, num_hot_hotels_, dsh::kRecommendationReadSize, pro);
    
    std::uniform_real_distribution<> coord_rand(0, static_cast<int>(dsh_config_.max_coord()) - 1);
    auto dates = rand_date_range_from_range({1, 1, 2020}, {31, 6, 2020}, rg_);
    double lat = coord_rand(rg_), lon = coord_rand(rg_);

    dsh::SearchTxn search_txn(txn_adapter, dates.first, dates.second, lat, lon, hotel_sample.begin(), hotel_sample.end());
    search_txn.Execute();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("search");
    procedure->add_args(dates.first);
    procedure->add_args(dates.second);
    procedure->add_args(std::to_string(lat));
    procedure->add_args(std::to_string(lon));    
}

void DeathStarHotelWorkload::get_recommendation(Transaction& txn, TransactionProfile& pro) {
    auto txn_adapter = std::make_shared<dsh::TxnKeyGenStorageAdapter>(txn);

    std::vector<uint32_t> hotel_sample = sample_(h_index_, num_hot_hotels_,  dsh::kRecommendationReadSize, pro);

    std::uniform_int_distribution<uint32_t> type_rand(0, 2);
    auto type = type_rand(rg_);
    double lat = 0.0, lon = 0.0;

    if (type == dsh::RecommendTxn::DISTANCE) {
        std::uniform_real_distribution<> coord_rand(0, static_cast<int>(dsh_config_.max_coord()) - 1);
        lat = coord_rand(rg_);
        lon = coord_rand(rg_);
    }

    dsh::RecommendTxn recommendation_txn(txn_adapter, static_cast<dsh::RecommendTxn::RecommendationType>(type), lat, lon, hotel_sample.begin(), hotel_sample.end());
    recommendation_txn.Execute();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("recommendation");
    procedure->add_args("type: " + std::to_string(type));
    procedure->add_args(std::to_string(lat));
    procedure->add_args(std::to_string(lon));
}

void DeathStarHotelWorkload::reserve_hotel(Transaction& txn, TransactionProfile& pro) {
    auto txn_adapter = std::make_shared<dsh::TxnKeyGenStorageAdapter>(txn);

    bool is_mh = pro.is_multi_home = std::bernoulli_distribution(mh_chance_)(rg_);
    bool is_mp = pro.is_multi_partition = std::bernoulli_distribution(mp_chance_)(rg_);

    uint32_t h_partition, u_partition;
    uint32_t h_region = local_region(), u_region = local_region();

    // I wish there were a better way to do the following but its just not that good because of the local region requirement
    if (is_mp) {
        std::vector<uint32_t> regions(config_->num_partitions());
        std::iota(regions.begin(), regions.end(), 0);
        std::shuffle(regions.begin(), regions.end(), rg_);
        h_partition = regions[0];
        u_partition = regions[1 % config_->num_partitions()]; // make sure not to segfault on 1 home MH txn (this shouldn't happen anyway)
    } else {
        h_partition = u_partition = std::uniform_int_distribution<uint32_t>(0, config_->num_partitions() - 1)(rg_);
    }

    if (is_mh) {
        std::vector<uint32_t> regions(GetNumRegions(config_));
        std::iota(regions.begin(), regions.end(), 0);
        std::shuffle(regions.begin(), regions.end(), rg_);
        h_region = regions[0];
        u_region = regions[1 % GetNumRegions(config_)]; // make sure not to segfault on 1 home MH txn (this shouldn't happen anyway)
    } 

    const auto& selectable_h = h_index_[h_partition][h_region];
    const auto& selectable_u = u_index_[u_partition][u_region];

    uint32_t user_id = sample_once(selectable_u, num_hot_users_);
    uint32_t hotel_id = sample_once(selectable_h, num_hot_hotels_);

    std::string uname = std::to_string(user_id);

    auto dates = rand_date_range_from_range({1, 1, 2020}, {31, 6, 2020}, rg_);
    uint32_t num_rooms = std::uniform_int_distribution<uint32_t>(0, 4)(rg_);

    dsh::ReservationTxn reservation_txn(txn_adapter, uname, uname, dates.first, dates.second, hotel_id, uname, num_rooms);
    reservation_txn.Execute();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("reservation");
    procedure->add_args(dsh::format_uname(uname));
    procedure->add_args(uname);
    procedure->add_args(dates.first);
    procedure->add_args(dates.second);
    procedure->add_args(std::to_string(hotel_id));
    procedure->add_args(uname);
    procedure->add_args(std::to_string(num_rooms));
}

/// @brief custom sample n function should give good enough results and handles mh + mp + skew + sunflower implementation cleanly
/// @tparam T type of source vector
/// @param source source vector to take samples from
/// @param hot_cnt how many IDs are hot records
/// @param cnt how many results
/// @param pro reference to txn profile to update efficiently
/// @return cnt samples of source with appropriate mh and mp percentages
template <typename T>
std::vector<T> DeathStarHotelWorkload::sample_(std::vector<std::vector<std::vector<T>>> source, size_t hot_cnt,  size_t cnt, TransactionProfile& pro) {
    std::vector<T> rval(cnt);
    bool is_mh = pro.is_multi_home = std::bernoulli_distribution(mh_chance_)(rg_);
    bool is_mp = pro.is_multi_partition = std::bernoulli_distribution(mp_chance_)(rg_);
    std::bernoulli_distribution hot_dist(hot_chance_);

    enum TXN_TYPE {
        NONE,
        MP_ONLY,
        MH_ONLY,
        BOTH
    };

    std::variant<std::uniform_int_distribution<uint32_t>, std::discrete_distribution<uint32_t>> region_rand;
    std::uniform_int_distribution<uint32_t> partition_rand(0, config_->num_partitions() - 1);
    
    // different distributions for different scenarios. discrete distribution is a loooooot slower 
    // hence the use of std::variant for all the rest of the scenarios
    if(sunflower_active_) {
        // auto ms_since_start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start_time_).count();
        // LOG(INFO) << "ms since start" << ms_since_start;
        auto percent_of_exp = static_cast<float>(total_txn_count) / static_cast<float>(params_.GetInt32(DURATION)) ;
        // check time here
        if (sf_weights_.size() > current_sf_index_ + 1 && percent_of_exp > sf_weights_[current_sf_index_].first ) {
            current_sf_index_++;
            LOG(INFO) << "new SF index " << current_sf_index_ << " with time " << sf_weights_[current_sf_index_].first << " at txnc " << total_txn_count << "(" << percent_of_exp << "%)";
        }
        region_rand = std::discrete_distribution<uint32_t> (sf_weights_[current_sf_index_].second.begin(), sf_weights_[current_sf_index_].second.end());
    } else {
        region_rand = std::uniform_int_distribution<uint32_t> (0, GetNumRegions(config_) - 1);
    }

    uint32_t txn_local_partition = partition_rand(rg_);
    uint32_t txn_local_home = local_region();

    // either we take the given hot record size, or we increase until we can sample unique values
    size_t hot_record_size = std::max(hot_cnt, cnt);

    // we should only shuffle the hot records if we need to 
    if (hot_active_) {
        for (auto& x : source) {
            for (auto& y : x) {
                std::shuffle(y.begin(), y.begin() + hot_record_size, rg_);
            }
        }
    }

    // maybe this should go in the for loop but the compiler will figure it out
    uint32_t partition = txn_local_partition, home = txn_local_home;
    TXN_TYPE type = static_cast<TXN_TYPE>(static_cast<uint8_t>(is_mh) << 1 | static_cast<uint8_t>(is_mp));
    for (size_t i = 0; i < cnt; i++) {
        // pick a region/partition based on if the txn is MH or MP. 
        switch (type) {
            case TXN_TYPE::NONE:
                break;
            case TXN_TYPE::MP_ONLY:
                partition = partition_rand(rg_);
                break;
            case TXN_TYPE::MH_ONLY:
                home = std::visit([this](auto&& d) { return d(this->rg_); }, region_rand);
                break;
            case TXN_TYPE::BOTH:
                partition = partition_rand(rg_);
                home = std::visit([this](auto&& d) { return d(this->rg_); }, region_rand);
                break;
        }
        if (hot_active_ && hot_dist(rg_)) {
            // we shuffled the hot records before -- this is because during high skew duplicates are more likely
            rval[i] = source[partition][home][i];
        } else {
            // yes theoretically theres a chance of duplicates from the sampling but that chance is pretty low -- performance is better here
            auto x = std::uniform_int_distribution<size_t>(hot_record_size, source[partition][home].size() - 1)(rg_);
            rval[i] = source[partition][home][x];
        }
    }
    return rval;
}

template <typename T>
T DeathStarHotelWorkload::sample_once(std::vector<T> source, size_t hot_cnt) {
    std::bernoulli_distribution hot_dist(hot_chance_);
    if (hot_active_ && hot_dist(rg_)) {
        return source[std::uniform_int_distribution<size_t>(0, std::max(1ul, hot_cnt - 1ul))(rg_)];
    } else {
        return source[std::uniform_int_distribution<size_t>(hot_cnt - 1, source.size())(rg_)];
    }
}


/** 
 * 
 * Parses a CSV file which contains times from 0-1 (increasing), and weights for each region afterward.
 * Weights can sum to any number. Passed with a duration parameter for accurate timing.
 *
 * Suppose transaction n is being generated. If the fraction (n/total) is greater than the time of the current weights, the 
 * currently selected weights will be incremented (meaning in the example the first 10% of transactions will use the weights .5, .5, the next 40% use .2, .8, etc.)
 * 
 * Consequently, a file must always have a row with time 1.0 (or greater) or there can be unexpected behavior
 * 
 * example file contents:
 * 0.1,.5,.5
 * 0.5,.2,.8
 * 0.7,.5,.5
 * 1.0,.2,.8
 */
void DeathStarHotelWorkload::load_sunflower() {

    auto num_regions = GetNumRegions(config_);
    std::ifstream sf_file(params_.GetString(SUNFLOWER_FILE).data());
    std::string line;

    while (getline (sf_file, line)) {
        auto values = Split(line, ",");
        if (values.size() != num_regions + 1) { 
            LOG(FATAL) << "Invalid number of regions in sunflower config";
        }
        std::vector<double> temp;
        for (size_t i = 0u; i < num_regions; i++) {
            temp.push_back(std::stod(values[i]));
        }
        // first weight is time to , next n are region weights
        sf_weights_.push_back({std::stod(values[0]), temp});
    }
    LOG(INFO) << "SF loading complete";
}

/**
 *  Does all relevant calculations for skewed scenario
 */
void DeathStarHotelWorkload::load_skew() {
    auto hot_percentage = std::max(params_.GetDouble(HOT), 0.0);
    double num_machines = GetNumRegions(config_) * config_->num_partitions();

    num_hot_hotels_ = static_cast<uint32_t>(hot_percentage * static_cast<double>(dsh_config_.num_hotels()) / num_machines);
    LOG(INFO) << "num hot hotels: " << num_hot_hotels_;

    num_hot_users_ = static_cast<uint32_t>(hot_percentage * static_cast<double>(dsh_config_.num_users()) / num_machines);
    LOG(INFO) << "num hot users per machine: " << num_hot_users_;

    if (num_hot_hotels_ < dsh::kRecommendationReadSize) {
        LOG(WARNING) << "Not enough hot hotels for a full read, skew is slightly inaccurate"; 
    }

}

} //namespace slog