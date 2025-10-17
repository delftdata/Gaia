#include "workload/movr.h"

#include <glog/logging.h>

#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "common/proto_utils.h"
#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"
#include "execution/movr/data_generator.h"
#include "movr.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::string;
using std::unordered_set;

using slog::movr::DataGenerator;
using slog::movr::kDefaultUsers;
using slog::movr::kDefaultVehicles;
using slog::movr::kDefaultRides;
using slog::movr::kDefaultCities;

namespace slog {
namespace {

// Existing parameters (copied from TPCC)
constexpr char PARTITION[] = "partition"; // Use specific partition, -1 for random
constexpr char HOMES[] = "homes";         // Max number of regions accessed in a multi-home txn
constexpr char MH_ZIPF[] = "mh_zipf";     // Zipf coefficient for selecting remote regions
constexpr char TXN_MIX[] = "txn-mix";     // Colon-separated percentages for MovR txn types
constexpr char SH_ONLY[] = "sh-only";     // Force single-home transactions

// MovR specific parameters         
constexpr char MULTI_HOME_PCT[] = "mh";                   // Percentage of transactions that are multi-home (0-100)
constexpr char MULTI_PARTITION_PCT[] = "mp";              // Percentage of transactions that are multi-partition (0-100)
constexpr char SKEW[] = "skew";                           // Skew factor for record access (e.g., Zipf theta, 0 = uniform)
constexpr char REGION_REQUEST_MIX[] = "reg-mix";          // Colon-separated percentages for request origins per region
constexpr char SUNFLOWER_MAX[] = "sunflower-max";         // Max % of txn from peak region (e.g. 50)
constexpr char SUNFLOWER_FALLOFF[] = "sunflower-falloff"; // Falloff (0.0 to 1.0)
constexpr char SUNFLOWER_CYCLES[] = "sunflower-cycles";   // Number of times the 'sun' orbits each region during the experiment

// Default values for MovR parameters
const RawParamMap DEFAULT_PARAMS =
  {{PARTITION, "-1"},
  {HOMES, "2"},
  {MH_ZIPF, "0"},
  {TXN_MIX, "30:5:5:15:30:15"}, // Example Mix: ViewVehicles: 40%, UserSignup: 5%, AddVehicle: 5%, StartRide: 30%, UpdateLocation: 15%, EndRide: 5%
  {SH_ONLY, "0"},
  {MULTI_HOME_PCT, "10"},       // Default percentage of multi-home transactions
  {MULTI_PARTITION_PCT, "10"},  // Default percentage of multi-partition transactions
  {SKEW, "0.0"},                // Default uniform access ( 0 = no contention)
  {REGION_REQUEST_MIX, ""},
  {SUNFLOWER_MAX, "40"},        // Default transactions % originating from peak region
  {SUNFLOWER_FALLOFF, "0.0"},
  {SUNFLOWER_CYCLES, "1"}};     // Default number of cycles

// ID generation constants from data loader
constexpr int kPartitionBits = 16;
constexpr int kMaxUsersPerCity = kDefaultUsers / kDefaultCities;
constexpr int kMaxVehiclesPerCity = kDefaultVehicles / kDefaultCities;
constexpr int kMaxRidesPerCity = kDefaultRides / kDefaultCities;

// Counters (for debugging/logging)
int view_vehicle_count = 0;
std::atomic<uint64_t> user_signup_count = 0;
std::atomic<uint64_t> add_vehicle_count = 0;
std::atomic<uint64_t> start_ride_count = 0;
std::atomic<uint64_t> update_location_count = 0;
std::atomic<uint64_t> end_ride_count = 0;
std::atomic<uint64_t> total_txn_count = 0;
std::atomic<uint64_t> multi_home_count = 0;
std::atomic<uint64_t> multi_partition_count = 0;

// Helper: Get number of regions (copied from TPCC)
int GetNumRegions(const ConfigurationPtr& config) {
  return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

/**
  * Generates a globally unique 64-bit ID by combining a city index and local record ID.
  * 
  * The ID is structured as:
  * [ City/Partition Identifier ][ Local Record ID ]
  *   MSB ----------------------- LSB
  * 
  * @param city_index    The index of the city/partition (uses upper bits)
  * @param local_id      The local record ID (uses lower bits)
  * @param partition_bits Number of bits to reserve for city/partition (default: 16)
  *                      - Determines max cities: 2^partition_bits
  *                      - Determines max records per city: 2^(64-partition_bits)
  * @return uint64_t     The generated globally unique ID
  * 
  * Example with default 16 partition bits:
  *   - City index 5 (0x0005) and local ID 123456 (0x0001E240)
  *   - Generated ID: 0x000500000001E240
  *   - Structure: [16-bit city][48-bit local ID]
  */
uint64_t GenerateGlobalId(int city_index, uint64_t local_id) {
  return (static_cast<uint64_t>(city_index) << (64 - kPartitionBits)) | (local_id & ((1ULL << (64 - kPartitionBits)) - 1));
}

int GetCityIndex(const string& city) {
  return std::stoi(city.substr(5)); // Extract index from "city_N"
}

int GetRegionFromCity(int city_index, const ConfigurationPtr& config) {
  int num_partitions = config->num_partitions();
  int num_regions = GetNumRegions(config);

  return (city_index / num_partitions) % num_regions;
}

int GetPartitionFromCity(int city_index, const ConfigurationPtr& config) {
  int num_partitions = config->num_partitions();
  return city_index % num_partitions;
}

vector<string> GetPartitionCities(int partition, const ConfigurationPtr& config) {
  vector<string> cities;
  int num_partitions = config->num_partitions();
  for (int i = 0; i < config->proto_config().movr_partitioning().cities(); i++) {
    if (i % num_partitions == partition) {
      cities.push_back(DataGenerator::EnsureFixedLength<64>("city_" + std::to_string(i)));
    }
  }
  return cities;
}

} // namespace

MovrWorkload::MovrWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const string& params_str,
                           std::pair<int, int> id_slot, const int duration, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      distance_ranking_(config->distance_ranking_from(region)),
      rg_(seed),
      client_txn_id_counter_(0),
      cities_(GetPartitionCities(config->local_partition(), config)),
      duration_(static_cast<double>(duration)) {
  name_ = "movr";

  // Initialise parameters
  zipf_coef_ = params_.GetDouble(MH_ZIPF);
  multi_home_pct_ = params_.GetInt32(MULTI_HOME_PCT);
  multi_partition_pct_ = params_.GetInt32(MULTI_PARTITION_PCT);
  max_homes_ = std::min(params_.GetInt32(HOMES), GetNumRegions(config_));
  skew_ = params_.GetDouble(SKEW);
  sh_only_ = params_.GetInt32(SH_ONLY) == 1;
  sunflower_max_pct_ = params_.GetDouble(SUNFLOWER_MAX);
  sunflower_falloff_ = params_.GetDouble(SUNFLOWER_FALLOFF);
  sunflower_cycles_ = params_.GetInt32(SUNFLOWER_CYCLES);

  // Initialize zipf distributions for skewed access
  user_id_dist_ = slog::zipf_distribution(skew_, kMaxUsersPerCity);
  vehicle_id_dist_ = slog::zipf_distribution(skew_, kMaxVehiclesPerCity);
  ride_id_dist_ = slog::zipf_distribution(skew_, kMaxRidesPerCity);

  // Setup region information and distance ranking
  start_time_ = std::chrono::steady_clock::now();
  num_regions_ = GetNumRegions(config_);
  if (distance_ranking_.empty()) {
    for (int i = 0; i < num_regions_; i++) {
      if (i != local_region_) {
        distance_ranking_.push_back(i);
      }
    }
    if (zipf_coef_ > 0) {
      LOG(WARNING) << "Distance ranking is not provided. MH_ZIPF is reset to 0.";
      zipf_coef_ = 0;
    }
  } else if (config_->num_regions() == 1) {
    // This case is for the Calvin experiment where there is only a single region.
    // The num_regions variable is equal to num_replicas at this point
    CHECK_EQ(static_cast<int>(distance_ranking_.size()), num_regions_ * (num_regions_ - 1));
    size_t from = local_region_ * (num_regions_ - 1);
    std::copy_n(distance_ranking_.begin() + from, num_regions_ - 1, distance_ranking_.begin());
    distance_ranking_.resize(num_regions_ - 1);
  }
  CHECK_EQ(static_cast<int>(distance_ranking_.size()), num_regions_ - 1) << "Distance ranking size must match the number of regions";

  // Validate and initialize distributions
  multi_home_dist_ = bernoulli_distribution(multi_home_pct_ / 100.0);
  multi_partition_dist_ = std::bernoulli_distribution(multi_partition_pct_ / 100.0);
  InitializeTxnMix();
  InitializeRegionSelection();
  InitializeCityIndex();
  PrintCityDistribution();
}

void MovrWorkload::InitializeTxnMix() {
  auto txn_mix_str = Split(params_.GetString(TXN_MIX), ":");
  CHECK_EQ(txn_mix_str.size(), static_cast<size_t>(MovrTxnType::NUM_TXN_TYPES))
      << "Invalid transaction mix configuration";
  
  vector<double> mix_weights;
  int total = 0;
  for (const auto& pct : txn_mix_str) {
    int val = std::stoi(Trim(pct));
    mix_weights.push_back(val);
    total += val;
  }
  CHECK_EQ(total, 100) << "Txn mix must sum to 100% (actual: " << total << ")";
  select_txn_dist_ = std::discrete_distribution<>(mix_weights.begin(), mix_weights.end());
}

void MovrWorkload::InitializeRegionSelection() {
  num_regions_ = GetNumRegions(config_);
  if (!params_.GetString(REGION_REQUEST_MIX).empty()) {
    // Parse custom region distribution
    auto region_pct = Split(params_.GetString(REGION_REQUEST_MIX), ":");
    CHECK_EQ(region_pct.size(), num_regions_) << "Region mix must match number of regions";
    
    vector<double> region_weights;
    int total = 0;
    for (const auto& pct : region_pct) {
      int val = std::stoi(Trim(pct));
      region_weights.push_back(val);
      total += val;
    }
    CHECK_EQ(total, 100) << "Region mix must sum to 100%";
    select_origin_region_dist_ = std::discrete_distribution<>(region_weights.begin(), region_weights.end());
  } else {
    // Uniform distribution
    vector<double> uniform_weights(num_regions_, 1.0/num_regions_);
    select_origin_region_dist_ = std::discrete_distribution<>(uniform_weights.begin(), uniform_weights.end());
  }
}

void MovrWorkload::InitializeCityIndex() {
  // Initialize city_index_ similar to warehouse_index_ in TPCC
  int num_partitions = config_->num_partitions();
  city_index_.resize(num_partitions);
  for (int i = 0; i < num_partitions; i++) {
    city_index_[i].resize(num_regions_);
  }
  
  int num_cities = config_->proto_config().movr_partitioning().cities();
  for (int i = 0; i < num_cities; i++) {
    int partition = i % num_partitions;
    int region = (i / num_partitions) % num_regions_;
    city_index_[partition][region].push_back(DataGenerator::EnsureFixedLength<64>("city_" + std::to_string(i)));
  }
}

// Selects a home city based on the configured region mix
std::string MovrWorkload::SelectHomeCity() {
  if (cities_.empty()) {
    LOG(FATAL) << "No cities available in partition";
  }
  
  std::string result;

  // Select based on region distribution
  int region_idx = select_origin_region_dist_(rg_);
  
  // Find cities in the local partition that belong to the selected region
  int partition = config_->local_partition();
  if (city_index_[partition][region_idx].empty()) {
    // Fallback to any city in the local partition if none in the selected region
    result = cities_[std::uniform_int_distribution<>(0, cities_.size() - 1)(rg_)];
  } else {
    // Select a random city from the chosen region and partition
    result = city_index_[partition][region_idx][
      std::uniform_int_distribution<>(0, city_index_[partition][region_idx].size() - 1)(rg_)
    ];
  }

  return DataGenerator::EnsureFixedLength<64>(result);
}

std::string MovrWorkload::SelectMultiHomeMultiPartitionCity(const std::string& home_city) {
  int home_city_idx = GetCityIndex(home_city);
  int home_region = GetRegionFromCity(home_city_idx, config_);
  int home_partition = GetPartitionFromCity(home_city_idx, config_);
  
  // Find all cities in different regions and different partitions
  std::vector<std::string> candidate_cities;
  for (int p = 0; p < config_->num_partitions(); p++) {
    if (p == home_partition) continue;
    
    for (int r = 0; r < num_regions_; r++) {
      if (r == home_region) continue;
      
      for (const auto& city : city_index_[p][r]) {
        candidate_cities.push_back(city);
      }
    }
  }
  
  if (candidate_cities.empty()) {
    return home_city;
  }
  
  return candidate_cities[std::uniform_int_distribution<>(0, candidate_cities.size() - 1)(rg_)];
}

std::string MovrWorkload::SelectMultiHomeCity(const std::string& home_city) {
  int home_city_idx = GetCityIndex(home_city);
  int home_region = GetRegionFromCity(home_city_idx, config_);
  int home_partition = GetPartitionFromCity(home_city_idx, config_);
  
  // Find all cities in same partition but different regions
  std::vector<std::string> candidate_cities;
  for (int r = 0; r < num_regions_; r++) {
    if (r == home_region) continue;
    
    for (const auto& city : city_index_[home_partition][r]) {
      candidate_cities.push_back(city);
    }
  }
  
  if (candidate_cities.empty()) {
    return home_city;
  }
  
  return candidate_cities[std::uniform_int_distribution<>(0, candidate_cities.size() - 1)(rg_)];
}

std::string MovrWorkload::SelectMultiPartitionCity(const std::string& home_city) {
  int home_city_idx = GetCityIndex(home_city);
  int home_region = GetRegionFromCity(home_city_idx, config_);
  int home_partition = GetPartitionFromCity(home_city_idx, config_);
  
  // Find all cities in same region but different partitions
  std::vector<std::string> candidate_cities;
  for (int p = 0; p < config_->num_partitions(); p++) {
    if (p == home_partition) continue;
    
    for (const auto& city : city_index_[p][home_region]) {
      candidate_cities.push_back(city);
    }
  }
  
  if (candidate_cities.empty()) {
    return home_city;
  }
  
  return candidate_cities[std::uniform_int_distribution<>(0, candidate_cities.size() - 1)(rg_)];
}


void MovrWorkload::UpdateSunflowerRegionWeights() {
  if (duration_ <= 0) {
    return;  // avoid division by zero
  }

  // Get elapsed time in seconds
  double elapsed_sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - start_time_).count();
  double progress = fmod(elapsed_sec / duration_, 1.0);
  double sun_pos = fmod(progress * sunflower_cycles_ * num_regions_, static_cast<double>(num_regions_));

  // Assign weights
  std::vector<double> region_weights(num_regions_);
  double sum_weights = 0.0;
  for (int i = 0; i < num_regions_; ++i) {
    double distance = std::abs(sun_pos - i);
    if (distance > num_regions_ / 2.0) {
      distance = num_regions_ - distance;  // Wraparound for cyclic sun
    }
    double weight = sunflower_max_pct_ * std::pow(1.0 - sunflower_falloff_, distance);
    region_weights[i] = weight;
    sum_weights += weight;
  }

  // Normalize to make total = 100%
  for (auto& w : region_weights) {
    w = w * 100.0 / sum_weights;
  }

  select_origin_region_dist_ = std::discrete_distribution<>(region_weights.begin(), region_weights.end());
}

std::pair<Transaction*, TransactionProfile> MovrWorkload::NextTransaction() {
  Transaction* txn = new Transaction();
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_home = false;
  pro.is_multi_partition = false;

  UpdateSunflowerRegionWeights();

  bool is_multi_home = false;
  bool is_multi_partition =false;

  // Select the transaction type
  MovrTxnType txn_type = static_cast<MovrTxnType>(select_txn_dist_(rg_));
  string home_city = SelectHomeCity();

  // Determine if this transaction is eligible to be multi-home or multi_partition
  if (txn_type == MovrTxnType::ADD_VEHICLE || 
    txn_type == MovrTxnType::START_RIDE || 
    txn_type == MovrTxnType::END_RIDE) {
      is_multi_home = !sh_only_ && multi_home_dist_(rg_);
      is_multi_partition = multi_partition_dist_(rg_);
  }

  switch (txn_type) {
    case MovrTxnType::VIEW_VEHICLES:
      GenerateViewVehiclesTxn(*txn, pro, home_city);
      view_vehicle_count++;
      break;
    case MovrTxnType::USER_SIGNUP:
      GenerateUserSignupTxn(*txn, pro, home_city);
      user_signup_count++;
      break;
    case MovrTxnType::ADD_VEHICLE:
      GenerateAddVehicleTxn(*txn, pro, home_city, is_multi_home, is_multi_partition);
      add_vehicle_count++;
      break;
    case MovrTxnType::START_RIDE:
      GenerateStartRideTxn(*txn, pro, home_city, is_multi_home, is_multi_partition);
      start_ride_count++;
      break;
    case MovrTxnType::UPDATE_LOCATION:
      GenerateUpdateLocationTxn(*txn, pro, home_city);
      update_location_count++;
      break;
    case MovrTxnType::END_RIDE:
      GenerateEndRideTxn(*txn, pro, home_city, is_multi_home, is_multi_partition);
      end_ride_count++;
      break;
    default:
      LOG(FATAL) << "Invalid MovR txn type selected";
  }

  // Update multi-home counter if the transaction is actually multi-home
  if (pro.is_multi_home) {
      multi_home_count++;
  }

  // Update multi-partition counter if the transaction is actually multi-partition
  if (pro.is_multi_partition) {
    multi_partition_count++;
  }

  total_txn_count++;

  txn->mutable_internal()->set_id(client_txn_id_counter_);
  client_txn_id_counter_++;

  LogStatistics();
  return {txn, pro};
}

// Logging for transaction counts/percentages
void MovrWorkload::LogStatistics() {
  if (total_txn_count % 10000 == 0) {
    LOG(INFO) << "MovR Stats - Total: " << total_txn_count
              << " ViewVehicles: " << view_vehicle_count
              << " UserSignups: " << user_signup_count
              << " AddVehicle: " << add_vehicle_count
              << " StartRide: " << start_ride_count
              << " UpdateLoc: " << update_location_count
              << " EndRide: " << end_ride_count
              << " MH%: " << (100.0 * multi_home_count / total_txn_count)
              << " MP%: " << (100.0 * multi_partition_count / total_txn_count);
  }
}

void MovrWorkload::PrintCityDistribution() {
  for (int p = 0; p < config_->num_partitions(); p++) {
    for (int r = 0; r < num_regions_; r++) {
      LOG(INFO) << "Partition " << p << ", Region " << r << " cities:";
      for (const auto& city : city_index_[p][r]) {
        LOG(INFO) << "  " << city;
      }
    }
  }
}

// --- Transaction Generation Implementations --- 

// Simple read transaction: Find vehicles near a location in the specified city.
// This transaction is typically single-home, focused on the 'city'.
void MovrWorkload::GenerateViewVehiclesTxn(Transaction& txn, TransactionProfile& pro, const std::string& city) {
  auto txn_adapter = std::make_shared<movr::TxnKeyGenStorageAdapter>(txn);
  std::vector<uint64_t> vehicle_ids;
  vehicle_ids.reserve(movr::kVehicleViewLimit);

  for (int i = 0; i < movr::kVehicleViewLimit; i++) {
    uint64_t local_id = vehicle_id_dist_(rg_);
    uint64_t global_id = GenerateGlobalId(GetCityIndex(city), local_id);
    vehicle_ids.push_back(global_id);
  }

  movr::ViewVehiclesTxn view_vehicles_txn(txn_adapter, vehicle_ids, city);
  view_vehicles_txn.Read();
  view_vehicles_txn.Write();
  txn_adapter->Finialize();

  auto* procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("view_vehicles");
  procedure->add_args(city);
  for (uint64_t id : vehicle_ids) {
    procedure->add_args(std::to_string(id));
  }
}

// Write transaction: Insert a new user record.
// This transaction is typically single-home, writing to the 'city' where the user signs up.
void MovrWorkload::GenerateUserSignupTxn(Transaction& txn, TransactionProfile& pro, const std::string& city) {
  auto txn_adapter = std::make_shared<movr::TxnKeyGenStorageAdapter>(txn);
  static const uint64_t users_per_city = kDefaultUsers / cities_.size();
  uint64_t local_id = users_per_city + (++user_signup_count);
  uint64_t global_id = GenerateGlobalId(GetCityIndex(city), local_id);
  std::string name = DataGenerator::GenerateName(rg_);
  std::string address = DataGenerator::GenerateAddress(rg_);
  std::string credit_card = DataGenerator::GenerateCreditCard(rg_);


  movr::UserSignupTxn user_signup_txn(txn_adapter, global_id, city, name, address, credit_card);
  user_signup_txn.Read();
  user_signup_txn.Write();
  txn_adapter->Finialize();

  auto* procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("user_signup");
  procedure->add_args(std::to_string(global_id));
  procedure->add_args(city);
  procedure->add_args(name);
  procedure->add_args(address);
  procedure->add_args(credit_card);
}

// Write transaction: Add a new vehicle owned by a user.
// Can be multi-home if the owner (user) is in a different city than the vehicle's home city.
// Can be multi-partition if the owner is in a different partition but same region.
void MovrWorkload::GenerateAddVehicleTxn(Transaction& txn, TransactionProfile& pro, const std::string& home_city,
  bool is_multi_home, bool is_multi_partition) {
  auto txn_adapter = std::make_shared<movr::TxnKeyGenStorageAdapter>(txn);
  std::string owner_city = home_city;

  if (is_multi_home && is_multi_partition) {
    owner_city = SelectMultiHomeMultiPartitionCity(home_city);
  } else if (is_multi_home) {
    owner_city = SelectMultiHomeCity(home_city);
  } else if (is_multi_partition) {
    owner_city = SelectMultiPartitionCity(home_city);
  }

  // Verify that owner_city is actually in a different region
  int home_region = GetRegionFromCity(GetCityIndex(home_city), config_);
  int owner_region = GetRegionFromCity(GetCityIndex(owner_city), config_);
  pro.is_multi_home = (home_region != owner_region);

  // Check if partitions are different
  int home_partition = GetPartitionFromCity(GetCityIndex(home_city), config_);
  int owner_partition = GetPartitionFromCity(GetCityIndex(owner_city), config_);    
  pro.is_multi_partition = (home_partition != owner_partition);

  static const uint64_t vehicles_per_city = kDefaultVehicles / cities_.size();
  uint64_t vehicle_local_id = vehicles_per_city + (++add_vehicle_count);
  uint64_t vehicle_id = GenerateGlobalId(GetCityIndex(home_city), vehicle_local_id);
  std::string type = DataGenerator::GenerateRandomVehicleType(rg_);
  uint64_t owner_local_id = user_id_dist_(rg_);
  uint64_t owner_id = GenerateGlobalId(GetCityIndex(owner_city), owner_local_id);
  uint64_t creation_time = std::chrono::system_clock::now().time_since_epoch().count();
  std::string status = DataGenerator::EnsureFixedLength<64>("available");
  std::string current_location = DataGenerator::GenerateAddress(rg_);
  std::string ext = DataGenerator::GenerateVehicleMetadata(rg_, type);

  movr::AddVehicleTxn add_vehicle_txn(txn_adapter, vehicle_id, home_city, type, owner_id, owner_city,
    creation_time, status, current_location, ext);
  add_vehicle_txn.Read();
  add_vehicle_txn.Write();
  txn_adapter->Finialize();

  auto* procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("add_vehicle");
  procedure->add_args(std::to_string(vehicle_id));
  procedure->add_args(home_city);
  procedure->add_args(type);
  procedure->add_args(std::to_string(owner_id));
  procedure->add_args(owner_city);
  procedure->add_args(std::to_string(creation_time));
  procedure->add_args(status);
  procedure->add_args(current_location);
  procedure->add_args(ext);
}

// Read/Write transaction: A user starts a ride on a vehicle.
// Reads user info, vehicle status. Writes new ride record, updates vehicle status.
// Can be multi-home if the user is in a different city than the vehicle.
void MovrWorkload::GenerateStartRideTxn(Transaction& txn, TransactionProfile& pro, const std::string& home_city,
  bool is_multi_home, bool is_multi_partition) {
  auto txn_adapter = std::make_shared<movr::TxnKeyGenStorageAdapter>(txn);
  std::string user_city = home_city;
  std::string vehicle_city = home_city; // Link to actual vehicle city
  std::string code = DataGenerator::GeneratePromoCode(rg_);
  std::string start_address = DataGenerator::GenerateAddress(rg_);
  uint64_t start_time = std::chrono::system_clock::now().time_since_epoch().count();

  if (is_multi_home && is_multi_partition) {
    if (std::bernoulli_distribution(0.5)(rg_)) { // 50% chance to change city for the user or the vehicle
        user_city = SelectMultiHomeMultiPartitionCity(home_city);
      } else {
        vehicle_city = SelectMultiHomeMultiPartitionCity(home_city);
    }
  } else if (is_multi_home) {
    if (std::bernoulli_distribution(0.5)(rg_)) { // 50% chance to change city for the user or the vehicle
        user_city = SelectMultiHomeCity(home_city);
      } else {
        vehicle_city = SelectMultiHomeCity(home_city);
    }
  } else if (is_multi_partition) {
    if (std::bernoulli_distribution(0.5)(rg_)) { // 50% chance to change city for the user or the vehicle
        user_city = SelectMultiPartitionCity(home_city);
      } else {
        vehicle_city = SelectMultiPartitionCity(home_city);
    }
  }

  // Verify that we're actually accessing different regions
  int home_region = GetRegionFromCity(GetCityIndex(home_city), config_);
  int user_region = GetRegionFromCity(GetCityIndex(user_city), config_);
  int vehicle_region = GetRegionFromCity(GetCityIndex(vehicle_city), config_);
  pro.is_multi_home = (home_region != user_region) || (home_region != vehicle_region);

  // Check if partitions are different
  int home_partition = GetPartitionFromCity(GetCityIndex(home_city), config_);
  int user_partition = GetPartitionFromCity(GetCityIndex(user_city), config_);
  int vehicle_partition = GetPartitionFromCity(GetCityIndex(vehicle_city), config_);
  pro.is_multi_partition = (home_partition != user_partition) || (home_partition != vehicle_partition);

  // Generate IDs within valid ranges
  uint64_t user_local_id = user_id_dist_(rg_);
  uint64_t vehicle_local_id = vehicle_id_dist_(rg_);
  static const uint64_t rides_per_city = kDefaultRides / cities_.size();
  uint64_t ride_local_id = rides_per_city + (++start_ride_count);
  
  uint64_t user_id = GenerateGlobalId(GetCityIndex(user_city), user_local_id);
  uint64_t vehicle_id = GenerateGlobalId(GetCityIndex(vehicle_city), vehicle_local_id);
  uint64_t ride_id = GenerateGlobalId(GetCityIndex(home_city), ride_local_id);

  movr::StartRideTxn start_ride_txn(txn_adapter, user_id, user_city, code, vehicle_id, vehicle_city,
    ride_id, home_city, start_address, start_time);
  start_ride_txn.Read();
  start_ride_txn.Write();
  txn_adapter->Finialize();

  auto* procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("start_ride");
  procedure->add_args(std::to_string(user_id));
  procedure->add_args(user_city);
  procedure->add_args(code);
  procedure->add_args(std::to_string(vehicle_id));
  procedure->add_args(vehicle_city);
  procedure->add_args(std::to_string(ride_id));
  procedure->add_args(home_city);
  procedure->add_args(start_address);
  procedure->add_args(std::to_string(start_time));
}

// Write transaction: Append a location update to a ride's history.
// Typically single-home, writing to the city where the ride is happening.
void MovrWorkload::GenerateUpdateLocationTxn(Transaction& txn, TransactionProfile& pro, const std::string& city) {
  auto txn_adapter = std::make_shared<movr::TxnKeyGenStorageAdapter>(txn);
  uint64_t ride_local_id = ride_id_dist_(rg_);
  uint64_t ride_id = GenerateGlobalId(GetCityIndex(city), ride_local_id);
  uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
  const auto loc = DataGenerator::GenerateRandomLatLong(rg_);
  uint64_t lat = static_cast<uint64_t>(stod(loc.first));
  uint64_t lon = static_cast<uint64_t>(stod(loc.second));

  movr::UpdateLocationTxn update_location_txn(txn_adapter, city, ride_id, timestamp, lat, lon);
  update_location_txn.Read();
  update_location_txn.Write();
  txn_adapter->Finialize();

  auto* procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("update_location");
  procedure->add_args(city);
  procedure->add_args(std::to_string(ride_id));
  procedure->add_args(std::to_string(timestamp));
  procedure->add_args(loc.first);
  procedure->add_args(loc.second);
}

// Read/Write transaction: End a ride, update vehicle status, calculate revenue.
// Reads ride info, vehicle info. Updates ride record, updates vehicle status.
// Can be multi-home if the ride spanned cities or user/vehicle are in different cities.
void MovrWorkload::GenerateEndRideTxn(Transaction& txn, TransactionProfile& pro, const std::string& home_city,
  bool is_multi_home, bool is_multi_partition) {
  auto txn_adapter = std::make_shared<movr::TxnKeyGenStorageAdapter>(txn);
  std::string user_city = home_city;
  std::string vehicle_city = home_city;

  if (is_multi_home && is_multi_partition) {
    vehicle_city = SelectMultiHomeMultiPartitionCity(home_city);
  } else if (is_multi_home) {
    vehicle_city = SelectMultiHomeCity(home_city);
  } else if (is_multi_partition) {
    vehicle_city = SelectMultiPartitionCity(home_city);
  }

  // Verify that we're actually accessing different regions
  int home_region = GetRegionFromCity(GetCityIndex(home_city), config_);
  int vehicle_region = GetRegionFromCity(GetCityIndex(vehicle_city), config_);
  pro.is_multi_home = (home_region != vehicle_region);

  // Check if partitions are different
  int home_partition = GetPartitionFromCity(GetCityIndex(home_city), config_);
  int vehicle_partition = GetPartitionFromCity(GetCityIndex(vehicle_city), config_);    
  pro.is_multi_partition = (home_partition != vehicle_partition);

  uint64_t ride_local_id = ride_id_dist_(rg_);
  uint64_t vehicle_local_id = vehicle_id_dist_(rg_);
  
  uint64_t ride_id = GenerateGlobalId(GetCityIndex(home_city), ride_local_id);
  uint64_t vehicle_id = GenerateGlobalId(GetCityIndex(vehicle_city), vehicle_local_id);

  std::string end_address = DataGenerator::GenerateAddress(rg_);
  uint64_t end_time = std::chrono::system_clock::now().time_since_epoch().count();
  std::string revenue_str = DataGenerator::GenerateRevenue(rg_);
  uint64_t revenue = static_cast<uint64_t>(std::stod(revenue_str));

  movr::EndRideTxn end_ride_txn(txn_adapter, ride_id, home_city, vehicle_id, vehicle_city,
    end_address, end_time, revenue);
  end_ride_txn.Read();
  end_ride_txn.Write();
  txn_adapter->Finialize();

  auto* procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("end_ride");
  procedure->add_args(std::to_string(ride_id));
  procedure->add_args(home_city);
  procedure->add_args(std::to_string(vehicle_id));
  procedure->add_args(vehicle_city);
  procedure->add_args(end_address);
  procedure->add_args(std::to_string(end_time));
  procedure->add_args(revenue_str);
}

} // namespace slog