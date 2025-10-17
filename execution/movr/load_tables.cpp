#include "execution/movr/load_tables.h"
#include "execution/movr/data_generator.h"
#include "execution/movr/constants.h"

#include <algorithm>
#include <random>
#include <thread>
#include <cstdint> // For uint64_t

#include "common/string_utils.h"
#include "execution/movr/table.h"
#include "workload/movr.h"

namespace slog {
namespace movr {

class PartitionedMovrDataLoader {
 public:
  PartitionedMovrDataLoader(const StorageAdapterPtr& storage_adapter, std::vector<std::string> partition_cities, int num_users,
    int num_vehicles, int num_rides, int num_histories, int num_codes, int num_user_codes, int num_regions,
    int num_partitions, int partition)
      : rg_(partition),
      storage_adapter_(storage_adapter),
      partition_cities_(partition_cities),
      num_users_(num_users),
      num_vehicles_(num_vehicles),
      num_rides_(num_rides),
      num_codes_(num_codes),
      num_user_codes_(num_user_codes),
      num_histories_(num_histories),
      num_partitions_(num_partitions),
      partition_(partition) {

      }

  bool BelongsToCurrentPartition(uint64_t global_id) const {
    const int partition_bits = 16;
    uint64_t partition_from_id = (global_id >> (64 - partition_bits)) % num_partitions_;
    return partition_from_id == static_cast<uint64_t>(partition_);
  }

  void Load() {
    LoadUsers();
    LoadVehicles();
    LoadRides();
    LoadHistories();
    LoadCodes();
    LoadUserCodes();
  }

  void LoadUsers() {
    Table<UsersSchema> users(storage_adapter_);
    LOG(INFO) << "Loading " << num_users_ << " users for each of the " << partition_cities_.size() << " cities";
    
    const uint64_t users_per_city = num_users_ / partition_cities_.size();
    
    for (size_t city_idx = 0; city_idx < partition_cities_.size(); city_idx++) {
      const auto& city = partition_cities_[city_idx];
      for (uint64_t local_id = 1; local_id <= users_per_city; local_id++) {
        uint64_t global_id = GenerateGlobalId(city_idx, local_id);
        auto name = DataGenerator::GenerateName(rg_);
        auto address = DataGenerator::GenerateAddress(rg_);
        auto credit_card = DataGenerator::GenerateCreditCard(rg_);
        
        users.Insert({
          MakeInt64Scalar(global_id),
          MakeFixedTextScalar<64>(city),
          MakeFixedTextScalar<64>(name),
          MakeFixedTextScalar<64>(address),
          MakeFixedTextScalar<64>(credit_card)
        });
      }
    }
  }

  void LoadVehicles() {
    Table<VehiclesSchema> vehicles(storage_adapter_);
    LOG(INFO) << "Loading " << num_vehicles_ << " vehicles for each of the " << partition_cities_.size() << " cities";
    
    const uint64_t vehicles_per_city = num_vehicles_ / partition_cities_.size();
    const uint64_t users_per_city = num_users_ / partition_cities_.size();
    
    // Status distribution thresholds
    const uint64_t in_use_threshold = static_cast<uint64_t>(vehicles_per_city * 0.55);
    const uint64_t available_threshold = in_use_threshold + static_cast<uint64_t>(vehicles_per_city * 0.40);
    
    for (size_t city_idx = 0; city_idx < partition_cities_.size(); city_idx++) {
      const auto& city = partition_cities_[city_idx];
      std::uniform_int_distribution<uint64_t> owner_rnd(1, users_per_city);
      
      for (uint64_t local_id = 1; local_id <= vehicles_per_city; local_id++) {
        uint64_t global_id = GenerateGlobalId(city_idx, local_id);
        uint64_t owner_id = GenerateGlobalId(city_idx, owner_rnd(rg_));
        auto type = DataGenerator::GenerateRandomVehicleType(rg_);
        auto creation_time = std::chrono::system_clock::now().time_since_epoch().count();
        
        // Determine status
        std::string status;
        if (local_id <= in_use_threshold) {
            status = "in_use";
        } else if (local_id <= available_threshold) {
            status = "available";
        } else {
            status = "lost";
        }
        
        auto current_location = DataGenerator::GenerateAddress(rg_);
        auto ext = DataGenerator::GenerateVehicleMetadata(rg_, type);
        
        vehicles.Insert({
          MakeInt64Scalar(global_id),
          MakeFixedTextScalar<64>(city),
          MakeFixedTextScalar<64>(type),
          MakeInt64Scalar(owner_id),
          MakeInt64Scalar(creation_time),
          MakeFixedTextScalar<64>(DataGenerator::EnsureFixedLength<64>(status)),
          MakeFixedTextScalar<64>(current_location),
          MakeFixedTextScalar<64>(ext)
        });
      }
    }
  }

  void LoadRides() {
    Table<RidesSchema> rides(storage_adapter_);
    LOG(INFO) << "Loading " << num_rides_ << " rides for each of the " << partition_cities_.size() << " cities";
    
    const uint64_t rides_per_city = num_rides_ / partition_cities_.size();
    const uint64_t users_per_city = num_users_ / partition_cities_.size();
    const uint64_t vehicles_per_city = num_vehicles_ / partition_cities_.size();
    
    for (size_t city_idx = 0; city_idx < partition_cities_.size(); city_idx++) {
      const auto& city = partition_cities_[city_idx];
      std::uniform_int_distribution<uint64_t> rider_rnd(1, users_per_city);
      std::uniform_int_distribution<uint64_t> vehicle_rnd(1, static_cast<uint64_t>(vehicles_per_city * 0.55));
      
      for (uint64_t local_id = 1; local_id <= rides_per_city; local_id++) {
        uint64_t global_id = GenerateGlobalId(city_idx, local_id);
        uint64_t rider_id = GenerateGlobalId(city_idx, rider_rnd(rg_));
        uint64_t vehicle_id = GenerateGlobalId(city_idx, vehicle_rnd(rg_));
        
        auto start_address = DataGenerator::GenerateAddress(rg_);
        auto end_address = DataGenerator::GenerateAddress(rg_);
        auto start_time = std::chrono::system_clock::now().time_since_epoch().count();
        auto end_time = start_time + 3600; // 1 hour later
        auto revenue_str = DataGenerator::GenerateRevenue(rg_);
        uint64_t revenue = static_cast<uint64_t>(std::stod(revenue_str));
        
        rides.Insert({
          MakeInt64Scalar(global_id),
          MakeFixedTextScalar<64>(city),
          MakeFixedTextScalar<64>(city), // vehicle_city same as home_city
          MakeInt64Scalar(rider_id),
          MakeInt64Scalar(vehicle_id),
          MakeFixedTextScalar<64>(start_address),
          MakeFixedTextScalar<64>(end_address),
          MakeInt64Scalar(start_time),
          MakeInt64Scalar(end_time),
          MakeInt64Scalar(revenue)
        });
      }
    }
  }

  void LoadHistories() {
    Table<VehicleLocationHistoriesSchema> histories(storage_adapter_);
    LOG(INFO) << "Loading " << num_histories_ << " histories for each of the " << partition_cities_.size() << " cities";
    
    const uint64_t histories_per_city = num_histories_ / partition_cities_.size();
    const uint64_t rides_per_city = num_rides_ / partition_cities_.size();
    
    for (size_t city_idx = 0; city_idx < partition_cities_.size(); city_idx++) {
      const auto& city = partition_cities_[city_idx];
      std::uniform_int_distribution<uint64_t> ride_rnd(1, rides_per_city);
      
      for (uint64_t i = 1; i <= histories_per_city; i++) {
        uint64_t ride_id = GenerateGlobalId(city_idx, ride_rnd(rg_));
        auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        auto latlon = DataGenerator::GenerateRandomLatLong(rg_);
        uint64_t lat = static_cast<uint64_t>(std::stod(latlon.first));
        uint64_t lon = static_cast<uint64_t>(std::stod(latlon.second));
        
        histories.Insert({
          MakeFixedTextScalar<64>(city),
          MakeInt64Scalar(ride_id),
          MakeInt64Scalar(timestamp),
          MakeInt64Scalar(lat),
          MakeInt64Scalar(lon)
        });
      }
    }
  }

  void LoadCodes() {
    Table<PromoCodesSchema> codes(storage_adapter_);
    LOG(INFO) << "Loading " << num_codes_ << " promo codes";
    std::mt19937 rg;

    for (int i = 1; i <= num_codes_; i++) {
      auto code = DataGenerator::GeneratePromoCode(rg_);
      auto description = DataGenerator::GenerateDescription(rg_);
      auto creation_time = std::chrono::system_clock::now().time_since_epoch().count();
      std::uniform_int_distribution<> duration(100, 1000);
      auto expiration_time = creation_time + duration(rg_);
      auto rules = DataGenerator::GenerateRules(rg_);
      codes.Insert({
        MakeFixedTextScalar<64>(code),
        MakeFixedTextScalar<64>(description),
        MakeInt64Scalar(creation_time),
        MakeInt64Scalar(expiration_time),
        MakeFixedTextScalar<64>(rules)
        });
    }
  }

  void LoadUserCodes() {
    Table<UserPromoCodesSchema> user_codes(storage_adapter_);
    LOG(INFO) << "Loading " << num_user_codes_ << " user promo codes for each of the " << partition_cities_.size() << " cities";
    
    const uint64_t codes_per_city = num_user_codes_ / partition_cities_.size();
    const uint64_t users_per_city = num_users_ / partition_cities_.size();
    
    for (size_t city_idx = 0; city_idx < partition_cities_.size(); city_idx++) {
      const auto& city = partition_cities_[city_idx];
      std::uniform_int_distribution<uint64_t> user_rnd(1, users_per_city);
      std::uniform_int_distribution<int> usage_rnd(0, 5);
      
      for (uint64_t i = 1; i <= codes_per_city; i++) {
        uint64_t user_id = GenerateGlobalId(city_idx, user_rnd(rg_));
        auto code = DataGenerator::GeneratePromoCode(rg_);
        auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        int usage_count = usage_rnd(rg_);
        
        user_codes.Insert({
          MakeFixedTextScalar<64>(city),
          MakeInt64Scalar(user_id),
          MakeFixedTextScalar<64>(code),
          MakeInt64Scalar(timestamp),
          MakeInt64Scalar(usage_count)
        });
      }
    }
  }

  private:
  std::mt19937 rg_;

  StorageAdapterPtr storage_adapter_;
  std::vector<std::string> partition_cities_;
  int num_users_;
  int num_vehicles_;
  int num_rides_;
  int num_codes_;
  int num_user_codes_;
  int num_histories_;
  int num_partitions_;
  int partition_;

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
  uint64_t GenerateGlobalId(int city_index, uint64_t local_id, int partition_bits = 16) {
    // Shift city index into the upper partition_bits
    uint64_t city_part = static_cast<uint64_t>(city_index) << (64 - partition_bits);
    
    // Mask local_id to use only the remaining (64 - partition_bits)
    uint64_t local_part = local_id & ((1ULL << (64 - partition_bits)) - 1);
    
    // Combine both parts
    return city_part | local_part;
  }
};


void LoadTables(const StorageAdapterPtr& storage_adapter, int cities, int num_regions, 
               int num_partitions, int partition, int num_threads) {
  // Define the cities for this partition
  std::vector<std::string> partition_cities;
  for (int i = 0; i < cities; i++) {
    if (i % num_partitions == partition) {
      partition_cities.push_back(DataGenerator::EnsureFixedLength<64>("city_" + std::to_string(i)));
    }
  }

  // Calculate per-city record counts
  const int users_per_city = kDefaultUsers / cities;
  const int vehicles_per_city = kDefaultVehicles / cities;
  const int rides_per_city = kDefaultRides / cities;
  const int histories_per_city = kDefaultHistories / cities;
  const int codes = kDefaultPromoCodes;
  const int user_codes_per_city = kDefaultUserPromoCodes / cities;

  // Create data loader for this partition
  PartitionedMovrDataLoader loader(
      storage_adapter,
      partition_cities,
      users_per_city,
      vehicles_per_city,
      rides_per_city,
      histories_per_city,
      codes,
      user_codes_per_city,
      num_regions,
      num_partitions,
      partition  // used as seed
  );

  // Load the data
  loader.Load();
}

}  // namespace movr
}  // namespace slog