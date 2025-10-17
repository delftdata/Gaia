#include "execution/dsh/load_tables.h"

#include <algorithm>
#include <random>
#include <thread>
#include <string>

#include "execution/dsh/table.h"
#include "common/string_utils.h"
#include "execution/dsh/utils.h"

namespace{
    const std::string kCharacters("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ");
}

namespace slog {
namespace dsh {

class PartitionedDSHLoader {
    public:
        PartitionedDSHLoader(const StorageAdapterPtr& storage_adapter, int num_p, int partition, int num_r, int num_u, int num_h, double max_coord, int seed)
        : rg_(seed), str_gen_(seed), storage_adapter_(storage_adapter), num_p_(num_p), partition_(partition), num_r_(num_r), num_u_(num_u), num_h_(num_h), coord_range_(max_coord) {}

        // load the database tables which need data beforehand
        void Load() {
            LoadUsers();
            LoadHotels();
            // LoadGeo();
            // reservations are only added, don't need to be initialized
        }

    private:
        std::mt19937 rg_;
        RandomStringGenerator str_gen_;
    
        StorageAdapterPtr storage_adapter_;
        uint32_t partition_;
        uint32_t num_p_;
        uint32_t num_r_;
        
        uint32_t num_u_;
        uint32_t num_h_;
        double coord_range_;

        // can't make MH or MP transactions here
        void LoadUsers() {
            Table<UserSchema> users(storage_adapter_);
            // this for loop is equivalent to pps but no checking
            for (uint32_t i = partition_; i < num_u_; i += num_p_) {
                users.Insert({
                    MakeFixedTextScalar<20>(format_uname(std::to_string(i))),
                    MakeVarTextScalar<60>(std::to_string(i))
                });
                // users.Insert({
                //     MakeInt32Scalar(i),
                //     MakeInt32Scalar(i)
                // });
                LOG(INFO) << "Load user with ID " << i; 
            }
        }

        // can't make em here either
        void LoadHotels() {
            Table<HotelSchema> hotels(storage_adapter_);

            std::uniform_real_distribution<> coord_rnd(0.0, coord_range_);
            std::uniform_real_distribution<> rating_rnd(0.0, 5.0);
            std::uniform_real_distribution<> price_rnd(0.0, kMaxHotelPrice);
            std::uniform_int_distribution<> capacity_rnd(kMinHotelCapacity, kMaxHotelCapacity);

            for (uint32_t i = partition_; i < num_h_; i += num_p_) {
                hotels.Insert({
                    MakeInt32Scalar(i),
                    MakeFloat64Scalar(coord_rnd(rg_)),
                    MakeFloat64Scalar(coord_rnd(rg_)),
                    MakeFloat64Scalar(rating_rnd(rg_)),
                    MakeFloat64Scalar(price_rnd(rg_)),
                    MakeInt32Scalar(capacity_rnd(rg_)),
                });
                LOG(INFO) << "Load hotel with ID " << i; 
            }
        }

        int computePartition(int id) const {
            return id % num_p_;
        }

        int computeRegion(int id) const {
            return id / num_p_ % num_r_;
        }
};


void LoadTables(const StorageAdapterPtr& storage_adapter, int num_partitions, int partition, int num_regions, int num_users, int num_hotels,
    double coord_range, int num_threads) {
    //populate users
    PartitionedDSHLoader loader(storage_adapter, num_partitions, partition, num_regions, num_users, num_hotels, coord_range, time(nullptr));
    loader.Load();
}

} // namespace dsh
} // namespace slog
