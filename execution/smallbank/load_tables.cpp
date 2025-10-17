#include "execution/smallbank/load_tables.h"

#include <algorithm>
#include <functional>  // For std::hash
#include <random>
#include <thread>

#include "common/string_utils.h"
#include "execution/smallbank/table.h"

namespace slog {
namespace smallbank {

namespace {
const size_t kRandStrPoolSize = 1000000;
const std::string kCharacters("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ");
}  // namespace

class PartitionedSmallBankDataLoader {
 public:
  PartitionedSmallBankDataLoader(const StorageAdapterPtr& storage_adapter, int from_w, int to_w, int seed,
                                 int partition, int num_partitions, int num_regions)
      : rg_(seed),
        str_gen_(seed),
        storage_adapter_(storage_adapter),
        from_w_(from_w),
        to_w_(to_w),
        partition_(partition),
        num_partitions_(num_partitions),
        num_regions_(num_regions),
        thread_number_(seed) {}

  void Load() { LoadAccounts(); }

 private:
  std::mt19937 rg_;
  RandomStringGenerator str_gen_;

  StorageAdapterPtr storage_adapter_;
  int from_w_;
  int to_w_;
  int partition_;
  int num_partitions_;
  int num_regions_;
  int thread_number_;

  void LoadAccounts() {
    LOG(INFO) << "Generating accounts using thread " << thread_number_ << " on partition " << partition_
              << " with total partition number " << num_partitions_ << " Starting from " << from_w_ << " to " << to_w_;
    Table<AccountsSchema> accounts(storage_adapter_);
    Table<CheckingSchema> checkings(storage_adapter_);
    Table<SavingsSchema> savings(storage_adapter_);
    std::mt19937 rg;
    std::uniform_int_distribution<> bal_rnd(100, 10000);
    for (int id = from_w_; id < to_w_; id += 1) {
      // Construct padded name: "Client" + id, padded with spaces to 10 characters
      std::string client_name = "Client" + std::to_string(id);
      client_name.resize(24, ' ');  // Pad with spaces if shorter
      uint32_t hash_value = murmurhash3(client_name);

      if (hash_value % num_partitions_ == partition_) {
        LOG(INFO) << client_name << " added. Its has is " << hash_value << "with client region"
                  << ((hash_value / num_partitions_) % num_regions_) << " and id region"
                  << ((id) / num_partitions_) % num_regions_;
        // LOG(INFO) << "Loading data for account: " << client_name << std::endl;
        // clang-format off
        accounts.Insert({
          MakeFixedTextScalar<24>(client_name),
          MakeInt32Scalar(id)
        });
      }
      if(id % num_partitions_ == partition_){
        LOG(INFO) << " Acconts for ID:  " << id << " added ";
        checkings.Insert({
          MakeInt32Scalar(id),
          MakeInt32Scalar(bal_rnd(rg))
        });
        savings.Insert({
          MakeInt32Scalar(id),
          MakeInt32Scalar(bal_rnd(rg))
        });
      }
    }
  }

  uint32_t murmurhash3(const std::string& str) {
    uint32_t seed = 42;  // You can use any seed
    uint32_t hash = seed;
    const char* data = str.c_str();
    int len = str.length();
  
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
  
    int i = 0;
    while (len >= 4) {
      uint32_t k = *(reinterpret_cast<const uint32_t*>(data + i));
      k *= c1;
      k = (k << 15) | (k >> 17);  // ROTL32(k, 15)
      k *= c2;
  
      hash ^= k;
      hash = (hash << 13) | (hash >> 19);  // ROTL32(hash, 13)
      hash = hash * 5 + 0xe6546b64;
  
      len -= 4;
      i += 4;
    }
  
    uint32_t tail = 0;
    switch (len) {
      case 3:
        tail ^= data[i + 2] << 16;
        break;
      case 2:
        tail ^= data[i + 1] << 8;
        break;
      case 1:
        tail ^= data[i];
        break;
    }
  
    tail *= c1;
    tail = (tail << 15) | (tail >> 17);
    tail *= c2;
    hash ^= tail;
  
    hash ^= str.length();
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);
  
    return hash;
  }

  
  
};

void LoadTables(const StorageAdapterPtr& storage_adapter, int num_clients, int num_regions, int num_partitions, int partition,
                int num_threads) {
  LOG(INFO) << "Generating ~" << num_clients / num_partitions << " accounts using " << num_threads << " threads. ";

  std::atomic<int> num_done = 0;
  auto LoadFn = [&](int from_w, int to_w, int seed, int partition) {
    PartitionedSmallBankDataLoader loader(storage_adapter, from_w, to_w, seed, partition, num_partitions, num_regions);
    loader.Load();
    num_done++;
  };
  std::vector<std::thread> threads;
  int range = num_clients / num_threads + 1;
  for (int i = 0; i < num_threads; i++) {
    int range_start = i * range;
    int range_end = std::min((i + 1) * range, num_clients);
    threads.emplace_back(LoadFn, range_start, range_end, i, partition );
  }
  while (num_done < num_threads) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
  for (auto& t : threads) {
    t.join();
  }
}
}  // namespace smallbank
}  // namespace slog