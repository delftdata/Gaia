#include "workload/smallbank.h"

#include <glog/logging.h>

#include <random>
#include <set>

#include "common/proto_utils.h"
#include "execution/smallbank/transaction.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::to_string;
using std::unordered_set;

namespace slog {
namespace {

constexpr char MH[] = "mh";
constexpr char MP[] = "mp";
constexpr char TXN_MIX[] = "mix";
constexpr char HOT[] = "hot";
constexpr char SUNFLOWER_TARGET_REGIONS[] = "sunflower_target_regions";
constexpr char SUNFLOWER_TARGET_PROBABILITIES[] = "sunflower_target_probabilities";

const RawParamMap DEFAULT_PARAMS = {{MH, "50"},
                                    {MP, "50"},
                                    {TXN_MIX, "40:25:15:5:15"},
                                    {HOT, "0.0"},
                                    {SUNFLOWER_TARGET_REGIONS, ""},
                                    {SUNFLOWER_TARGET_PROBABILITIES, ""}};

struct TxnCounters {
  int total = 0;
  int sh = 0, mh = 0, sp = 0, mp = 0;
};

struct TxnStats {
  TxnCounters balance, deposit, saving, amalgamate, writecheck;
  int sent_sunflower = 0;
} stats;

int total_txn_count = 0;

template <typename G>
int NURand(G& g, int A, int x, int y) {
  std::uniform_int_distribution<> rand1(0, A);
  std::uniform_int_distribution<> rand2(x, y);
  return (rand1(g) | rand2(g)) % (y - x + 1) + x;
}

template <typename G, typename T>
T SkewedPick(G& gen, const std::vector<T>& vec, double skew) {
  int size = vec.size();
  int A = skew * size;
  int idx = NURand(gen, A, 0, size - 1);
  return vec[idx];
}

template <typename G>
bool rollWithProbability(G& gen, double x) {
  std::uniform_real_distribution<> dist(0.0, 1.0);
  double randValue = dist(gen);
  return randValue < (x / 100.0);
}

template <typename G>
int probabilityCalculator(G& gen, double prob_mh, double prob_mp) {
  bool mh = rollWithProbability(gen, prob_mh);
  bool mp = rollWithProbability(gen, prob_mp);
  int choice = 1;
  if (mh) {
    if (mp) {
      choice = 2;  // Multi home multi partition
    } else {
      choice = 3;  // Multi home single partition
    }
  } else if (mp) {
    choice = 4;  // Single home multi partition
  }
  return choice;
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

void TrackChoices(int choice, int& sh, int& mh, int& sp, int& mp) {
  switch (choice) {
    case 1:
      sh++;
      sp++;
      break;
    case 2:
      mh++;
      mp++;
      break;
    case 3:
      mh++;
      sp++;
      break;
    case 4:
      sh++;
      mp++;
      break;
  }
}
void PrintTxnTypeStats(const std::string& label, const TxnCounters& counters) {
  LOG(INFO) << label << " -> "
            << " SH: " << counters.sh << " MH: " << counters.mh << " SP: " << counters.sp << " MP: " << counters.mp
            << " TOTAL: " << counters.total;
}

void LogTxnStats(const TxnStats& stats) {
  PrintTxnTypeStats("BALANCE", stats.balance);
  PrintTxnTypeStats("DEPOSIT", stats.deposit);
  PrintTxnTypeStats("SAVING", stats.saving);
  PrintTxnTypeStats("AMALGAMATE", stats.amalgamate);
  PrintTxnTypeStats("WRITECHECK", stats.writecheck);
  LOG(INFO) << "SUNFLOWER" << " -> " << stats.sent_sunflower;
}

// For the Calvin experiment, there is a single region, so replace the regions by the replicas so that
// we generate the same workload as other experiments
int GetNumRegions(const ConfigurationPtr& config) {
  return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

}  // namespace

SmallBankWorkload::SmallBankWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica,
                                     const string& params_str, std::pair<int, int> id_slot, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      distance_ranking_(config->distance_ranking_from(region)),
      rg_(seed),
      client_txn_id_counter_(0),
      pending_balance_txn_(nullptr),
      pending_saving_txn_(nullptr),
      pending_deposit_txn_(nullptr),
      pending_writecheck_txn_(nullptr),
      pending_amalgamate_txn_(nullptr),
      previous_amalgamate_txn_(nullptr),
      sunflower_current_region_index_(0) {
  name_ = "smallbank";
  CHECK(config_->proto_config().has_smallbank_partitioning())
      << "small_bank workload is only compatible with small_bank partitioning";

  auto num_regions = GetNumRegions(config_);
  auto num_partitions = config_->num_partitions();
  auto num_clients = config_->proto_config().smallbank_partitioning().clients();

  for (int i = 0; i < num_partitions; i++) {
    vector<vector<int>> ids(num_regions);
    client_partition_map_.push_back(ids);
  }

  for (int i = 0; i < num_regions; i++) {
    vector<std::string> ids;
    sh_sp_accounts_by_region_.push_back(ids);
    sh_mp_accounts_by_region_.push_back(ids);
    sunflower_sent_regions.push_back(0);
  }

  for (int i = 0; i < num_clients; i++) {
    std::string client_name = "Client" + std::to_string(i);
    client_name.resize(24, ' ');
    client_names_by_id_[i] = client_name;

    uint32_t name_hash = murmurhash3(client_name);
    uint32_t name_partition = name_hash % num_partitions;
    uint32_t name_home = (name_hash / num_partitions) % num_regions;

    uint32_t client_partition = i % num_partitions;
    uint32_t id_home = (i / num_partitions) % num_regions;

    bool same_partition = (name_partition == client_partition);
    bool same_home = (name_home == id_home);

    if (same_partition && same_home) {
      sh_sp_accounts_by_region_[name_home].push_back(client_name);
    } else if (!same_partition && !same_home) {
      mh_mp_account_names_.push_back(client_name);
    } else if (!same_partition && same_home) {
      sh_mp_accounts_by_region_[name_home].push_back(client_name);
    } else {
      mh_sp_account_names_.push_back(client_name);
    }

    client_partition_map_[client_partition][id_home].push_back(i);
  }

  for (int region = 0; region < num_regions; ++region) {
    LOG(INFO) << "Region " << region << " sh_sp_accounts_by_region_: " << sh_sp_accounts_by_region_[region].size();
  }

  LOG(INFO) << "mh_mp_account_names_ size: " << mh_mp_account_names_.size();

  for (int region = 0; region < num_regions; ++region) {
    LOG(INFO) << "Region " << region << " sh_mp_accounts_by_region_: " << sh_mp_accounts_by_region_[region].size();
  }

  LOG(INFO) << "mh_sp_account_names_ size: " << mh_sp_account_names_.size();

  auto txn_mix_str = Split(params_.GetString(TXN_MIX), ":");
  CHECK_EQ(txn_mix_str.size(), 5) << "There must be exactly 5 values for txn mix";
  for (const auto& t : txn_mix_str) {
    txn_mix_.push_back(std::stoi(t));
  }

  std::string sunflower = params_.GetString(SUNFLOWER_TARGET_REGIONS);
  std::string sunflower_probabilities = params_.GetString(SUNFLOWER_TARGET_PROBABILITIES);
  if (IsSunflowerEnabled()) {
    auto region_mix_str = Split(sunflower, ":");
    for (const auto& t : region_mix_str) {
      region_mix_.push_back(std::stoi(t));
    }

    auto probability_mix_str = Split(sunflower_probabilities, ":");
    for (const auto& t : probability_mix_str) {
      probability_mix_.push_back(std::stoi(t));
    }
  }

  std::cout << "region_mix_: ";
  for (const auto& val : region_mix_) std::cout << val << " ";
  std::cout << std::endl;

  std::cout << "probability_mix_: ";
  for (const auto& val : probability_mix_) std::cout << val << " ";
  std::cout << std::endl;
}

std::pair<Transaction*, TransactionProfile> SmallBankWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_partition = false;
  pro.is_multi_home = false;
  pro.is_foreign_single_home = false;
  Transaction* txn = new Transaction();

  if (pending_balance_txn_ != nullptr) {
    // LOG(INFO) << "[Phase 2] Creating BALANCE transaction for returned customer ID: " << returned_first_customer_id;
    CHECK(pending_balance_txn_->keys_size() == 1);
    memcpy(&returned_first_customer_id, pending_balance_txn_->keys(0).value_entry().value().data(), sizeof(int));
    Balance(*txn, pro, 2);
    stats.balance.total++;
    pro.transaction_type = TransactionProfile::TransactionType::NOTHING;
    pending_balance_txn_ = nullptr;

  } else if (pending_deposit_txn_ != nullptr) {
    // LOG(INFO) << "[Phase 2] Creating DEPOSIT_CHECKING transaction for returned customer ID: "
    // << returned_first_customer_id;
    CHECK(pending_deposit_txn_->keys_size() == 1);
    memcpy(&returned_first_customer_id, pending_deposit_txn_->keys(0).value_entry().value().data(), sizeof(int));
    DepositChecking(*txn, pro, 2);
    stats.deposit.total++;
    pro.transaction_type = TransactionProfile::TransactionType::NOTHING;
    pending_deposit_txn_ = nullptr;

  } else if (pending_saving_txn_ != nullptr) {
    memcpy(&returned_first_customer_id, pending_saving_txn_->keys(0).value_entry().value().data(), sizeof(int));
    // LOG(INFO) << "[Phase 2] Creating TRANSACTION_SAVING txn for: " << returned_first_customer_id;
    CHECK(pending_saving_txn_->keys_size() == 1);
    TransactionSaving(*txn, pro, 2);
    stats.saving.total++;
    pro.transaction_type = TransactionProfile::TransactionType::NOTHING;
    pending_saving_txn_ = nullptr;

  } else if (pending_writecheck_txn_ != nullptr) {
    memcpy(&returned_first_customer_id, pending_writecheck_txn_->keys(0).value_entry().value().data(), sizeof(int));
    // LOG(INFO) << "[Phase 2] Creating WRITECHECK txn for: " << returned_first_customer_id;
    CHECK(pending_writecheck_txn_->keys_size() == 1);
    Writecheck(*txn, pro, 2);
    stats.writecheck.total++;
    pro.transaction_type = TransactionProfile::TransactionType::NOTHING;
    pending_writecheck_txn_ = nullptr;

  } else if (pending_amalgamate_txn_ != nullptr && previous_amalgamate_txn_ == nullptr) {
    // LOG(INFO) << "[Phase 2] Preparing AMALGAMATE txn, preserving previous transaction state.";
    pro.transaction_type = TransactionProfile::TransactionType::AMALGAMATE;
    Amalgamate(*txn, pro, 2);
    previous_amalgamate_txn_ = pending_amalgamate_txn_;  // special handling for amalgamate
    pending_amalgamate_txn_ = nullptr;

  } else if (pending_amalgamate_txn_ != nullptr && previous_amalgamate_txn_ != nullptr && params_.GetDouble(MH) != 0) {
    memcpy(&am_returned_first_customer_id, previous_amalgamate_txn_->keys(0).value_entry().value().data(), sizeof(int));
    memcpy(&am_returned_second_customer_id, pending_amalgamate_txn_->keys(0).value_entry().value().data(), sizeof(int));
    // LOG(INFO) << "[Phase 3] Creating AMALGAMATE txn for: " << am_returned_first_customer_id << "and "
    // << am_returned_second_customer_id;
    CHECK(pending_amalgamate_txn_->keys_size() == 1);
    CHECK(previous_amalgamate_txn_->keys_size() == 1);
    pending_amalgamate_txn_ = nullptr;
    previous_amalgamate_txn_ = nullptr;
    pro.transaction_type = TransactionProfile::TransactionType::NOTHING;
    Amalgamate(*txn, pro, 3);
    stats.amalgamate.total++;

  } else {
    std::discrete_distribution<> select_smallbank_txn(txn_mix_.begin(), txn_mix_.end());
    int choice = select_smallbank_txn(rg_);
    switch (choice) {
      case 0:
        // LOG(INFO) << "[Phase 1] Creating BALANCE transaction (random selection)";
        Balance(*txn, pro, 1);
        pro.transaction_type = TransactionProfile::TransactionType::BALANCE;
        break;
      case 1:
        // LOG(INFO) << "[Phase 1] Creating DEPOSIT_CHECKING transaction (random selection)";
        DepositChecking(*txn, pro, 1);
        pro.transaction_type = TransactionProfile::TransactionType::DEPOSIT_CHECKING;
        break;
      case 2:
        // LOG(INFO) << "[Phase 1] Creating TRANSACTION_SAVING transaction (random selection)";
        TransactionSaving(*txn, pro, 1);
        pro.transaction_type = TransactionProfile::TransactionType::TRANSACTION_SAVING;
        break;
      case 3:
        // LOG(INFO) << "[Phase 1] Creating AMALGAMATE transaction (random selection)";
        Amalgamate(*txn, pro, 1);
        pro.transaction_type = TransactionProfile::TransactionType::AMALGAMATE;
        break;
      case 4:
        // LOG(INFO) << "[Phase 1] Creating WRITECHECK transaction (random selection)";
        Writecheck(*txn, pro, 1);
        pro.transaction_type = TransactionProfile::TransactionType::WRITECHECK;
        break;
      default:
        LOG(FATAL) << "Invalid txn choice";
    }
  }
  total_txn_count++;
  // if (total_txn_count % 100000 == 0) {
  //   LogTxnStats(stats);
  // }

  // LOG(INFO) << "Sunflower sent region counts:";
  // for (size_t i = 0; i < sunflower_sent_regions.size(); ++i) {
  //   LOG(INFO) << "  Region[" << i << "] = " << sunflower_sent_regions[i];
  // }

  txn->mutable_internal()->set_id(client_txn_id_counter_);
  client_txn_id_counter_++;

  return {txn, pro};
}  // namespace slog

std::string SmallBankWorkload::PickAccountName(int choice) {
  double skew = params_.GetDouble(HOT);
  const auto& sunflower = params_.GetString(SUNFLOWER_TARGET_REGIONS);
  bool prob = false;
  if (IsSunflowerEnabled())
    prob = std::bernoulli_distribution(probability_mix_[sunflower_current_region_index_] / 100.0)(rg_);
  if (prob == true) stats.sent_sunflower++;
  int region = (IsSunflowerEnabled() && prob) ? (region_mix_[sunflower_current_region_index_]) : local_region_;

  switch (choice) {
    case 1:
      return SkewedPick(rg_, sh_sp_accounts_by_region_[region], skew);
    case 2:
      return SkewedPick(rg_, mh_mp_account_names_, skew);
    case 3:
      return SkewedPick(rg_, mh_sp_account_names_, skew);
    case 4:
      return SkewedPick(rg_, sh_mp_accounts_by_region_[region], skew);
    default:
      LOG(FATAL) << "Invalid account selection choice: " << choice;
  }
}

void SmallBankWorkload::GetCustomerIdByName(Transaction& txn, TransactionProfile& pro, int choice,
                                            const std::string& override_account_name) {
  std::string name = override_account_name.empty() ? PickAccountName(choice) : override_account_name;

  auto txn_adapter = std::make_shared<smallbank::TxnKeyGenStorageAdapter>(txn);
  smallbank::GetCustomerIdByNameTxn getCustomerIdByNameTxn_txn(txn_adapter, name);
  getCustomerIdByNameTxn_txn.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("getCustomerIdByName");
  procedure->add_args(name);
}

void SmallBankWorkload::Balance(Transaction& txn, TransactionProfile& pro, int phase) {
  // LOG(INFO) << "Creating Balance transaction for phase " << phase;
  CHECK(phase == 1 || phase == 2) << "Invalid phase for Balance transaction: " << phase;

  if (phase == 1) {
    int choice = probabilityCalculator(rg_, params_.GetDouble(MH), params_.GetDouble(MP));
    GetCustomerIdByName(txn, pro, choice);
    TrackChoices(choice, stats.balance.sh, stats.balance.mh, stats.balance.sp, stats.balance.mp);
    pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;

  } else {
    auto txn_adapter = std::make_shared<smallbank::TxnKeyGenStorageAdapter>(txn);
    smallbank::BalanceTxn balance_txn(txn_adapter, client_names_by_id_[returned_first_customer_id],
                                      returned_first_customer_id);
    balance_txn.Read();
    balance_txn.Write();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("balance");
    procedure->add_args(client_names_by_id_[returned_first_customer_id]);
    procedure->add_args(to_string(returned_first_customer_id));

    pro.dependency_type = TransactionProfile::DependencyType::SECOND_PHASE;
  }
}

void SmallBankWorkload::DepositChecking(Transaction& txn, TransactionProfile& pro, int phase) {
  // LOG(INFO) << "Creating DepositChecking transaction for phase " << phase;
  CHECK(phase == 1 || phase == 2) << "Invalid phase for DepositChecking transaction: " << phase;

  if (phase == 1) {
    int choice = probabilityCalculator(rg_, params_.GetDouble(MH), params_.GetDouble(MP));
    TrackChoices(choice, stats.deposit.sh, stats.deposit.mh, stats.deposit.sp, stats.deposit.mp);
    GetCustomerIdByName(txn, pro, choice);
    pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;

  } else {
    int amount = std::uniform_int_distribution<>(100, 10000)(rg_);
    auto txn_adapter = std::make_shared<smallbank::TxnKeyGenStorageAdapter>(txn);
    smallbank::DepositCheckingTxn DepositCheckingTxn_txn(txn_adapter, client_names_by_id_[returned_first_customer_id],
                                                         returned_first_customer_id, amount);
    DepositCheckingTxn_txn.Read();
    DepositCheckingTxn_txn.Write();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("depositChecking");
    procedure->add_args(client_names_by_id_[returned_first_customer_id]);
    procedure->add_args(to_string(returned_first_customer_id));
    procedure->add_args(to_string(amount));

    pro.dependency_type = TransactionProfile::DependencyType::SECOND_PHASE;
  }
}

void SmallBankWorkload::TransactionSaving(Transaction& txn, TransactionProfile& pro, int phase) {
  // LOG(INFO) << "Creating TransactionSaving transaction for phase " << phase;
  CHECK(phase == 1 || phase == 2) << "Invalid phase for TransactionSaving transaction: " << phase;

  if (phase == 1) {
    int choice = probabilityCalculator(rg_, params_.GetDouble(MH), params_.GetDouble(MP));
    TrackChoices(choice, stats.saving.sh, stats.saving.mh, stats.saving.sp, stats.saving.mp);
    GetCustomerIdByName(txn, pro, choice);
    pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;

  } else {
    int amount = std::uniform_int_distribution<>(100, 10000)(rg_);
    auto txn_adapter = std::make_shared<smallbank::TxnKeyGenStorageAdapter>(txn);
    smallbank::TransactionSavingTxn transactionSavingTxn(txn_adapter, client_names_by_id_[returned_first_customer_id],
                                                         returned_first_customer_id, amount);
    transactionSavingTxn.Read();
    transactionSavingTxn.Write();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("transactionSaving");
    procedure->add_args(client_names_by_id_[returned_first_customer_id]);
    procedure->add_args(to_string(returned_first_customer_id));
    procedure->add_args(to_string(amount));

    pro.dependency_type = TransactionProfile::DependencyType::SECOND_PHASE;
  }
}

void SmallBankWorkload::Amalgamate(Transaction& txn, TransactionProfile& pro, int phase) {
  // LOG(INFO) << "Creating Amalgamate transaction for phase " << phase;
  CHECK(phase == 1 || phase == 2 || phase == 3) << "Invalid phase for Amalgamate transaction: " << phase;
  std::random_device rd;
  std::uniform_int_distribution<> rand_partition(0, client_partition_map_.size() - 1);
  auto pick_valid_home = [&](int part, int min_clients = 1) {
    std::uniform_int_distribution<> rand_home(0, client_partition_map_[part].size() - 1);
    int home;
    do {
      home = rand_home(rg_);
    } while (client_partition_map_[part][home].size() < min_clients);
    return home;
  };

  auto pick_client_index = [&](int part, int home) {
    int size = client_partition_map_[part][home].size();
    double skew = params_.GetDouble(HOT);
    int A = skew * size;
    int index = NURand(rg_, A, 0, size - 1);
    return index;
  };

  if (phase == 1) {
    int partition = rand_partition(rg_);
    int home = local_region_;
    int id = pick_client_index(partition, home);
    amalgamate_src_ = client_names_by_id_[client_partition_map_[partition][home][id]];
    GetCustomerIdByName(txn, pro, 0, amalgamate_src_);
    pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;

  } else if (phase == 2) {
    auto num_regions = GetNumRegions(config_);
    auto num_partitions = config_->num_partitions();
    int partition1 = am_returned_first_customer_id % num_partitions;
    int id_home1 = ((am_returned_first_customer_id / num_partitions) % num_regions);
    bool is_multi_home = rollWithProbability(rg_, params_.GetDouble(MH));
    bool is_multi_partition = rollWithProbability(rg_, params_.GetDouble(MP));

    int partition2, id_home2, idx2;

    if (is_multi_partition) {
      do {
        partition2 = rand_partition(rg_);
      } while (partition2 == partition1);

      if (is_multi_home) {
        do {
          id_home2 = pick_valid_home(partition2, 1);
        } while (id_home1 == id_home2);
        stats.amalgamate.mh++;
      } else {
        id_home2 = id_home1;
        stats.amalgamate.sh++;
      }
      stats.amalgamate.mp++;
    } else {
      partition2 = partition1;
      if (is_multi_home) {
        do {
          id_home2 = pick_valid_home(partition2, 1);
        } while (id_home1 == id_home2);
        stats.amalgamate.mh++;
      } else {
        id_home2 = id_home1;
        stats.amalgamate.sh++;
      }
      stats.amalgamate.sp++;
    }
    do {
      idx2 = pick_client_index(partition2, id_home2);
    } while (am_returned_first_customer_id == idx2);
    amalgamate_dst_ = client_names_by_id_[client_partition_map_[partition2][id_home2][idx2]];
    GetCustomerIdByName(txn, pro, 0, amalgamate_dst_);
    pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;
  } else {
    auto txn_adapter = std::make_shared<smallbank::TxnKeyGenStorageAdapter>(txn);

    smallbank::AmalgamateTxn amalgamateTxn(txn_adapter, client_names_by_id_[am_returned_first_customer_id],
                                           client_names_by_id_[am_returned_second_customer_id],
                                           am_returned_first_customer_id, am_returned_second_customer_id);
    amalgamateTxn.Read();
    amalgamateTxn.Write();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("amalgamate");
    procedure->add_args(client_names_by_id_[am_returned_first_customer_id]);
    procedure->add_args(client_names_by_id_[am_returned_second_customer_id]);
    procedure->add_args(to_string(am_returned_first_customer_id));
    procedure->add_args(to_string(am_returned_second_customer_id));

    pro.dependency_type = TransactionProfile::DependencyType::SECOND_PHASE;
  }
}

void SmallBankWorkload::Writecheck(Transaction& txn, TransactionProfile& pro, int phase) {
  // LOG(INFO) << "Creating Writecheck transaction for phase " << phase;
  CHECK(phase == 1 || phase == 2) << "Invalid phase for Writecheck transaction: " << phase;

  if (phase == 1) {
    int choice = probabilityCalculator(rg_, params_.GetDouble(MH), params_.GetDouble(MP));
    TrackChoices(choice, stats.writecheck.sh, stats.writecheck.mh, stats.writecheck.sp, stats.writecheck.mp);
    GetCustomerIdByName(txn, pro, choice);
    pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;
  } else {
    int amount = std::uniform_int_distribution<>(100, 10000)(rg_);
    auto txn_adapter = std::make_shared<smallbank::TxnKeyGenStorageAdapter>(txn);
    smallbank::WritecheckTxn writecheck_txn(txn_adapter, client_names_by_id_[returned_first_customer_id],
                                            returned_first_customer_id, amount);
    writecheck_txn.Read();
    writecheck_txn.Write();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("writecheck");
    procedure->add_args(client_names_by_id_[returned_first_customer_id]);
    procedure->add_args(to_string(returned_first_customer_id));
    procedure->add_args(to_string(amount));
    pro.dependency_type = TransactionProfile::DependencyType::SECOND_PHASE;
  }
}

bool SmallBankWorkload::IsSunflowerEnabled() const { return !params_.GetString(SUNFLOWER_TARGET_REGIONS).empty(); }

void SmallBankWorkload::refreshSunflowerRegions(int64_t duration, int64_t elapsed_time) {
  if (!IsSunflowerEnabled()) {
    return;
  }

  if (sunflower_current_region_index_ + 1 < region_mix_.size() &&
      1.0 * elapsed_time / duration > 1.0 * (sunflower_current_region_index_ + 1) / region_mix_.size()) {
    sunflower_current_region_index_++;
    LogTxnStats(stats);
    LOG(INFO) << "Sunflower scenario: switching to index " << sunflower_current_region_index_ << " with region "
              << region_mix_[sunflower_current_region_index_];
  }
}
}  // namespace slog