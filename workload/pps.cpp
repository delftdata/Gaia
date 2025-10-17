#include "workload/pps.h"

#include <glog/logging.h>

#include <random>
#include <set>

#include "common/proto_utils.h"
#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::to_string;
using std::unordered_set;

namespace slog {
namespace {

// Percentage of multi-home transactions.
constexpr char MH_PCT[] = "mh";
// Percentage of multi-partition transactions.
constexpr char MP_PCT[] = "mp";
// Colon-separated list of % of the existing txn types.
constexpr char TXN_MIX[] = "mix";
// Skewness of the workload. A theta value between 0.0 and 1.0.
constexpr char HOT[] = "hot";
// Target region for the sunflower scenario.
constexpr char SUNFLOWER_TARGET[] = "sunflower";
// If set to 1, every SH transaction will be sent to the nearest region.
constexpr char NEAREST[] = "nearest";

// By default, the focus will be on the order_product and get_parts_by_product transactions.
const RawParamMap DEFAULT_PARAMS = {{MH_PCT, "0"}, {MP_PCT, "0"}, {TXN_MIX, "80:8:8:2:2"}, 
                                    {HOT, "0.0"}, {SUNFLOWER_TARGET, "-1"}, {NEAREST, "1"}};

// Sample a random number in the range [x, y] non-uniformly.
// For small values of A, the distribution is more uniform.
// For large values of A, the distribution is more skewed.
template <typename G>
int NURand(G& g, double skew, int x, int y) {
  int A = skew * (y - x + 1);
  std::uniform_int_distribution<> rand1(0, A);
  std::uniform_int_distribution<> rand2(x, y);
  return (rand1(g) | rand2(g)) % (y - x + 1) + x;
}

// Sample a random element from the source vector.
template <typename T, typename G>
T SampleOnce(G& g, const std::vector<T>& source) {
  CHECK(!source.empty());
  size_t i = std::uniform_int_distribution<size_t>(0, source.size() - 1)(g);
  return source[i];
}

// Treate Calvin's case separately, as it has a single region but potentially multiple replicas.
// We'll replace the regions by the replicas so that we generate the same workload as other experiments.
int GetNumRegions(const ConfigurationPtr& config) {
  return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

std::string showArray(const std::vector<int>& parts) {
  if (parts.empty()) {
    return "[]";
  }
  std::ostringstream oss;
  oss << "[" << parts[0];
  for (size_t i = 1; i < parts.size(); i++) {
    oss << ", " << parts[i];
  }
  oss << "]";
  return oss.str();
}

vector<string> split(const std::string& str, const std::string& delims) {
  vector<string> res;
  string token;
  size_t pos = NextToken(token, str, delims, 0);
  while (pos != std::string::npos) {
    res.push_back(token);
    pos = NextToken(token, str, delims, pos);
  }
  return res;
}

}  // namespace

PPSWorkload::PPSWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const string& params_str,
                           std::pair<int, int> id_slot, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      id_slot_(id_slot),
      local_region_((config->num_regions() == 1 ? replica : region)),
      local_replica_(replica),
      rg_(seed),
      client_txn_id_counter_(0),
      num_regions_(GetNumRegions(config)),
      num_partitions_(config->num_partitions()),
      prev_txn_(nullptr),
      num_products_(config->proto_config().pps_partitioning().products()),
      num_parts(config->proto_config().pps_partitioning().parts()),
      num_suppliers_(config->proto_config().pps_partitioning().suppliers()),
      sunflower_redirect_pct_(0),
      sunflower_target_region_(params_.GetInt32(SUNFLOWER_TARGET)) {
  name_ = "pps";
  CHECK(config_->proto_config().has_pps_partitioning()) << "PPS workload is only compatible with PPS partitioning";
  LOG(INFO) << "PPS workload created (id_slot=" << id_slot_.first << "/" << id_slot_.second
            << ", region=" << static_cast<int>(local_region_) << ", replica=" << static_cast<int>(local_replica_) << ", params=" << params_str
            << ", num_products=" << num_products_ << ", num_parts=" << num_parts
            << ", num_suppliers=" << num_suppliers_ << ". seed=" << seed << ")";

  // Reserve space for the parts buffered for the dependent transactions order_product.
  parts_to_retrieve_.reserve(pps::kPartsPerProduct);
  for (int i = 0; i < pps::kPartsPerProduct; i++) {
    parts_to_retrieve_.push_back(i + 1);
  }
}

std::pair<Transaction*, TransactionProfile> PPSWorkload::NextTransaction() {
  TransactionProfile pro;
  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_partition = false;
  pro.is_multi_home = false;
  pro.is_foreign_single_home = false;
  pro.dependency_type = TransactionProfile::DependencyType::NONE;
  txn_total_++;

  Transaction* txn = new Transaction();

  // If the previous transaction was the first phase of the dependent transaction order_product, 
  // we need to further generate the second phase as the next transaction.
  if (prev_txn_ != nullptr) {
    CHECK(prev_txn_->keys_size() == pps::kPartsPerProduct) << "First phase order_product returned incorrect number of keys";
    for (int i = 0; i < pps::kPartsPerProduct; i++) {
      const std::size_t offset = 5;
      int index;
      std::memcpy(&index, prev_txn_->keys(i).key().data() + offset, sizeof(int));
      CHECK(index > 0 && index <= pps::kPartsPerProduct) << "Invalid index for part: " << index;
      memcpy(&parts_to_retrieve_[index - 1], prev_txn_->keys(i).value_entry().value().data(), sizeof(int));
    }
    
    auto& arg = prev_txn_->mutable_code()->procedures(0).args(1);
    int product_id = std::stoi(arg);
    CHECK(product_id > 0 && product_id <= num_products_) << "Invalid product id: " << product_id;
    
    prev_txn_ = nullptr;
    
    pro.dependency_type = TransactionProfile::DependencyType::SECOND_PHASE;
    orderProductTransaction(*txn, pro, product_id);
  } else {
    // Select a random transaction type according to the distribution.
    std::vector<int> txn_mix;
    auto txn_mix_str = Split(params_.GetString(TXN_MIX), ":");
    CHECK_EQ(txn_mix_str.size(), 5) << "There must be exactly 5 values for txn mix";
    for (const auto& t : txn_mix_str) {
      txn_mix.push_back(std::stoi(t));
    }
    std::discrete_distribution<> select_pps_txn(txn_mix.begin(), txn_mix.end());
    switch (select_pps_txn(rg_)) {
      case 0:
        pro.dependency_type = TransactionProfile::DependencyType::FIRST_PHASE;
        order_product_1st_phase_total_++;
        orderProductTransaction(*txn, pro, -1);
        break;
      case 1:
        get_parts_by_product_total_++;
        getPartsByProductTransaction(*txn, pro);
        break;
      case 2:
        update_product_part_total_++;
        updateProductPartTable(*txn, pro);
        break;
      case 3:
        get_product_total_++;
        getProductTransaction(*txn, pro);
        break;
      case 4: 
        get_part_total_++; 
        getPartTransaction(*txn, pro);
        break;
      default:
        LOG(FATAL) << "Unknown transaction type";
    }
  }

  txn->mutable_internal()->set_id(client_txn_id_counter_);
  client_txn_id_counter_++;

  return {txn, pro};
}

void PPSWorkload::orderProductTransaction(Transaction& txn, TransactionProfile& pro, int product_id) {
  if (product_id == -1) {
    getPartsByProductTransaction(txn, pro, true);
  } else  {
    int category = ((product_id - 1) % (4 * num_partitions_ * num_regions_)) / (num_partitions_ * num_regions_);
    CHECK(category >= 0 && category < 4) << "Invalid category for product: " << product_id;
    order_product_2nd_phase_cateogry_total_[category]++;

    VLOG(1) << "[Create order_product 2nd] product=" << product_id << ", parts=" << showArray(parts_to_retrieve_);
    auto txn_adapter = std::make_shared<pps::TxnKeyGenStorageAdapter>(txn);
    pps::OrderProduct order_product_txn(txn_adapter, product_id, parts_to_retrieve_);
    order_product_txn.Read();
    order_product_txn.Write();
    txn_adapter->Finialize();

    auto procedure = txn.mutable_code()->add_procedures();
    procedure->add_args("order_product");
    procedure->add_args(to_string(product_id));
    for (const auto& part_id : parts_to_retrieve_) {
      procedure->add_args(to_string(part_id));
    }
  }
}

void PPSWorkload::getPartsByProductTransaction(Transaction& txn, TransactionProfile& pro, bool is_part_of_order_product) {
  int product_id = selectProduct();

  VLOG(1) << (is_part_of_order_product ? "[Create order_product 1nd] product=" : "[Create get_parts_by_product] product=") << product_id;
  auto txn_adapter = std::make_shared<pps::TxnKeyGenStorageAdapter>(txn);
  pps::GetPartsByProduct get_parts_by_product_txn(txn_adapter, product_id);
  get_parts_by_product_txn.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("get_parts_by_product");
  procedure->add_args(to_string(product_id));
}

void PPSWorkload::updateProductPartTable(Transaction& txn, TransactionProfile& pro) {
  int product_id = selectProduct();

  VLOG(1) << "[Create update_product_part] product=" << product_id;
  auto txn_adapter = std::make_shared<pps::TxnKeyGenStorageAdapter>(txn);
  pps::UpdateProductPart update_product_part_txn(txn_adapter, product_id);
  update_product_part_txn.Read();
  update_product_part_txn.Write();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("update_product_part");
  procedure->add_args(to_string(product_id));
}

void PPSWorkload::getProductTransaction(Transaction& txn, TransactionProfile& pro) {
  int product_id = selectProduct();

  VLOG(1) << "[Create get_product] product=" << product_id;
  auto txn_adapter = std::make_shared<pps::TxnKeyGenStorageAdapter>(txn);
  pps::GetProduct get_product_txn(txn_adapter, product_id);
  get_product_txn.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("get_product");
  procedure->add_args(to_string(product_id));
}

void PPSWorkload::getPartTransaction(Transaction& txn, TransactionProfile& pro) {
  int part_id = std::uniform_int_distribution<>(1, num_parts)(rg_);

  VLOG(1) << "[Create get_part] part=" << part_id;
  auto txn_adapter = std::make_shared<pps::TxnKeyGenStorageAdapter>(txn);
  pps::GetPart get_part_txn(txn_adapter, part_id);
  get_part_txn.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("get_part");
  procedure->add_args(to_string(part_id));
}

int PPSWorkload::selectProduct() {
  /* 
   * The product partitioning and mastering is as follows (e.g., for 4 partitions and 3 regions):
   * =============================================================================================
   * | partition / region |  0  |  1  |  2  |  0  |  1  |  2  |  0  |  1  |  2  |  0  |  1  |  2  |
   * ---------------------------------------------------------------------------------------------
   * | 0                  |  1  |  5  |  9  | 13  | 17  | 21  | 25  | 29  | 33  | 37  | 41  | 45  |
   * | 1                  |  2  |  6  | 10  | 14  | 18  | 22  | 26  | 30  | 34  | 38  | 42  | 46  |
   * | 2                  |  3  |  7  | 11  | 15  | 19  | 23  | 27  | 31  | 35  | 39  | 43  | 47  |
   * | 3                  |  4  |  8  | 12  | 16  | 20  | 24  | 28  | 32  | 36  | 40  | 44  | 48  |
   * =============================================================================================
   * 
   * We will divide the product into sequential block of size (num_partitions_ * num_regions_).
   * Each block will be part of one of the following cateogories:
   *  - Cateogory I: The products have parts from the same region and partition.
   *  - Cateogory II: The products have parts from the same region but different partitions.
   *  - Cateogory III: The products have parts from different regions but the same partition.
   *  - Cateogory IV: The products have parts from different regions and different partitions.
   * We will assign the cateogires to block periodically. Examples:
   *  - The first block (products 1-48) and fifth block (products 193-240) are both of cateogory I.
   * 
   * Important rules for the product selection:
   *  1. The cateogry is determined by is_mh and is_mp.
   *  2. The region within a block is determined by the locality of the client.
   *  3. The block and the partition within a block are uniformly selected.
   */

  // Cateogry Selection Procedure:
  // 1. We select the cateogry based on the MH and MP percentages.
  int mh = params_.GetInt32(MH_PCT);
  int mp = params_.GetInt32(MP_PCT);
  bool is_mh = std::bernoulli_distribution(mh / 100.0)(rg_);
  bool is_mp = std::bernoulli_distribution(mp / 100.0)(rg_);

  // Region Selection Procedure:
  // 1. If the sunflower scenario is enabled, follow the sunflower pattern with the given percentage.
  // 2. If the transaction is single-home, select a product owned by the local region.
  // 3. If the transaction is multi-home, select a product owned by a random region.
  bool follow_sunflower = (sunflower_target_region_ == -1 ? false :
                            std::bernoulli_distribution(sunflower_redirect_pct_ / 100.0)(rg_));
  int nearest = params_.GetInt32(NEAREST);
  int chosen_region = (follow_sunflower ? sunflower_target_region_ :
                       (!is_mh && nearest == 1) ? local_region_ : std::uniform_int_distribution<>(0, num_regions_ - 1)(rg_));
  
  // Block and Partition Selection Procedure:
  // 1. We will select the block and partition using the NURand skewed distribution.
  int num_products = config_->proto_config().pps_partitioning().products();
  int total_blocks_per_cateogory = num_products / (4 * num_partitions_ * num_regions_);
  double skew = params_.GetDouble(HOT);
  int choice = NURand(rg_, skew, 0, total_blocks_per_cateogory * num_partitions_ - 1);
  int chosen_block_within_cateogory = choice / num_partitions_;
  int chosen_partition = choice % num_partitions_;
  
  int cateogry = (is_mh << 1) | is_mp;  // 0: I, 1: II, 2: III, 3: IV
  int product_id = chosen_block_within_cateogory * (4 * num_partitions_ * num_regions_) + chosen_region * num_partitions_ + chosen_partition + 1 + (cateogry * num_partitions_ * num_regions_);
  CHECK(product_id > 0 && product_id <= num_products) << "Invalid product id: " << product_id;
  return product_id;
}

void PPSWorkload::printStatistics() const {
  LOG(INFO) << "========================================";
  LOG(INFO) << "Statistics for generator " << id_slot_.first << "/" << id_slot_.second;
  LOG(INFO) << "Total: " << txn_total_;
  LOG(INFO) << "[Order Product 1st Phase] Total: " << order_product_1st_phase_total_;
  LOG(INFO) << "[Order Product 2nd Phase] "
    << "SH SP:" << order_product_2nd_phase_cateogry_total_[0] << ", "
    << "SH MP:" << order_product_2nd_phase_cateogry_total_[1] << ", "
    << "MH SP:" << order_product_2nd_phase_cateogry_total_[2] << ", "
    << "MH MP:" << order_product_2nd_phase_cateogry_total_[3];
  LOG(INFO) << "[Get Parts By Product] Total: " << get_parts_by_product_total_;
  LOG(INFO) << "[Update Product Part] Total: " << update_product_part_total_;
  LOG(INFO) << "[Get Product] Total: " << get_product_total_;
  LOG(INFO) << "[Get Part] Total: " << get_part_total_; 
  LOG(INFO) << "========================================";
}

void PPSWorkload::updateForSunflowerScenario(int64_t duration, int64_t elapsed_time) {
  if (sunflower_target_region_ == -1) {
    return;
  }

  // Increase the percentage of transactions to the target linearly by 10%.
  if (1.0 * elapsed_time / duration > sunflower_redirect_pct_ / 100.0) {
    sunflower_redirect_pct_ += 10;
    LOG(INFO) << "Sunflower scenario: redirecting " << sunflower_redirect_pct_ << "% of transactions to region "
              << sunflower_target_region_;
  }
}

}  // namespace slog