#include "workload/tpcc.h"

#include <glog/logging.h>

#include <random>
#include <set>

#include "common/proto_utils.h"
#include "execution/tpcc/constants.h"
#include "execution/tpcc/transaction.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::to_string;
using std::unordered_set;

namespace slog {
namespace {

// Partition that is used in a single-partition transaction.
// Use a negative number to select a random partition for
// each transaction
constexpr char PARTITION[] = "sp_partition";
// Max number of regions to select warehouse from
constexpr char HOMES[] = "homes";
// Zipf coefficient for selecting regions to access in a txn. Must be non-negative.
// The lower this is, the more uniform the regions are selected
constexpr char MH_ZIPF[] = "mh_zipf";
// Colon-separated list of % of the 5 txn types. Default is: "45:43:4:4:4"
constexpr char TXN_MIX[] = "mix";
// Only send single-home transactions
constexpr char SH_ONLY[] = "sh_only";
// Modify the probability of any item on the new order list coming from a remote warehouse. Default is 0.01.
constexpr char REM_ITEM_PROB[] = "rem_item_prob";
// Modify the probability of any payment txn going to a remote warehouse. Default is 0.01.
constexpr char REM_PAYMENT_PROB[] = "rem_payment_prob";
// Skewness of the workload. A theta value between 0.0 and 1.0. Use -1 for default skewing
constexpr char SKEW[] = "skew";

// Should actually contain an equal amount of New Order & Payement. 1 Delivery, 1 Stock Level, 1 Order Status per 10 New Order txns.
// "TPC-C specification requires that 10% of New Order transactions need to access two separate warehouses, which may
// become multi-partition and/or multi-home transactions if those two warehouses are located in separate partitions (which is greater than
// 75% probability in our 4-partition set-up) or have different home regions."
const RawParamMap DEFAULT_PARAMS = {{PARTITION, "-1"}, {HOMES, "2"}, {MH_ZIPF, "0"}, {TXN_MIX, "44:44:4:4:4"},
                                    {SH_ONLY, "0"}, {REM_ITEM_PROB, "0.01"}, {REM_PAYMENT_PROB, "0.01"}, {SKEW, "-1.0"}};
//const RawParamMap DEFAULT_PARAMS = {{PARTITION, "-1"}, {HOMES, "2"}, {MH_ZIPF, "0"}, {TXN_MIX, "45:43:4:4:4"}, {SH_ONLY, "0"}}; // Not sure why they had 45% and 43%?

int new_order_count = 0;
int fsh_no = 0;
int mh_no = 0;

int payment_count = 0;
int fsh_pay = 0;
int mh_pay = 0;

int delivery_count = 0;
int fsh_del = 0;
int mh_del = 0;

int order_status_count = 0;
int fsh_os = 0;
int mh_os = 0;

int stock_level_count = 0;
int fsh_sl = 0;
int mh_sl = 0;

int total_txn_count = 0;

// TODO: Add default params from TPC-C skewness spec
int default_item_skewness = 8191; // maxItems at 100k
int default_cust_skewness = 1023; // maxCust per district at 3k

double org_item_skew = (double) default_item_skewness / tpcc::kMaxItems; // 0.08191
double org_cust_skew = (double) default_cust_skewness / tpcc::kCustPerDist; // 0.341

// Random number generator to 
template <typename G>
int NURand(G& g, int A, int x, int y) {
  std::uniform_int_distribution<> rand1(0, A);
  std::uniform_int_distribution<> rand2(x, y);
  return (rand1(g) | rand2(g)) % (y - x + 1) + x;
}

template <typename T, typename G>
T SampleOnce(G& g, const std::vector<T>& source) {
  CHECK(!source.empty());
  size_t i = std::uniform_int_distribution<size_t>(0, source.size() - 1)(g);
  return source[i];
}

// For the Calvin experiment, there is a single region, so replace the regions by the replicas so that
// we generate the same workload as other experiments
int GetNumRegions(const ConfigurationPtr& config) {
  return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

}  // namespace

TPCCWorkload::TPCCWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const string& params_str,
                           std::pair<int, int> id_slot, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      distance_ranking_(config->distance_ranking_from(region)),
      zipf_coef_(params_.GetInt32(MH_ZIPF)),
      rg_(seed),
      client_txn_id_counter_(0) {
  name_ = "tpcc";
  CHECK(config_->proto_config().has_tpcc_partitioning()) << "TPC-C workload is only compatible with TPC-C partitioning";

  auto num_regions = GetNumRegions(config_);
  if (distance_ranking_.empty()) {
    for (int i = 0; i < num_regions; i++) {
      if (i != local_region()) {
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
    CHECK_EQ(distance_ranking_.size(), num_regions * (num_regions - 1));
    size_t from = local_region() * (num_regions - 1);
    std::copy_n(distance_ranking_.begin() + from, num_regions, distance_ranking_.begin());
    distance_ranking_.resize(num_regions - 1);
  }

  CHECK_EQ(distance_ranking_.size(), num_regions - 1) << "Distance ranking size must match the number of regions";

  auto num_partitions = config_->num_partitions();
  for (int i = 0; i < num_partitions; i++) {
    vector<vector<int>> partitions(num_regions);
    warehouse_index_.push_back(partitions);
  }
  auto num_warehouses = config_->proto_config().tpcc_partitioning().warehouses();
  for (int i = 0; i < num_warehouses; i++) {
    int partition = i % num_partitions;
    int home = i / num_partitions % num_regions;
    warehouse_index_[partition][home].push_back(i + 1);
  }
  id_generator_ = TPCCIdGenerator(num_warehouses, id_slot.first, id_slot.second);

  auto txn_mix_str = Split(params_.GetString(TXN_MIX), ":");
  CHECK_EQ(txn_mix_str.size(), 5) << "There must be exactly 5 values for txn mix";
  for (const auto& t : txn_mix_str) {
    txn_mix_.push_back(std::stoi(t));
  }
}

std::pair<Transaction*, TransactionProfile> TPCCWorkload::NextTransaction() {
  //LOG(INFO) << "Creating next TPCC transaction";
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;
  pro.is_multi_partition = false;
  pro.is_multi_home = false;
  pro.is_foreign_single_home = false;

  auto num_partitions = config_->num_partitions();
  auto partition = params_.GetInt32(PARTITION);
  if (partition < 0) {
    partition = std::uniform_int_distribution<>(0, num_partitions - 1)(rg_);
  }

  const auto& selectable_w = warehouse_index_[partition][local_region()];
  CHECK(!selectable_w.empty()) << "Not enough warehouses";
  int w = SampleOnce(rg_, selectable_w);

  Transaction* txn = new Transaction();
  std::discrete_distribution<> select_tpcc_txn(txn_mix_.begin(), txn_mix_.end());
  switch (select_tpcc_txn(rg_)) {
    case 0:
      NewOrder(*txn, pro, w, partition);
      new_order_count++;
      break;
    case 1:
      Payment(*txn, pro, w, partition);
      payment_count++;
      break;
    case 2:
      OrderStatus(*txn, w);
      order_status_count++;
      break;
    case 3:
      Deliver(*txn, w);
      delivery_count++;
      break;
    case 4:
      StockLevel(*txn, w);
      stock_level_count++;
      break;
    default:
      LOG(FATAL) << "Invalid txn choice";
  }
  total_txn_count++;
  if (total_txn_count % 100000 == 0) {
    LOG(INFO) << "Current SH txn counts: Total: " << total_txn_count << " NO: " << new_order_count << " P: "<< payment_count << " OS: " << order_status_count << " D: "<< delivery_count << " SL: "<< stock_level_count;
    LOG(INFO) << "Current FSH txn counts: Total: " << total_txn_count << " NO: " << fsh_no << " P: " << fsh_pay << " OS: " << fsh_os << " D: " << fsh_del << " SL: "<< fsh_sl;
    LOG(INFO) << "Current MH txn counts: Total: " << total_txn_count << " NO: " << mh_no << " P: " << mh_pay << " OS: " << mh_os << " D: " << mh_del << " SL: "<< mh_sl;
    LOG(INFO) << "Current SH txn percentages: NO: " << 100*new_order_count/(double)total_txn_count << " P: " << 100*payment_count/(double)total_txn_count << " OS: " << 100*order_status_count/(double)total_txn_count << " D: " << 100*delivery_count/(double)total_txn_count << " SL: " << 100*stock_level_count/(double)total_txn_count;
    LOG(INFO) << "Current FSH txn percentages: NO: " << 100*fsh_no/(double)total_txn_count << " P: " << 100*fsh_pay/(double)total_txn_count << " OS: " << 100*fsh_os/(double)total_txn_count << " D: " << 100*fsh_del/(double)total_txn_count << " SL: " << 100*fsh_sl/(double)total_txn_count;
    LOG(INFO) << "Current MH txn percentages: NO: " << 100*mh_no/(double)total_txn_count << " P: " << 100*mh_pay/(double)total_txn_count << " OS: " << 100*mh_os/(double)total_txn_count << " D: " << 100*mh_del/(double)total_txn_count << " SL: " << 100*mh_sl/(double)total_txn_count;
  }

  txn->mutable_internal()->set_id(client_txn_id_counter_);
  client_txn_id_counter_++;

  return {txn, pro};
}

void TPCCWorkload::NewOrder(Transaction& txn, TransactionProfile& pro, int w_id, int partition) {
  //LOG(INFO) << "Making new NewOrder txn";
  double skew = params_.GetDouble(SKEW);
  int final_item_skew, final_cust_skew;
  if (skew == -1.0) {
    final_item_skew = org_item_skew;
    final_cust_skew = org_cust_skew;
  } else {
    final_item_skew = skew * tpcc::kMaxItems;
    final_cust_skew = skew * tpcc::kCustPerDist;
  }
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);
  auto remote_warehouses = SelectRemoteWarehouses(partition);
  int d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  int c_id = NURand(rg_, final_cust_skew, 1, tpcc::kCustPerDist);
  int o_id = id_generator_.NextOId(w_id, d_id);
  // Partition ID on global scale
  int i_w_id = partition + static_cast<int>(local_region() * config_->num_partitions()) + 1;
  auto datetime = std::chrono::system_clock::now().time_since_epoch().count();
  std::array<tpcc::NewOrderTxn::OrderLine, tpcc::kLinePerOrder> ol;
  std::bernoulli_distribution is_remote(params_.GetDouble(REM_ITEM_PROB));
  std::uniform_int_distribution<> quantity_rnd(1, 10);
  int supply_w_ids[tpcc::kLinePerOrder];
  std::set<int> unique_regions;
  for (size_t i = 0; i < tpcc::kLinePerOrder; i++) {
    auto supply_w_id = w_id;
    if (is_remote(rg_) && !remote_warehouses.empty()) {
      supply_w_id = remote_warehouses[i % remote_warehouses.size()];
      pro.is_multi_home = true;
    }
    ol[i] = tpcc::NewOrderTxn::OrderLine({
        .id = static_cast<int>(i),
        .supply_w_id = supply_w_id,
        .item_id = NURand(rg_, final_item_skew, 1, tpcc::kMaxItems),
        .quantity = quantity_rnd(rg_),
    });
    supply_w_ids[i] = supply_w_id;
    int supply_region = GetRegionFromWarehouse(supply_w_id);
    //LOG(INFO) << "Current Region: " << supply_region;
    unique_regions.insert(supply_region);
  }
  if (pro.is_multi_home) {
    mh_no++;
  }
  // Count number of unique '.supply_w_id's in all Order Lines to get whether we have a FSH or a MH txn
  // Note FSH txns will now be counted both as FSH and MH txns
  //for (int i = 0; i < tpcc::kLinePerOrder; i++) {
  //  LOG(INFO) << "Current SH txn counts: Total: "
  //}
  if (unique_regions.size() == 1 && pro.is_multi_home) {
    pro.is_foreign_single_home = true;
    fsh_no++;
  }

  tpcc::NewOrderTxn new_order_txn(txn_adapter, w_id, d_id, c_id, o_id, datetime, i_w_id, ol);
  new_order_txn.Read();
  new_order_txn.Write();
  txn_adapter->Finialize();

  // Serialize the new Order to a string
  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("new_order");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(o_id));
  procedure->add_args(to_string(datetime));
  procedure->add_args(to_string(i_w_id));
  for (const auto& l : ol) {
    auto order_lines = txn.mutable_code()->add_procedures();
    order_lines->add_args(to_string(l.id));
    order_lines->add_args(to_string(l.supply_w_id));
    order_lines->add_args(to_string(l.item_id));
    order_lines->add_args(to_string(l.quantity));
  }
}

void TPCCWorkload::Payment(Transaction& txn, TransactionProfile& pro, int w_id, int partition) {
  double skew = params_.GetDouble(SKEW);
  int final_cust_skew;
  if (skew == -1.0) {
    final_cust_skew = org_cust_skew;
  } else {
    final_cust_skew = skew * tpcc::kCustPerDist;
  }
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  auto remote_warehouses = SelectRemoteWarehouses(partition);
  std::uniform_int_distribution<> d_id_rnd(1, tpcc::kDistPerWare);
  int c_id = NURand(rg_, final_cust_skew, 1, tpcc::kCustPerDist);
  auto datetime = std::chrono::system_clock::now().time_since_epoch().count();
  std::uniform_int_distribution<> quantity_rnd(1, 10);
  std::bernoulli_distribution is_remote(params_.GetDouble(REM_PAYMENT_PROB));

  auto d_id = d_id_rnd(rg_);
  auto c_w_id = w_id;
  auto c_d_id = d_id;
  auto h_id = id_generator_.NextHId(w_id, d_id);
  auto amount = std::uniform_int_distribution<int64_t>(100, 500000)(rg_);
  if (is_remote(rg_) && !remote_warehouses.empty()) {
    c_w_id = SampleOnce(rg_, remote_warehouses);
    c_d_id = d_id_rnd(rg_);
    // Note: all Payment txns that are FSH, are also counted as MH
    pro.is_foreign_single_home = true;
    pro.is_multi_home = true;
    mh_pay++;
    fsh_pay++;
  }
  tpcc::PaymentTxn payment_txn(txn_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
  payment_txn.Read();
  payment_txn.Write();
  // Imitating a commit using Finalize()
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("payment");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(c_w_id));
  procedure->add_args(to_string(c_d_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(amount));
  procedure->add_args(to_string(datetime));
  procedure->add_args(to_string(h_id));
}

void TPCCWorkload::OrderStatus(Transaction& txn, int w_id) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  double skew = params_.GetDouble(SKEW);
  int final_cust_skew;
  if (skew == -1.0) {
    final_cust_skew = org_cust_skew;
  } else {
    final_cust_skew = skew * tpcc::kCustPerDist;
  }
  auto d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  int c_id = NURand(rg_, final_cust_skew, 1, tpcc::kCustPerDist);
  auto max_o_id = id_generator_.max_o_id();
  auto o_id = std::uniform_int_distribution<>(max_o_id - 5, max_o_id)(rg_);

  tpcc::OrderStatusTxn order_status_txn(txn_adapter, w_id, d_id, c_id, o_id);
  order_status_txn.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("order_status");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(o_id));
}

void TPCCWorkload::Deliver(Transaction& txn, int w_id) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);
  double skew = params_.GetDouble(SKEW);
  int final_cust_skew;
  if (skew == -1.0) {
    final_cust_skew = org_cust_skew;
  } else {
    final_cust_skew = skew * tpcc::kCustPerDist;
  }
  int c_id = NURand(rg_, final_cust_skew, 1, tpcc::kCustPerDist);
  auto d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  auto no_o_id = id_generator_.NextNOOId(w_id, d_id);
  auto datetime = std::chrono::system_clock::now().time_since_epoch().count();
  auto carrier = std::uniform_int_distribution<>(1, 10)(rg_);
  tpcc::DeliverTxn deliver(txn_adapter, w_id, d_id, no_o_id, c_id, carrier, datetime);
  deliver.Read();
  deliver.Write();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("deliver");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(no_o_id));
  procedure->add_args(to_string(c_id));
  procedure->add_args(to_string(carrier));
  procedure->add_args(to_string(datetime));
}

void TPCCWorkload::StockLevel(Transaction& txn, int w_id) {
  auto txn_adapter = std::make_shared<tpcc::TxnKeyGenStorageAdapter>(txn);

  auto d_id = std::uniform_int_distribution<>(1, tpcc::kDistPerWare)(rg_);
  auto o_id = id_generator_.max_o_id();
  std::array<int, tpcc::StockLevelTxn::kTotalItems> i_ids;
  std::uniform_int_distribution<> i_id_rnd(1, tpcc::kMaxItems);
  for (size_t i = 0; i < i_ids.size(); i++) {
    i_ids[i] = i_id_rnd(rg_);
  }
  tpcc::StockLevelTxn stock_level(txn_adapter, w_id, d_id, o_id, i_ids);
  stock_level.Read();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("stock_level");
  procedure->add_args(to_string(w_id));
  procedure->add_args(to_string(d_id));
  procedure->add_args(to_string(o_id));
  auto items = txn.mutable_code()->add_procedures();
  for (auto i_id : i_ids) {
    items->add_args(to_string(i_id));
  }
}

std::vector<int> TPCCWorkload::SelectRemoteWarehouses(int partition) {
  if (params_.GetInt32(SH_ONLY) == 1) {
    return {SampleOnce(rg_, warehouse_index_[partition][local_region()])};
  }

  auto num_regions = GetNumRegions(config_);
  auto max_num_homes = std::min(params_.GetInt32(HOMES), num_regions);
  if (max_num_homes < 2) {
    return {};
  }
  auto num_homes = std::uniform_int_distribution{2, max_num_homes}(rg_);
  auto remote_warehouses = zipf_sample(rg_, zipf_coef_, distance_ranking_, num_homes - 1);

  for (size_t i = 0; i < remote_warehouses.size(); i++) {
    auto r = remote_warehouses[i];
    remote_warehouses[i] = SampleOnce(rg_, warehouse_index_[partition][r]);
  }

  return remote_warehouses;
}

int TPCCWorkload::GetRegionFromWarehouse(int warehouse_id) {
  auto num_partitions = config_->num_partitions();
  auto num_regions = config_->num_regions();

  int partition = (warehouse_id - 1) % num_partitions; // Warehouses are 1-indexed
  int region = ((warehouse_id - 1) / num_partitions) % num_regions;

  return region;
}

}  // namespace slog