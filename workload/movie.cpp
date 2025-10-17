#include "workload/movie.h"

#include <glog/logging.h>

#include <random>
#include <set>

#include "common/proto_utils.h"
#include "execution/movie/storage_adapter.h"
#include "execution/movie/transaction.h"
#include "execution/movie/constants.h"
#include "execution/movie/load_tables.h"

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
constexpr char HOMES[] = "homes";

constexpr char MH_CHANCE[] = "mh";
constexpr char MP_CHANCE[] = "mp";
// Skewness of the workload. A theta value between 0.0 and 1.0. Use 0.0 for defaul skewing
constexpr char SKEW[] = "skew";
constexpr char SUNFLOWER[] = "sunflower";
constexpr char SF_FRACTION[] = "sf_fraction";
constexpr char SF_HOME[] = "sf_home";


const RawParamMap DEFAULT_PARAMS = {{PARTITION, "-1"}, {HOMES, "2"}, {SKEW, "0.0"}, {MH_CHANCE, "25"}, {MP_CHANCE, "50"}, {SUNFLOWER, "0"}, {SF_FRACTION, "0.9"}, {SF_HOME, "0"}};



int total_txn_count = 0;
int mh_txn_count = 0;
int sh_txn_count = 0;
int mp_txn_count = 0;
int sp_txn_count = 0;



template <typename G>
long NURand(G& g, long A, long x, long y) {
  std::uniform_int_distribution<long> rand1(0, A);
  std::uniform_int_distribution<long> rand2(x, y);
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

MovieWorkload::MovieWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const string& params_str,
                           std::pair<int, int> id_slot, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      rg_(seed),
      client_txn_id_counter_(0),
      skew_(params_.GetDouble(SKEW)) {
  name_ = "movie";
  CHECK(config_->proto_config().has_movie_partitioning()) << "Movie workload is only compatible with movie partitioning";

  auto num_regions = GetNumRegions(config_);
  
  auto num_partitions = config_->num_partitions();
}

std::pair<Transaction*, TransactionProfile> MovieWorkload::NextTransaction() {
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

  auto mhpercent = params_.GetInt32(MH_CHANCE); // Multihome transaction chance from args
  auto mppercent = params_.GetInt32(MP_CHANCE); // Multipartition transaction chance from args
  double mhfraction = mhpercent/100.0;
  double mpfraction = mppercent/100.0;
  auto sunflowerarg = params_.GetInt32(SUNFLOWER);
  bool sunflower = sunflowerarg == 1;
  auto sunflowerhome = params_.GetInt32(SF_HOME);
  auto sunflowerfraction = params_.GetDouble(SF_FRACTION);
  int UserHomeIfSunflower;

  bool mh = std::uniform_real_distribution<>(0.0, 1.0)(rg_) < mhfraction;
  bool mp = std::uniform_real_distribution<>(0.0, 1.0)(rg_) < mpfraction;

  // Calculate the home region for the User record, if sunflower is active
  if(std::uniform_real_distribution<>(0.0, 1.0)(rg_) < sunflowerfraction || config_->num_regions() == 1) {
    UserHomeIfSunflower = sunflowerhome;
  } else {
    UserHomeIfSunflower = std::abs(sunflowerhome - 1); //Only works with 2 homes
  }



  Transaction* txn = new Transaction();
  NewReview(*txn, pro, sunflower, UserHomeIfSunflower, mh, mp);
  total_txn_count++;
  if(total_txn_count % 100000 == 0) {
    //LOG(INFO) << "Current txn counts: Total: " << total_txn_count;
  }

  txn->mutable_internal()->set_id(client_txn_id_counter_);
  client_txn_id_counter_++;

  return {txn, pro};
}

std::string generate_random_string(size_t length, std::mt19937 rg_) {
    const std::string charset =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::uniform_int_distribution<> dis(0, charset.size() - 1);

    std::string result;
    result.reserve(length);

    for (size_t i = 0; i < length; ++i) {
        result += charset[dis(rg_)];
    }

    return result;
}

void MovieWorkload::NewReview(Transaction& txn, TransactionProfile& pro, bool sunflower, int sunflowerhome, bool multihome, bool multipart) {
  auto txn_adapter = std::make_shared<movie::TxnKeyGenStorageAdapter>(txn);
  int homes = config_->num_regions();
  int partitions = config_->num_partitions();
  int moviecount = sizeof(movie::movies) / sizeof(movie::movies[0]);
  int movieidxmax = moviecount - 1;

  int user_id;
  if(sunflower) {
    user_id = random_id_for_home(sunflowerhome, 1000);
  } else {
    if(multihome) {
      user_id = NURand(rg_, skew_*1000, 0, 1000);
    } else {
      user_id = random_id_for_home(local_region_, 1000);
    }
    
  }
  int64_t reviewid;
  int64_t titleindex;

  if(multihome && homes > 1) {
    if(multipart && partitions > 1) {
      reviewid = diff_home_diff_part(user_id, 1000);
      titleindex = diff_home_diff_part(user_id, movieidxmax);
    }
    else {
      reviewid = diff_home_same_part(user_id, 1000);
      titleindex = diff_home_same_part(user_id, movieidxmax);
    }
  } else {

    if (multipart && partitions > 1) {
      reviewid = same_home_diff_part(user_id, 1000);
      titleindex = same_home_diff_part(user_id, movieidxmax);
    }
    else {
      reviewid = same_home_same_part(user_id, 1000);
      titleindex = same_home_same_part(user_id, movieidxmax);
    }
  }
  int64_t req_id = reviewid;
  int64_t review_id = reviewid;
  int rating = std::uniform_int_distribution<>(0, 10)(rg_);
  std::string text = generate_random_string(256, rg_);
  int64_t timestamp = reviewid;
  std::string userid = std::to_string(user_id);
  
  
  movie::addLeadingZeros(12, userid);

  std::string username = userid + "_username";
  std::string titleidx = std::to_string(titleindex);
  movie::addLeadingZeros(12, titleidx);


  std::string title = titleidx + "_" + movie::movies[titleindex];
  movie::addTrailingSpaces(100, title);

  int reviewhome = calculatehome(review_id);
  int userhome = calculatehome(user_id);
  int reviewpart = calculatepart(review_id);
  int userpart = calculatepart(user_id);
  
  if(reviewhome != userhome) {
    pro.is_multi_home = true;
    mh_txn_count++;
  } else{
    sh_txn_count++;
  }
  if(reviewpart != userpart) {
    pro.is_multi_partition = true;
    mp_txn_count++;
  } else {
    sp_txn_count++;
  }


  movie::NewReviewTxn new_review_txn(txn_adapter, req_id, rating, username, title, timestamp, review_id, text);
  new_review_txn.Read();
  new_review_txn.Write();
  txn_adapter->Finialize();

  auto procedure = txn.mutable_code()->add_procedures();
  procedure->add_args("newReview");
  procedure->add_args(to_string(req_id));
  procedure->add_args(to_string(rating));
  procedure->add_args(username);
  procedure->add_args(title);
  procedure->add_args(to_string(timestamp));
  procedure->add_args(to_string(review_id));
  procedure->add_args(text);
}

// Calculate home region of ID
int MovieWorkload::calculatehome(int id) {
  int num_partitions = config_->num_partitions();
  int num_regions = config_->num_regions();
  return (id / num_partitions) % num_regions;
}

// Calculate partition of ID
int MovieWorkload::calculatepart(int id) {
  int num_partitions = config_->num_partitions();
  int num_regions = config_->num_regions();
  return id % num_partitions;
}

// Generate a random ID belonging to the given home region
long MovieWorkload::random_id_for_home(int h_req, long MAX_ID)  
{
  // Collect vector of possible IDs
  vector<long> candidates;
  for(long i = 0L; i <= MAX_ID; i++) {
    int h1 = calculatehome(i);
    if(h1 == h_req) {
      candidates.push_back(i);
    }
  }
  long maxidx = candidates.size() - 1;
  // Choose one of the IDs using a distribution
  long chosen = NURand(rg_, skew_*maxidx, 0, maxidx);
  return candidates.at(chosen);
}

// Generates a new ID with the same home region and partition as the given ID
long MovieWorkload::same_home_same_part(long id, long MAX_ID)
{
    const int h0 = calculatehome(id);
    const int p0 = calculatepart(id);
    // Collect vector of possible IDs
    vector<long> candidates;
    for(long i = 0L; i <= MAX_ID; i++) {
      int h1 = calculatehome(i);
      int p1 = calculatepart(i);
      if(h1 == h0 && p0 == p1) {
        candidates.push_back(i);
      }
    }
    long maxidx = candidates.size() - 1;
    // Choose one of the IDs using a distribution
    long chosen = NURand(rg_, skew_*maxidx, 0, maxidx);
    return candidates.at(chosen);
}

// Generates a new ID with the same home region and a different partition to the given ID
long MovieWorkload::same_home_diff_part(long id, long MAX_ID)
{
  const int h0 = calculatehome(id);
  const int p0 = calculatepart(id);
  // Collect vector of possible IDs
  vector<long> candidates;
  for(long i = 0L; i <= MAX_ID; i++) {
    int h1 = calculatehome(i);
    int p1 = calculatepart(i);
    if(h1 == h0 && p0 != p1) {
      candidates.push_back(i);
    }
  }
  long maxidx = candidates.size() - 1;
  // Choose one of the IDs using a distribution
  long chosen = NURand(rg_, skew_*maxidx, 0, maxidx);
  return candidates.at(chosen);
}

// Generates a new ID with a different home region and the same partition as the given ID
long MovieWorkload::diff_home_same_part(long id, long MAX_ID)
{
  const int h0 = calculatehome(id);
  const int p0 = calculatepart(id);
  // Collect vector of possible IDs
  vector<long> candidates;
  for(long i = 0L; i <= MAX_ID; i++) {
    int h1 = calculatehome(i);
    int p1 = calculatepart(i);
    if(h1 != h0 && p0 == p1) {
      candidates.push_back(i);
    }
  }
  long maxidx = candidates.size() - 1;
  // Choose one of the IDs using a distribution
  long chosen = NURand(rg_, skew_*maxidx, 0, maxidx);
  return candidates.at(chosen);
}
// Generates a new ID with a different home region and partition to the given ID
long MovieWorkload::diff_home_diff_part(long id, long MAX_ID)
{
  const int h0 = calculatehome(id);
  const int p0 = calculatepart(id);
  
  // Collect vector of possible IDs
  vector<long> candidates;
  for(long i = 0L; i <= MAX_ID; i++) {
    int h1 = calculatehome(i);
    int p1 = calculatepart(i);
    if(h1 != h0 && p0 != p1) {
      candidates.push_back(i);
    }
  }
  long maxidx = candidates.size() - 1;
  // Choose one of the IDs using a distribution
  long chosen = NURand(rg_, skew_*maxidx, 0, maxidx);
  return candidates.at(chosen);
}

}  // namespace slog