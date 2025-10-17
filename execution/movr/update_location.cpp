#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"

namespace slog {
namespace movr {

UpdateLocationTxn::UpdateLocationTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
  const std::string& city, const uint64_t ride_id, const uint64_t timestamp,
  const uint64_t lat, const uint64_t lon)
    : location_histories_ (storage_adapter) {
  a_city_ = MakeFixedTextScalar<64>(city);
  a_ride_id_ = MakeInt64Scalar(ride_id);
  a_timestamp_ = MakeInt64Scalar(timestamp);
  a_lat_ = MakeInt64Scalar(lat);
  a_lon_ = MakeInt64Scalar(lon);
}

bool UpdateLocationTxn::Read() {
  return true;
}

void UpdateLocationTxn::Compute() {
  
}

bool UpdateLocationTxn::Write() {
  bool ok = true;
  
  if (!location_histories_.Insert({a_city_, a_ride_id_, a_timestamp_, a_lat_, a_lon_})) {
    SetError("Cannot insert into Location Histories");
    ok = false;
  }

  return ok;
}

}  // namespace movr
}  // namespace slog