#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"
#include "execution/movr/data_generator.h"

namespace slog {
namespace movr {

EndRideTxn::EndRideTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
  const uint64_t ride_id, const std::string& city, const uint64_t vehicle_id,
  const std::string& vehicle_city, const std::string& end_address,
  const uint64_t end_time, const uint64_t revenue)
    : vehicles_ (storage_adapter),
      rides_ (storage_adapter) {
  a_ride_id_ = MakeInt64Scalar(ride_id);
  a_home_city_ = MakeFixedTextScalar<64>(city);
  a_vehicle_id_ = MakeInt64Scalar(vehicle_id);
  a_vehicle_city_ = MakeFixedTextScalar<64>(vehicle_city);
  a_end_address_ = MakeFixedTextScalar<64>(end_address);
  a_end_time_ = MakeInt64Scalar(end_time);
  a_revenue_ = MakeInt64Scalar(revenue);
}

bool EndRideTxn::Read() {
  bool ok = true;
  if (auto res = vehicles_.Select({a_ride_id_, a_home_city_},
        {VehiclesSchema::Column::STATUS}); !res.empty()) {
    auto status = UncheckedCast<FixedTextScalar>(res[0]);
    if (status->to_string() !=  DataGenerator::EnsureFixedLength<64>("in_use")) {
      SetError("Ride is not active");
      ok = false;
    }
  } else {
    SetError("Ride does not exist");
    ok = false;
  }
  return ok;
}

void EndRideTxn::Compute() {
  
}

bool EndRideTxn::Write() {
  bool ok = true;

  if (!vehicles_.Update({a_vehicle_id_, a_vehicle_city_}, {VehiclesSchema::Column::STATUS},
    {MakeFixedTextScalar<64>(DataGenerator::EnsureFixedLength<64>("available"))})) {
    SetError("Cannot update vehicle status");
    ok = false;
  }
  
  if (!rides_.Update({a_ride_id_, a_home_city_}, {RidesSchema::Column::END_ADDRESS,
    RidesSchema::Column::END_TIME, RidesSchema::Column::REVENUE},
    {a_end_address_, a_end_time_, a_revenue_})) {
    SetError("Cannot update Rides");
    ok = false;
  }

  return ok;
}

}  // namespace movr
}  // namespace slog