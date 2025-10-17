#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"

namespace slog {
namespace movr {

AddVehicleTxn::AddVehicleTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
  const uint64_t vehicle_id, const std::string& city, const std::string& type,
  const uint64_t owner_id, const std::string& owner_city,
  const uint64_t creation_time, const std::string& status,
  const std::string& current_location, const std::string& ext)
    : vehicles_ (storage_adapter),
      users_(storage_adapter) {
  a_vehicle_id_ = MakeInt64Scalar(vehicle_id);
  a_home_city_ = MakeFixedTextScalar<64>(city);
  a_type_ = MakeFixedTextScalar<64>(type);
  a_owner_id_ = MakeInt64Scalar(owner_id);
  a_owner_city_ = MakeFixedTextScalar<64>(owner_city);
  a_creation_time_ = MakeInt64Scalar(creation_time);
  a_status_ = MakeFixedTextScalar<64>(status);
  a_current_location_ = MakeFixedTextScalar<64>(current_location);
  a_ext_ = MakeFixedTextScalar<64>(ext);
}

bool AddVehicleTxn::Read() {
  bool ok = true;
  if (users_.Select({a_owner_id_, a_owner_city_}, {UsersSchema::Column::ID}).empty()) {
    SetError("Vehicle owner does not exist");
    ok = false;
  }
  return ok;
}

void AddVehicleTxn::Compute() {
  
}

bool AddVehicleTxn::Write() {
  bool ok = true;
  
  if (!vehicles_.Insert({a_vehicle_id_, a_home_city_, a_type_, a_owner_id_, a_creation_time_, a_status_, a_current_location_, a_ext_})) {
    SetError("Cannot insert into Vehicles");
    ok = false;
  }

  return ok;
}

}  // namespace movr
}  // namespace slog