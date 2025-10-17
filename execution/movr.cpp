#include <string>

#include "execution/execution.h"
#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"

namespace slog {

using std::stoi;
using std::stoll;

MovrExecution::MovrExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void MovrExecution::Execute(Transaction& txn) {
  auto txn_adapter = std::make_shared<movr::TxnStorageAdapter>(txn);

  if (txn.code().procedures().empty() || txn.code().procedures(0).args().empty()) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Invalid code");
    return;
  }

  std::ostringstream abort_reason;
  const auto& args = txn.code().procedures(0).args();
  const auto& txn_name = args[0];

  if (txn_name == "view_vehicles") {
    if (args.size() <= 2) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("ViewVehicles Txn - Invalid number of arguments");
      return;
    }
    std::string city = args[1];
    std::vector<uint64_t> vehicle_ids;
    for (size_t i = 2; i < args.size(); i++) {
      vehicle_ids.push_back(stoll(args[i]));
    }

    movr::ViewVehiclesTxn view_vehicles(txn_adapter, vehicle_ids, city);
    if (!view_vehicles.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("ViewVehicles Txn - " + view_vehicles.error());
      LOG(ERROR) << "ViewVehicles failed: " << view_vehicles.error();
      return;
    }
  } else if (txn_name == "user_signup") {
    if (args.size() != 6) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("UserSignup Txn - Invalid number of arguments");
      return;
    }
    int user_id = stoll(args[1]);
    std::string city = args[2];
    std::string name = args[3];
    std::string address = args[4];
    std::string credit_card = args[5];

    movr::UserSignupTxn user_signup(txn_adapter, user_id, city, name, address, credit_card);
    if (!user_signup.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("UserSignup Txn - " + user_signup.error());
      LOG(ERROR) << "UserSignup failed: " << user_signup.error();
      return;
    }
  } else if (txn_name == "add_vehicle") {
    if (args.size() != 10) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("AddVehicle Txn - Invalid number of arguments");
      return;
    }
    int vehicle_id = stoll(args[1]);
    std::string home_city = args[2];
    std::string type = args[3];
    int owner_id = stoll(args[4]);
    std::string owner_city = args[5];
    int creation_time = stoi(args[6]);
    std::string status = args[7];
    std::string current_location = args[8];
    std::string ext = args[9];

    movr::AddVehicleTxn add_vehicle(txn_adapter, vehicle_id, home_city,
      type, owner_id, owner_city, creation_time, status, current_location, ext);
    if (!add_vehicle.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("AddVehicle Txn - " + add_vehicle.error());
      LOG(ERROR) << "AddVehicle failed: " << add_vehicle.error();
      return;
    }
  } else if (txn_name == "start_ride") {
    if (args.size() != 10) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("StartRide Txn - Invalid number of arguments");
      return;
    }
    int user_id = stoll(args[1]);
    std::string user_city = args[2];
    std::string code = args[3];
    int vehicle_id = stoll(args[4]);
    std::string vehicle_city = args[5];
    int ride_id = stoll(args[6]);
    std::string home_city = args[7];
    std::string start_address = args[8];
    int start_time = stoi(args[9]);

    movr::StartRideTxn start_ride(txn_adapter, user_id, user_city, code, vehicle_id,
      vehicle_city, ride_id, home_city, start_address, start_time);
    if (!start_ride.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("StartRide Txn - " + start_ride.error());
      LOG(ERROR) << "StartRide failed: " << start_ride.error();
      return;
    }
  } else if (txn_name == "update_location") {
    if (args.size() != 6) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("UpdateLocation Txn - Invalid number of arguments");
      return;
    }
    std::string city = args[1];
    int ride_id = stoll(args[2]);
    int timestamp = stoi(args[3]);
    double lat = stod(args[4]);
    double lon = stod(args[5]);

    movr::UpdateLocationTxn update_location(txn_adapter, city, ride_id, timestamp, lat, lon);
    if (!update_location.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("StockLevel Txn - " + update_location.error());
      LOG(ERROR) << "UpdateLocation failed: " << update_location.error();
      return;
    }
  } else if (txn_name == "end_ride") {
    if (args.size() != 9) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("EndRide Txn - Invalid number of arguments");
      return;
    }
    int ride_id = stoll(args[1]);
    std::string home_city = args[2];
    int vehicle_id = stoll(args[3]);
    std::string vehicle_city = args[4];
    std::string end_address = args[5];
    int end_time = stoi(args[6]);
    double revenue = stod(args[7]);

    movr::EndRideTxn end_ride(txn_adapter, ride_id, home_city, vehicle_id,
      vehicle_city, end_address, end_time, revenue);
    if (!end_ride.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("EndRide Txn - " + end_ride.error());
      LOG(ERROR) << "EndRide failed: " << end_ride.error();
      return;
    }
  } else {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Unknown procedure name");
    return;
  }
  txn.set_status(TransactionStatus::COMMITTED);
  ApplyWrites(txn, sharder_, storage_);
}

}  // namespace slog