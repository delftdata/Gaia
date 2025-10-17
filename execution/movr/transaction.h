#pragma once

#include <array>

#include "execution/movr/constants.h"
#include "execution/movr/table.h"

namespace slog {
namespace movr {

class MovrTransaction {
 public:
  virtual ~MovrTransaction() = default;
  bool Execute() {
    if (!Read()) {
      return false;
    }
    Compute();
    if (!Write()) {
      return false;
    }
    return true;
  }
  virtual bool Read() = 0;
  virtual void Compute() = 0;
  virtual bool Write() = 0;

  const std::string& error() const { return error_; }

 protected:
  void SetError(const std::string& error) {
    if (error_.empty()) error_ = error;
  }

 private:
  std::string error_;
};

// Specific transaction classes for MovR

class ViewVehiclesTxn : public MovrTransaction {
    public:
     ViewVehiclesTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
        const std::vector<uint64_t> ids, const std::string city);
     bool Read() final;
     void Compute() final;
     bool Write() final;
   
    private:
     Table<VehiclesSchema> vehicles_;
   
     // Arguments
     std::vector<Int64ScalarPtr> a_ids_;
     FixedTextScalarPtr a_city_;
     
   
     // Read results
     std::vector<Int64ScalarPtr> vehicle_id_results;
     std::vector<FixedTextScalarPtr> city_results;
     std::vector<FixedTextScalarPtr> status_results;
};

class UserSignupTxn : public MovrTransaction {
    public:
     UserSignupTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
        const uint64_t user_id, const std::string& city, const std::string& name,
        const std::string& address, const std::string& credit_card);
     bool Read() final;
     void Compute() final;
     bool Write() final;
   
    private:
     Table<UsersSchema> users_;
   
     // Arguments
     Int64ScalarPtr a_user_id_;
     FixedTextScalarPtr a_city_;
     FixedTextScalarPtr a_name_;
     FixedTextScalarPtr a_address_;
     FixedTextScalarPtr a_credit_card_;
};

class AddVehicleTxn : public MovrTransaction {
    public:
     AddVehicleTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
        const uint64_t vehicle_id, const std::string& city, const std::string& type,
        const uint64_t owner_id, const std::string& owner_city,
        const uint64_t creation_time, const std::string& status,
        const std::string& current_location, const std::string& ext);
     bool Read() final;
     void Compute() final;
     bool Write() final;
   
    private:
     Table<VehiclesSchema> vehicles_;
     Table<UsersSchema> users_;
   
     // Arguments
     Int64ScalarPtr a_vehicle_id_;
     FixedTextScalarPtr a_home_city_;
     FixedTextScalarPtr a_type_;
     Int64ScalarPtr a_owner_id_;
     FixedTextScalarPtr a_owner_city_;
     Int64ScalarPtr a_creation_time_;
     FixedTextScalarPtr a_status_;
     FixedTextScalarPtr a_current_location_;
     FixedTextScalarPtr a_ext_;
};

class StartRideTxn : public MovrTransaction {
    public:
     StartRideTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
        const uint64_t user_id, const std::string& user_city, const std::string& code, const uint64_t vehicle_id,
        const std::string& vehicle_city, const uint64_t ride_id, const std::string& city,
        const std::string& start_address, const uint64_t start_time);
     bool Read() final;
     void Compute() final;
     bool Write() final;
   
    private:
     Table<VehiclesSchema> vehicles_;
     Table<RidesSchema> rides_;
     Table<UsersSchema> users_;
     Table<UserPromoCodesSchema> user_promo_codes_;
   
     // Arguments
     Int64ScalarPtr a_user_id_;
     FixedTextScalarPtr a_user_city_;
     FixedTextScalarPtr a_code_;
     Int64ScalarPtr a_vehicle_id_;
     FixedTextScalarPtr a_vehicle_city_;
     Int64ScalarPtr a_ride_id_;
     FixedTextScalarPtr a_home_city_;
     FixedTextScalarPtr a_start_address_;
     Int64ScalarPtr a_start_time_;
   
     // Read results 
     FixedTextScalarPtr code_result = MakeFixedTextScalar();
     Int64ScalarPtr usage_count_result = MakeInt64Scalar();
};

class UpdateLocationTxn : public MovrTransaction {
    public:
     UpdateLocationTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
        const std::string& city, const uint64_t ride_id, const uint64_t timestamp,
        const uint64_t lat, const uint64_t lon);
     bool Read() final;
     void Compute() final;
     bool Write() final;
   
    private:
     Table<VehicleLocationHistoriesSchema> location_histories_;
   
     // Arguments
     FixedTextScalarPtr a_city_;
     Int64ScalarPtr a_ride_id_;
     Int64ScalarPtr a_timestamp_;
     Int64ScalarPtr a_lat_;
     Int64ScalarPtr a_lon_;
   };

   class EndRideTxn : public MovrTransaction {
    public:
     EndRideTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
        const uint64_t ride_id, const std::string& city, const uint64_t vehicle_id,
        const std::string& vehicle_city, const std::string& end_address,
        const uint64_t end_time, const uint64_t revenue);
     bool Read() final;
     void Compute() final;
     bool Write() final;
   
    private:
     Table<VehiclesSchema> vehicles_;
     Table<RidesSchema> rides_;
   
     // Arguments
     Int64ScalarPtr a_ride_id_;
     FixedTextScalarPtr a_home_city_;
     Int64ScalarPtr a_vehicle_id_;
     FixedTextScalarPtr a_vehicle_city_;
     FixedTextScalarPtr a_end_address_;
     Int64ScalarPtr a_end_time_;
     Int64ScalarPtr a_revenue_;
   };

}  // namespace movr
}  // namespace slog