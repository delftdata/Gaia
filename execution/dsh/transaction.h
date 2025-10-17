#pragma once

#include <array>

#include "execution/dsh/table.h"
#include "execution/dsh/utils.h"
#include <string>


namespace slog {
namespace dsh {

class DSHTransaction {
 public:
  virtual ~DSHTransaction() = default;
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


class UserLoginTxn : public DSHTransaction {
  public:

    UserLoginTxn(const StorageAdapterPtr& storage_adapter, std::string username, std::string password);
    bool Read() final;
    void Compute() final;
    bool Write() final { return true; };

  private:
    Table<UserSchema> users_;

    // Arguments
    FixedTextScalarPtr username_;
    VarTextScalarPtr password_;

    // Read results
    VarTextScalarPtr read_paswd_ = MakeVarTextScalar();

    // Computed values
    // 1 for success, 0 for failure
    Int8ScalarPtr result_;
};

class SearchTxn : public DSHTransaction {
  public:
    bool Read() final;
    void Compute() final {};
    bool Write() final { return true; };
  private:
    Table<HotelSchema> hotels_;
    Table<ReservationCountSchema> reservation_counts_;

    // Arguments
    FixedTextScalarPtr in_date_;
    FixedTextScalarPtr out_date_;
    Float64ScalarPtr lat_;
    Float64ScalarPtr lon_;    
    std::array<Int32ScalarPtr, kRecommendationReadSize> hotel_ids_;
  
    struct GeoScalar {
      Int32ScalarPtr hotel_id = MakeInt32Scalar();
      Float64ScalarPtr lat = MakeFloat64Scalar();
      Float64ScalarPtr lon = MakeFloat64Scalar();
    };

    
  public:
    template <typename Iterator>
    SearchTxn(const StorageAdapterPtr& storage_adapter, std::string in_date, std::string out_date, 
                        double lat, double lon, Iterator h_ids_begin, Iterator h_ids_end) : reservation_counts_(storage_adapter), hotels_(storage_adapter) {
        in_date_ = MakeFixedTextScalar<10>(in_date);
        out_date_ = MakeFixedTextScalar<10>(out_date);
        lat_ = MakeFloat64Scalar(lat);
        lon_ = MakeFloat64Scalar(lon);
        size_t count = 0;
        for (auto it = h_ids_begin; it != h_ids_end && count < kRecommendationReadSize; it++) {
            hotel_ids_[count++] = MakeInt32Scalar(*it);
        }
    }
};

class RecommendTxn : public DSHTransaction {
  public:
    enum RecommendationType {
      DISTANCE, RATING, PRICE
    };
    bool Read() final;
    void Compute() final;
    bool Write() final { return true; };

  private:
    Table<HotelSchema> hotels_;

    // Arguments
    std::shared_ptr<RecommendationType> type_;
    Float64ScalarPtr lat_;
    Float64ScalarPtr lon_;
    std::array<Int32ScalarPtr, kRecommendationReadSize> hotel_ids_;


    // Read results
    struct RecommendationScalar {
      Int32ScalarPtr h_id;
      Float64ScalarPtr lat;
      Float64ScalarPtr lon;
      Float64ScalarPtr rating;
      Float64ScalarPtr price;
    };

    std::array<RecommendationScalar, kRecommendationReadSize> read_recommendations_;
    
    // Compute results
    Int32ScalarPtr chosen_hotel_id_ = MakeInt32Scalar();
    Float64ScalarPtr chosen_dist_ = MakeFloat64Scalar(0xFFFFFFFF); // This should be high enough
    Float64ScalarPtr chosen_price_ = MakeFloat64Scalar(kMaxHotelPrice);
    Float64ScalarPtr chosen_rating_ = MakeFloat64Scalar();
  public:
    // have to do this here because of template
    template <typename Iterator>
    RecommendTxn(const StorageAdapterPtr& storage_adapter, 
            RecommendationType type, double lat, double lon, 
            Iterator h_ids_begin, Iterator h_ids_end) 
            : hotels_(storage_adapter) {
        type_ = std::make_shared<RecommendationType>(type);
        lat_ = MakeFloat64Scalar(lat);
        lon_ = MakeFloat64Scalar(lon);
        size_t count = 0;
        for (auto it = h_ids_begin; it != h_ids_end && count < kRecommendationReadSize; it++) {
            hotel_ids_[count++] = MakeInt32Scalar(*it);
        }
    }   
};

class ReservationTxn : public DSHTransaction {
  public:
    ReservationTxn(const StorageAdapterPtr& storage_adapter, std::string username, std::string password, 
                   std::string in_date, std::string out_date, int32_t hotel_id, std::string cust_name, int32_t num_rooms);
    bool Read() final;
    void Compute() final;
    bool Write() final;

  private:
    Table<ReservationSchema> reservations_;
    Table<HotelSchema> hotels_;
    Table<ReservationCountSchema> reservation_counts_;
    Table<UserSchema> users_;

    // Arguments
    FixedTextScalarPtr in_date_;
    FixedTextScalarPtr out_date_;
    Int32ScalarPtr hotel_id_;
    VarTextScalarPtr cust_name_;
    Int32ScalarPtr num_rooms_;
    FixedTextScalarPtr username_;
    VarTextScalarPtr password_;
    
    Int32ScalarPtr new_id_;

    // max stay length allows for std::array use
    std::vector<FixedTextScalarPtr> date_range_;


    // Read results
    Int32ScalarPtr hotel_capacity_ = MakeInt32Scalar(); //SELECT ROOM_NUM FROM RESERVATION_NUM WHERE HOTEL_ID = ?
    FixedTextScalarPtr saved_password_ = MakeFixedTextScalar();

    // Calculated
    std::array<Int32ScalarPtr, kMaxStay> new_reservation_count;
    Int8ScalarPtr correct_password_ = MakeInt8Scalar();

};

}  // namespace tpcc
}  // namespace slog