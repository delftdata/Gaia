#include "execution/dsh/transaction.h"
#include "execution/dsh/utils.h"


namespace slog {
namespace dsh {

    
ReservationTxn::ReservationTxn(const StorageAdapterPtr& storage_adapter, std::string username, std::string password, 
        std::string in_date, std::string out_date, int32_t hotel_id, std::string cust_name, int32_t num_rooms) 
        : hotels_(storage_adapter), reservations_(storage_adapter), users_(storage_adapter), reservation_counts_(storage_adapter) {
    in_date_ = MakeFixedTextScalar<10>(in_date);
    out_date_ = MakeFixedTextScalar<10>(out_date);
    num_rooms_ = MakeInt32Scalar(num_rooms);
    // needs editing
    cust_name_ = MakeVarTextScalar<55>(cust_name);
    hotel_id_ = MakeInt32Scalar(hotel_id);

    username_ = MakeFixedTextScalar<20>(format_uname(username));
    password_ = MakeVarTextScalar<60>(password);

}

bool ReservationTxn::Read() {
    bool ok = true;
    if (auto user_login_res = users_.Select({username_}, {UserSchema::Column::PASSWORD}); !user_login_res.empty()) {
        saved_password_ = UncheckedCast<FixedTextScalar>(user_login_res[0]);
    } else {
        ok = false;
        SetError("User not found");
    }

    if (auto hotel_capacity_res = hotels_.Select({hotel_id_}, {HotelSchema::Column::CAPACITY}); !hotel_capacity_res.empty()) {
        hotel_capacity_ = UncheckedCast<Int32Scalar>(hotel_capacity_res[0]);
    } else {
        SetError("Hotel capacity does not exist");
        ok = false;
    }
    // only check this if we have results for both of the original calls
    if (ok && num_rooms_ > hotel_capacity_) {
        SetError("Hotel capacity is too low");
        ok = false;
    }

    // each hotel + date has a saved reservation count, we need to check each of these counts
    // to determine if the hotel has enough capacity. If there is nothing saved, assume max capacity
    date_range_ = date_interp(in_date_->to_string(), out_date_->to_string());
    if (date_range_.size() > kMaxStay) {
        SetError("Stay is too long");
        ok = false;
    }
    for (size_t i = 0; i < date_range_.size(); i++) {
        auto count_res = reservation_counts_.Select({hotel_id_, date_range_[i]}, {ReservationCountSchema::Column::COUNT});
        // for the clientside txn to determine r/w set
        if (!ok) {
            continue;
        }
        // max capacity if empty
        if (count_res.empty()) {
            new_reservation_count[i] = (MakeInt32Scalar(hotel_capacity_->value - num_rooms_->value));
            continue;
        }
        // calculate new capacity -- maybe moved to COMPUTE() later
        int32_t new_room_count = UncheckedCast<Int32Scalar>(count_res[0])->value - num_rooms_->value;
        if (new_room_count < 0) {
            SetError("Too many reservations on " + date_range_[i]->to_string());
            ok = false;
        }
        new_reservation_count[i] = (MakeInt32Scalar(new_room_count));
    }

    return ok;
}

void ReservationTxn::Compute() {
    correct_password_->value = saved_password_->to_string() == password_->to_string();
}

bool ReservationTxn::Write() {
    for (size_t i = 0; i < date_range_.size(); i++) {
        // check if the table has the value (it is not auto-populated)
        if (new_reservation_count[i]->value + num_rooms_->value == hotel_capacity_->value) {
            if(!reservation_counts_.Insert({hotel_id_, date_range_[i], new_reservation_count[i]})) {
                SetError("Reservation count update failed");
                return false;
            }
            continue;
        }
        // update the value
        if (!reservation_counts_.Update({hotel_id_, date_range_[i]}, {ReservationCountSchema::Column::COUNT}, {new_reservation_count[i]})) {
            SetError("Reservation count update failed");
            return false;
        }
    }
    // save the reservation
    if (!reservations_.Insert({hotel_id_, new_id_, cust_name_, in_date_, out_date_, num_rooms_})) {
        SetError("Reservation insertion failed");
        return false;
    }
    return true;
}



} // namespace dsh
} // namespace slog
