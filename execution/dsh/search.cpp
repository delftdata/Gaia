#include "execution/dsh/transaction.h"
#include "execution/dsh/utils.h"

// #include <charconv>
#include <cmath>
#include <algorithm>

namespace slog {
namespace dsh {


bool SearchTxn::Read() {
    bool ok = true;
    // find distance from each hotel
    std::vector<std::pair<double, Int32ScalarPtr>> distance_ranking;
    for (Int32ScalarPtr h_id : hotel_ids_) {        
        if (auto res = hotels_.Select({h_id}, {HotelSchema::Column::LAT, HotelSchema::Column::LON}); !res.empty()) {
            distance_ranking.push_back({
                dist(lat_->value, lon_->value, 
                     UncheckedCast<Float64Scalar>(res[0])->value, UncheckedCast<Float64Scalar>(res[1])->value), 
                h_id});
        } else {
            SetError("Hotel not found");
            ok = false;
        }
    }

    // find knn -- I would like to put this in calculate but since it's all reading it shouldn't impact performance at all 
    // and there is more reading after reliant on this calculation
    std::sort(distance_ranking.begin(), distance_ranking.end(), [](const std::pair<double, Int32ScalarPtr>& p1, const std::pair<double, Int32ScalarPtr>& p2) {
        return p1.first < p2.first;
    });

    auto date_range = date_interp(in_date_->to_string(), out_date_->to_string());
    // check availability of all dates for the closest hotel
    // also this should be empty in the client txn which tests for the read set so this for loop doesn't need to be properly made
    for (std::pair<double, Int32ScalarPtr> hotel : distance_ranking) {
        bool all_dates_available = true;
        // iterate over the date range
        for (auto date : date_range) {
            auto res = reservation_counts_.Select({hotel.second, date}, {ReservationCountSchema::Column::COUNT});
            // if the hotel has no reservations, there won't be anything in the table.
            if (res.empty()) {
                auto hotel_cap = hotels_.Select({hotel.second}, {HotelSchema::Column::CAPACITY});
                if (hotel_cap.empty()) {
                    SetError("Hotel capacity not found");
                    ok = false;
                    continue;
                }
                // maybe need to change semantics here
                res = hotel_cap;
            }
            // if the hotel is fully booked, set the flag
            if (UncheckedCast<Int32Scalar>(res[0])->value <= 0) {
                all_dates_available = false;
                break;
            }
        }
        // if we have made it without setting false, we found the hotel and can finish the transaction
        if (all_dates_available) {
            break;
        }
    }
    return ok;
}

}
}
