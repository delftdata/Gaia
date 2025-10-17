#include "execution/dsh/transaction.h"
#include "execution/dsh/utils.h"

#include <charconv>

namespace slog {
namespace dsh {


bool RecommendTxn::Read() {
    bool ok = true;
    // basically instead of reading the whole database we will somehow pick
    // k hotels to read (at "random") -- a bit finer tuning
    for (size_t i = 0ul; i < kRecommendationReadSize; i++) {
        // maybe better to just select all and discard results?
        if (auto res = hotels_.Select({hotel_ids_[i]}); !res.empty()) {
            read_recommendations_[i] = {UncheckedCast<Int32Scalar>(res[0]),
                UncheckedCast<Float64Scalar>(res[1]),
                UncheckedCast<Float64Scalar>(res[2]),
                UncheckedCast<Float64Scalar>(res[3]),
                UncheckedCast<Float64Scalar>(res[4])};
        } else {
            SetError("Cannot find recommendation hotel");
            ok = false;
        }
    }
    return ok;
}

void RecommendTxn::Compute() {
    // I would love to avoid this much nesting but I am not gonna evaluate the switch every time
    // This is a performance test after all
    // Though maybe the compiler knows to optimize it away
    switch (*type_) {
        case RecommendationType::DISTANCE:
            for (RecommendationScalar s : read_recommendations_) {
                double d = dist(s.lat->value, s.lon->value, lat_->value, lon_->value);
                if (d < chosen_dist_->value) {
                    chosen_dist_->value = d;
                    chosen_hotel_id_ = s.h_id;
                }
            }
            break;
        case RecommendationType::PRICE:
            for (RecommendationScalar s : read_recommendations_) {
                if (s.price->value < chosen_price_->value) {
                    chosen_price_ = s.price;
                    chosen_hotel_id_ = s.h_id;
                }
            }
            break;
        case RecommendationType::RATING:
            for (RecommendationScalar s : read_recommendations_) {
                if (s.rating->value > chosen_rating_->value) {
                    chosen_rating_ = s.rating;
                    chosen_hotel_id_ = s.h_id;
                }
            }
            break;
    }
}

}
}