#include "execution/movie/transaction.h"

namespace slog {
namespace movie {

NewReviewTxn::NewReviewTxn(const StorageAdapterPtr& storage_adapter, int64_t req_id, int rating, std::string username, std::string title, int64_t timestamp, int64_t review_id, std::string text)
    : user_(storage_adapter), movie_(storage_adapter), review_(storage_adapter) {
  a_username_ = MakeFixedTextScalar<21>(username);
  a_title_ = MakeFixedTextScalar<100>(title);
  a_rating_ = MakeInt32Scalar(rating);
  a_timestamp_ = MakeInt64Scalar(timestamp);
  a_req_id_ = MakeInt64Scalar(req_id);
  a_review_id_ = MakeInt64Scalar(review_id);
  a_text_ = MakeFixedTextScalar<256>(text);
}

bool NewReviewTxn::Read() {
  bool ok = true;
  if (auto res = user_.Select(
          {a_username_}, {UserSchema::Column::USER_ID, UserSchema::Column::REVIEWS});
      !res.empty()) {
    r_user_id = UncheckedCast<Int64Scalar>(res[0]);
    r_reviews = UncheckedCast<Int64Scalar>(res[1]);
  } else {
    SetError("User does not exist");
    ok = false;
  } 

  if (auto res = movie_.Select({a_title_}, {MovieSchema::Column::MOVIE_ID});
      !res.empty()) {
    r_movie_id = UncheckedCast<FixedTextScalar>(res[0]);
  } else {
    SetError("Movie does not exist");
    ok = false;
  }

  return ok;
}

void NewReviewTxn::Compute() {
  c_reviews->value = r_reviews->value + 1;
}

bool NewReviewTxn::Write() {
  bool ok = true;

  if(!review_.Insert({a_review_id_, a_req_id_, a_text_, a_rating_, a_timestamp_, r_movie_id, r_user_id})){
    SetError("Could not insert review");
    ok = false;
  };
  if(!user_.Update({a_username_}, {UserSchema::Column::REVIEWS}, {c_reviews})) {
    SetError("Could not update user reviews");
    ok = false;
  }

  return ok;
}

}  // namespace movie
}  // namespace slog