#pragma once

#include <array>
#include <string>

#include "execution/movie/constants.h"
#include "execution/movie/table.h"

namespace slog {
namespace movie {

class MovieTransaction {
 public:
  virtual ~MovieTransaction() = default;
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

class NewReviewTxn : public MovieTransaction {
 public:

  NewReviewTxn(const StorageAdapterPtr& storage_adapter, int64_t req_id, int rating, std::string username, std::string title, int64_t timestamp, int64_t review_id, std::string text);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<UserSchema> user_;
  Table<MovieSchema> movie_;
  Table<ReviewSchema> review_;

  // Arguments
  FixedTextScalarPtr a_username_;
  FixedTextScalarPtr a_title_;
  Int32ScalarPtr a_rating_;
  Int64ScalarPtr a_timestamp_;
  Int64ScalarPtr a_req_id_;
  FixedTextScalarPtr a_text_;
  Int64ScalarPtr a_review_id_;
  

  // Read results
  Int64ScalarPtr r_user_id = MakeInt64Scalar();
  FixedTextScalarPtr r_movie_id = MakeFixedTextScalar();
  Int64ScalarPtr r_reviews = MakeInt64Scalar();

  // Computed values
  Int64ScalarPtr c_reviews = MakeInt64Scalar();
};

}  // namespace movie
}  // namespace slog