#include <string>

#include "execution/execution.h"
#include "execution/movie/constants.h"
#include "execution/movie/transaction.h"

namespace slog {

using std::stoi;
using std::stoll;

MovieExecution::MovieExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void MovieExecution::Execute(Transaction& txn) {
  auto txn_adapter = std::make_shared<movie::TxnStorageAdapter>(txn);

  if (txn.code().procedures().empty() || txn.code().procedures(0).args().empty()) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Invalid code");
    return;
  }

  std::ostringstream abort_reason;
  const auto& args = txn.code().procedures(0).args();
  const auto& txn_name = args[0];

  if (txn_name == "new_review") {
    if (args.size() != 8) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("NewReview Txn - Invalid number of arguments");
      return;
    }
    std::string a_username = args[1];
    std::string a_title = args[2];
    int a_rating = stoi(args[3]);
    int64_t a_timestamp = stoll(args[4]);
    int64_t a_req_id = stoll(args[5]);
    std::string a_text = args[6];
    int64_t a_review_id = stoll(args[7]);

    movie::NewReviewTxn review(txn_adapter, a_req_id, a_rating, a_username, a_title, a_timestamp, a_review_id, a_text);
    if (!review.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("Review Txn - " + review.error());
      return;
    }
  }  else {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Unknown procedure name");
    return;
  }
  txn.set_status(TransactionStatus::COMMITTED);
  ApplyWrites(txn, sharder_, storage_);
}

}  // namespace slog