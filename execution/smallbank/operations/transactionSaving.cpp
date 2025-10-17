#include "execution/smallbank/transaction.h"

namespace slog {
namespace smallbank {

TransactionSavingTxn::TransactionSavingTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name,
                                           int customer_id, int amount)
    : accounts_(storage_adapter), savings_(storage_adapter) {
  a_acount_name_ = MakeFixedTextScalar<24>(acount_name);
  a_customer_id_ = MakeInt32Scalar(customer_id);
  a_amount_ = MakeInt32Scalar(amount);
}

bool TransactionSavingTxn::Read() {
  bool ok = true;
  if (auto res = accounts_.Select({a_acount_name_}, {AccountsSchema::Column::ID}); !res.empty()) {
    w_customer_id = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this name");
    ok = false;
  }
  if (auto res = savings_.Select({a_customer_id_}, {SavingsSchema::Column::BALANCE}); !res.empty()) {
    w_balance_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this customer_id");
    ok = false;
  }
  return ok;
}

void TransactionSavingTxn::Compute() { w_new_checking_balance_->value = w_balance_->value + a_amount_->value; }

bool TransactionSavingTxn::Write() {
  bool ok = true;
  if (!savings_.Update({a_customer_id_}, {SavingsSchema::Column::BALANCE}, {w_new_checking_balance_})) {
    SetError("Cannot update Savings ballance");
    ok = false;
  }
  return ok;
}

}  // namespace smallbank
}  // namespace slog