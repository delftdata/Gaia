#include "execution/smallbank/transaction.h"

namespace slog {
namespace smallbank {

BalanceTxn::BalanceTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name, int customer_id)
    : accounts_(storage_adapter), checking_(storage_adapter), savings_(storage_adapter) {
  a_acount_name_ = MakeFixedTextScalar<24>(acount_name);
  a_customer_id_ = MakeInt32Scalar(customer_id);
}

bool BalanceTxn::Read() {
  bool ok = true;
  if (auto res = accounts_.Select({a_acount_name_}, {AccountsSchema::Column::ID}); !res.empty()) {
    w_customer_id = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this name");
    ok = false;
  }

  if (auto res = checking_.Select({a_customer_id_}, {CheckingSchema::Column::BALANCE}); !res.empty()) {
    w_checkings_balance_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account checkings associated with this customer_id");
    ok = false;
  }

  if (auto res = savings_.Select({a_customer_id_}, {SavingsSchema::Column::BALANCE}); !res.empty()) {
    w_savings_balance_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account savings associated with this customer_id");
    ok = false;
  }
  return ok;
}

void BalanceTxn::Compute() {
  w_total_balance_->value = w_checkings_balance_->value + w_savings_balance_->value;
  // LOG(INFO) << "TOTAL BALANCE for" << &a_acount_name_ << " is " << w_total_balance_->value;
}

bool BalanceTxn::Write() {
  bool ok = true;
  return ok;
}

}  // namespace smallbank
}  // namespace slog