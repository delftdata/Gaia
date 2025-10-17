#include "execution/smallbank/transaction.h"

namespace slog {
namespace smallbank {

WritecheckTxn::WritecheckTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name, int customer_id,
                             int value)
    : accounts_(storage_adapter), savings_(storage_adapter), checking_(storage_adapter) {
  a_acount_name_ = MakeFixedTextScalar<24>(acount_name);
  a_customer_id_ = MakeInt32Scalar(customer_id);
  a_value_ = MakeInt32Scalar(value);
}

bool WritecheckTxn::Read() {
  bool ok = true;
  if (auto res = accounts_.Select({a_acount_name_}, {AccountsSchema::Column::ID}); !res.empty()) {
    w_customer_id = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this name");
    ok = false;
  }
  if (auto res = checking_.Select({a_customer_id_}, {CheckingSchema::Column::BALANCE}); !res.empty()) {
    w_checking_customer_id_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account checkings associated with this customer_id");
    ok = false;
  }

  if (auto res = savings_.Select({a_customer_id_}, {SavingsSchema::Column::BALANCE}); !res.empty()) {
    w_savings_customer_id_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account savings associated with this customer_id");
    ok = false;
  }

  return ok;
}

void WritecheckTxn::Compute() {}

bool WritecheckTxn::Write() {
  bool ok = true;
  if ((w_checking_customer_id_->value + w_savings_customer_id_->value) < a_value_->value) {
    w_updated_balance_->value = w_checking_customer_id_->value - (a_value_->value + 1);
    if (!checking_.Update({a_customer_id_}, {CheckingSchema::Column::BALANCE}, {w_updated_balance_})) {
      SetError("Cannot update Checking Ballance");
      ok = false;
    }
  } else {
    w_updated_balance_->value = w_checking_customer_id_->value - (a_value_->value);
    if (!checking_.Update({a_customer_id_}, {CheckingSchema::Column::BALANCE}, {w_updated_balance_})) {
      SetError("Cannot update Checking Ballance");
      ok = false;
    }
  }
  return ok;
}

}  // namespace smallbank
}  // namespace slog