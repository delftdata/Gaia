#include "execution/smallbank/transaction.h"

namespace slog {
namespace smallbank {

AmalgamateTxn::AmalgamateTxn(const StorageAdapterPtr& storage_adapter, std::string first_acount_name,
                             std::string second_acount_name, int first_customer_id, int second_customer_id)
    : accounts_(storage_adapter), savings_(storage_adapter), checking_(storage_adapter) {
  a_first_acount_name_ = MakeFixedTextScalar<24>(first_acount_name);
  a_second_acount_name_ = MakeFixedTextScalar<24>(second_acount_name);
  a_first_customer_id_ = MakeInt32Scalar(first_customer_id);
  a_second_customer_id_ = MakeInt32Scalar(second_customer_id);
}

bool AmalgamateTxn::Read() {
  bool ok = true;
  if (auto res = accounts_.Select({a_first_acount_name_}, {AccountsSchema::Column::ID}); !res.empty()) {
    w_first_customer_id = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this name");
    ok = false;
  }

  if (auto res = accounts_.Select({a_second_acount_name_}, {AccountsSchema::Column::ID}); !res.empty()) {
    w_second_customer_id = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this name");
    ok = false;
  }

  if (auto res = checking_.Select({a_first_customer_id_}, {CheckingSchema::Column::BALANCE}); !res.empty()) {
    w_checking_first_customer_id_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account checkings associated with this customer_id");
    ok = false;
  }

  if (auto res = savings_.Select({a_first_customer_id_}, {SavingsSchema::Column::BALANCE}); !res.empty()) {
    w_savings_first_customer_id_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account savings associated with this customer_id");
    ok = false;
  }

  if (auto res = savings_.Select({a_second_customer_id_}, {SavingsSchema::Column::BALANCE}); !res.empty()) {
    w_checking_second_customer_id_ = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account savings associated with this customer_id");
    ok = false;
  }

  return ok;
}

void AmalgamateTxn::Compute() {
  w_new_checking_second_customer_id_->value = w_checking_first_customer_id_->value +
                                              w_savings_first_customer_id_->value +
                                              w_checking_second_customer_id_->value;
  w_savings_first_customer_id_->value = 0;
  w_checking_second_customer_id_->value = 0;
}

bool AmalgamateTxn::Write() {
  bool ok = true;

  if (!checking_.Update({a_first_customer_id_}, {CheckingSchema::Column::BALANCE}, {w_savings_first_customer_id_})) {
    SetError("Cannot update Checking Ballance");
    ok = false;
  }

  if (!savings_.Update({a_first_customer_id_}, {SavingsSchema::Column::BALANCE}, {w_checking_second_customer_id_})) {
    SetError("Cannot update Savings Ballance");
    ok = false;
  }

  if (!checking_.Update({a_second_customer_id_}, {CheckingSchema::Column::BALANCE},
                        {w_new_checking_second_customer_id_})) {
    SetError("Cannot update Checking Ballance");
    ok = false;
  }

  return ok;
}

}  // namespace smallbank
}  // namespace slog