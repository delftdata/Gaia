#include "execution/smallbank/transaction.h"

namespace slog {
namespace smallbank {

GetCustomerIdByNameTxn::GetCustomerIdByNameTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name)
    : accounts_(storage_adapter) {
  a_acount_name_ = MakeFixedTextScalar<24>(acount_name);
}

bool GetCustomerIdByNameTxn::Read() {
  bool ok = true;
  if (auto res = accounts_.Select({a_acount_name_}, {AccountsSchema::Column::ID}); !res.empty()) {
    w_customer_id = UncheckedCast<Int32Scalar>(res[0]);
  } else {
    SetError("There is no account associated with this name");
    ok = false;
  }

  return ok;
}

void GetCustomerIdByNameTxn::Compute() {}

bool GetCustomerIdByNameTxn::Write() {
  bool ok = true;
  return ok;
}

}  // namespace smallbank
}  // namespace slog