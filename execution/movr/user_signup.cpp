#include "execution/movr/constants.h"
#include "execution/movr/transaction.h"

namespace slog {
namespace movr {

UserSignupTxn::UserSignupTxn(const std::shared_ptr<StorageAdapter>& storage_adapter,
    const uint64_t user_id, const std::string& city, const std::string& name,
    const std::string& address, const std::string& credit_card)
    : users_(storage_adapter) {
  a_user_id_ = MakeInt64Scalar(user_id);
  a_city_ = MakeFixedTextScalar<64>(city);
  a_name_ = MakeFixedTextScalar<64>(name);
  a_address_ = MakeFixedTextScalar<64>(address);
  a_credit_card_ = MakeFixedTextScalar<64>(credit_card);
}

bool UserSignupTxn::Read() {
  return true;
}

void UserSignupTxn::Compute() {
  
}

bool UserSignupTxn::Write() {
  bool ok = true;
  
  if (!users_.Insert({a_user_id_, a_city_, a_name_, a_address_, a_credit_card_})) {
    SetError("Cannot insert into Users");
    ok = false;
  }

  return ok;
}

}  // namespace movr
}  // namespace slog