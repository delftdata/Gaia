#include "execution/dsh/transaction.h"

namespace slog {
namespace dsh {

UserLoginTxn::UserLoginTxn(const StorageAdapterPtr& storage_adapter, std::string username, std::string password)
: users_ (storage_adapter) {
    username_ = MakeFixedTextScalar<20>(format_uname(username));
    password_ = MakeVarTextScalar<60>(password);
}

bool UserLoginTxn::Read() {
    if (auto res = users_.Select({username_}, {UserSchema::Column::PASSWORD}); !res.empty()) {
        read_paswd_ = UncheckedCast<VarTextScalar>(res[0]);
    } else {
        SetError("User does not exist");
        return false;
    }
    return true;
}

void UserLoginTxn::Compute() {
    result_->value = read_paswd_ == password_;
}





}   //namespace dsh
}   //namespace slog