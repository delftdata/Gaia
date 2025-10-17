#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

GetProduct::GetProduct(const StorageAdapterPtr& storage_adapter, int product_id):
    product_(storage_adapter) {
        product_id_ = MakeInt32Scalar(product_id);
    }

bool GetProduct::Read() {
    bool ok = true;
    int product_id = product_id_->value;
    if (auto res = product_.Select({product_id_}, {ProductSchema::Column::NAME}); !res.empty()) {
        product_name_ = UncheckedCast<FixedTextScalar>(res[0]);
    } else {
        std::stringstream ss;
        ss << "The product with id " << product_id << " does not exist";
        SetError(ss.str());
        ok = false;
    }
    return ok;
}

void GetProduct::Compute() {}

bool GetProduct::Write() {
    return true;
}

}  // namespace pps
}  // namespace slog