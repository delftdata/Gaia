#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

GetPartsByProduct::GetPartsByProduct(const StorageAdapterPtr& storage_adapter, int product_id):
    product_parts_(storage_adapter) {
        product_id_ = MakeInt32Scalar(product_id);
        parts_ids_.reserve(pps::kPartsPerProduct);
        for (int i = 0; i < pps::kPartsPerProduct; i++) {
            parts_ids_.push_back(MakeInt32Scalar());
        }
    }

bool GetPartsByProduct::Read() {
    bool ok = true;
    for (int i = 0; i < parts_ids_.size(); i++) {
        Int32ScalarPtr part_index = MakeInt32Scalar(i + 1);
        auto res = product_parts_.Select({product_id_, part_index}, {ProductPartsSchema::Column::PART_ID});
        if (res.empty()) {
            SetError("The part does not exist");
            ok = false;
        } else {
            parts_ids_[i] = UncheckedCast<Int32Scalar>(res[0]);
        }
    }
    return ok;
}

void GetPartsByProduct::Compute() { }

bool GetPartsByProduct::Write() {
    return true;
}

}  // namespace pps
}  // namespace slog