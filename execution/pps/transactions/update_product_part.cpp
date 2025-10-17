#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

UpdateProductPart::UpdateProductPart(const StorageAdapterPtr& storage_adapter, int product_id):
    product_parts_(storage_adapter) {
        product_id_ = MakeInt32Scalar(product_id);
    }

bool UpdateProductPart::Read() {
    bool ok = true;
    auto res_first = product_parts_.Select({product_id_, MakeInt32Scalar(1)}, {ProductPartsSchema::Column::PART_ID});
    if (res_first.empty()) {
        SetError("Cannot find the first part id");
        ok = false;
    } else {
        part_id_first_ = UncheckedCast<Int32Scalar>(res_first[0]);
    }
    auto res_last = product_parts_.Select({product_id_, MakeInt32Scalar(pps::kPartsPerProduct)}, {ProductPartsSchema::Column::PART_ID});
    if (res_last.empty()) {
        SetError("Cannot find the last part id");
        ok = false;
    } else {
        part_id_last_ = UncheckedCast<Int32Scalar>(res_last[0]);
    }
    return ok;
}

void UpdateProductPart::Compute() { }

bool UpdateProductPart::Write() {
    bool ok = true;
    if (!product_parts_.Update({product_id_, MakeInt32Scalar(1)}, 
        {ProductPartsSchema::Column::PART_ID}, {part_id_last_})) {
        SetError("Cannot update the first part id");
        ok = false;
    }
    if (!product_parts_.Update({product_id_, MakeInt32Scalar(pps::kPartsPerProduct)}, 
        {ProductPartsSchema::Column::PART_ID}, {part_id_first_})) {
        SetError("Cannot update the last part id");
        ok = false;
    }
    return ok;
}

}  // namespace pps
}  // namespace slog