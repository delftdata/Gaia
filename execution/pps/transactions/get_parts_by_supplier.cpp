#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

GetPartsBySupplier::GetPartsBySupplier(const StorageAdapterPtr& storage_adapter, int supplier_id):
    supplier_parts_(storage_adapter) {
        supplier_id_ = MakeInt32Scalar(supplier_id);
        parts_ids_.reserve(pps::kPartsPerSupplier);
        for (int i = 0; i < pps::kPartsPerSupplier; i++) {
            parts_ids_.push_back(MakeInt32Scalar());
        }
    }

bool GetPartsBySupplier::Read() {
    bool ok = true;
    for (int i = 0; i < parts_ids_.size(); i++) {
        Int32ScalarPtr part_index = MakeInt32Scalar(i + 1);
        auto res = supplier_parts_.Select({supplier_id_, part_index}, {SupplierPartsSchema::Column::PART_ID});
        if (res.empty()) {
            SetError("The part does not exist");
            ok = false;
        } else {
            parts_ids_[i] = UncheckedCast<Int32Scalar>(res[0]);
        }
    }
    return ok;
}

void GetPartsBySupplier::Compute() { }

bool GetPartsBySupplier::Write() {
    return true;
}

}  // namespace pps
}  // namespace slog