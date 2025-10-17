#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

SupplierRestock::SupplierRestock(const StorageAdapterPtr& storage_adapter, int supplier_id, std::vector<int>& parts_ids):
    part_(storage_adapter),
    supplier_parts_(storage_adapter) {
        suppplier_id_ = MakeInt32Scalar(supplier_id);
        parts_amounts_.reserve(parts_ids.size());
        parts_ids_.reserve(parts_ids.size());
        new_parts_amounts_.reserve(parts_ids.size());
        for (int i = 0; i < parts_ids.size(); i++) {
            parts_amounts_.push_back(MakeInt64Scalar());
            new_parts_amounts_.push_back(MakeInt64Scalar());
            parts_ids_.push_back(MakeInt32Scalar(parts_ids[i]));
        }
    }

bool SupplierRestock::Read() {
    if (parts_ids_.size() != pps::kPartsPerSupplier) {
        SetError("The number of parts is not correct");
        return false;
    }

    bool ok = true;
    for (int i = 0; i < pps::kPartsPerSupplier; i++) {
        auto res = supplier_parts_.Select({suppplier_id_, MakeInt32Scalar(i + 1)}, {SupplierPartsSchema::Column::PART_ID});
        if (res.empty()) {
            SetError("The supplier-part relationship does not exist");
            ok = false;
        } else {
            auto part_id = UncheckedCast<Int32Scalar>(res[0]);
            if (part_id->value != parts_ids_[i]->value) {
                LOG(INFO) << "The part id is " << part_id->value << " and the supplier id is " << parts_ids_[i]->value;
                SetError("The part doesn't correspond to the supplier");
                ok = false;
            }
        }
    }
    for (int i = 0; i < parts_ids_.size(); i++) {
        auto res = part_.Select({parts_ids_[i]}, {PartSchema::Column::AMOUNT});
        if (res.empty()) {
            SetError("The part does not exist");
            ok = false;
        } else {
            parts_amounts_[i] = UncheckedCast<Int64Scalar>(res[0]);
        }
    }
    return ok;
}

void SupplierRestock::Compute() {
    for (int i = 0; i < parts_ids_.size(); i++) {
        new_parts_amounts_[i]->value = parts_amounts_[i]->value + 1;
    }
}

bool SupplierRestock::Write() {
    bool ok = true;
    for (int i = 0; i < parts_ids_.size(); i++) {
        if (!part_.Update({parts_ids_[i]}, {PartSchema::Column::AMOUNT}, {new_parts_amounts_[i]})) {
            SetError("Cannot update part");
            ok = false;
        }
    }
    return ok;
}

}  // namespace pps
}  // namespace slog