#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

OrderProduct::OrderProduct(const StorageAdapterPtr& storage_adapter, int product_id, std::vector<int>& parts_ids):
    part_(storage_adapter),
    product_parts_(storage_adapter) {
        product_id_ = MakeInt32Scalar(product_id);
        parts_amounts_.reserve(parts_ids.size());
        parts_ids_.reserve(parts_ids.size());
        new_parts_amounts_.reserve(parts_ids.size());
        for (int i = 0; i < parts_ids.size(); i++) {
            parts_amounts_.push_back(MakeInt64Scalar());
            new_parts_amounts_.push_back(MakeInt64Scalar());
            parts_ids_.push_back(MakeInt32Scalar(parts_ids[i]));
        }
    }

bool OrderProduct::Read() {
    if (parts_ids_.size() != pps::kPartsPerProduct) {
        SetError("The number of parts is not correct");
        return false;
    }

    bool ok = true;
    for (int i = 0; i < pps::kPartsPerProduct; i++) {
        auto res = product_parts_.Select({product_id_, MakeInt32Scalar(i + 1)}, {ProductPartsSchema::Column::PART_ID});
        if (res.empty()) {
            SetError("The product-part relationship does not exist");
            ok = false;
        } else {
            auto part_id = UncheckedCast<Int32Scalar>(res[0]);
            if (part_id->value != parts_ids_[i]->value) {
                std::ostringstream oss;
                oss << "The part doesn't correspond to the product (" << part_id->value << " != " << parts_ids_[i]->value << ")"; 
                SetError(oss.str());
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

void OrderProduct::Compute() {
    for (int i = 0; i < parts_ids_.size(); i++) {
        new_parts_amounts_[i]->value = parts_amounts_[i]->value - 1;
    }
}

bool OrderProduct::Write() {
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