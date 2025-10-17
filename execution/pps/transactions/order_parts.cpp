#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

OrderParts::OrderParts(const StorageAdapterPtr& storage_adapter, std::vector<int>& parts_ids):
    part_(storage_adapter) {
        parts_amounts_.reserve(parts_ids.size());
        parts_ids_.reserve(parts_ids.size());
        new_parts_amounts_.reserve(parts_ids.size());
        for (int i = 0; i < parts_ids.size(); i++) {
            parts_amounts_.push_back(MakeInt64Scalar());
            new_parts_amounts_.push_back(MakeInt64Scalar());
            parts_ids_.push_back(MakeInt32Scalar(parts_ids[i]));
        }
    }

bool OrderParts::Read() {
    bool ok = true;
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

void OrderParts::Compute() {
    for (int i = 0; i < parts_ids_.size(); i++) {
        new_parts_amounts_[i]->value = parts_amounts_[i]->value - 1;
    }
}

bool OrderParts::Write() {
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