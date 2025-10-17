#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {
namespace pps {

GetPart::GetPart(const StorageAdapterPtr& storage_adapter, int part_id):
    part_(storage_adapter) {
        part_id_ = MakeInt32Scalar(part_id);
    }

bool GetPart::Read() {
    bool ok = true;
    int part_id = part_id_->value;
    if (auto res = part_.Select({part_id_}, {PartSchema::Column::NAME, PartSchema::Column::AMOUNT}); !res.empty()) {
        part_name_ = UncheckedCast<FixedTextScalar>(res[0]);
        part_amount_ = UncheckedCast<Int64Scalar>(res[1]);
    } else {
        SetError("The part does not exist");
        ok = false;
    }
    return ok;
}

void GetPart::Compute() {}

bool GetPart::Write() {
    return true;
}

}  // namespace pps
}  // namespace slog