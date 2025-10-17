#include <string>

#include "execution/execution.h"
#include "execution/pps/constants.h"
#include "execution/pps/transaction.h"

namespace slog {

using std::stoi;
using std::stoll;

PPSExecution::PPSExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void PPSExecution::Execute(Transaction& txn) {
  auto txn_adapter = std::make_shared<pps::TxnStorageAdapter>(txn);

  if (txn.code().procedures().empty() || txn.code().procedures(0).args().empty()) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Invalid code");
    return;
  }

  std::ostringstream abort_reason;
  const auto& args = txn.code().procedures(0).args();
  const auto& txn_name = args[0];

  if (txn_name == "get_product") {
    int product_id = stoi(args[1]);
    pps::GetProduct get_product(txn_adapter, product_id);
    if (!get_product.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("GetProduct Txn - " + get_product.error());
      return;
    }
  } else if (txn_name == "get_part") {
    int part_id = stoi(args[1]);
    pps::GetPart get_part(txn_adapter, part_id);
    if (!get_part.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("GetPart Txn - " + get_part.error());
      return;
    } 
  } else if (txn_name == "order_parts") {
    std::vector<int> parts_ids;
    for (size_t i = 1; i < args.size(); ++i) {
      parts_ids.push_back(stoi(args[i]));
    }
    pps::OrderParts order_parts(txn_adapter, parts_ids);
    if (!order_parts.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("OrderParts Txn - " + order_parts.error());
      return;
    }
  } else if (txn_name == "order_product") {
    int product_id = stoi(args[1]);
    std::vector<int> parts_ids;
    for (size_t i = 2; i < args.size(); ++i) {
      parts_ids.push_back(stoi(args[i]));
    }
    pps::OrderProduct order_product(txn_adapter, product_id, parts_ids);
    if (!order_product.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("OrderProduct Txn - " + order_product.error());
      return;
    }
  } else if (txn_name == "supplier_restock") {
    int supplier_id = stoi(args[1]);
    std::vector<int> parts_ids;
    for (size_t i = 2; i < args.size(); ++i) {
      parts_ids.push_back(stoi(args[i]));
    }
    pps::SupplierRestock supplier_restock(txn_adapter, supplier_id, parts_ids);
    if (!supplier_restock.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("SupplierRestock Txn - " + supplier_restock.error());
      return;
    }
  } else if (txn_name == "get_parts_by_product") {
    int product_id = stoi(args[1]);
    pps::GetPartsByProduct get_parts_by_product(txn_adapter, product_id);
    if (!get_parts_by_product.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("GetPartsByProduct Txn - " + get_parts_by_product.error());
      return;
    }
  } else if (txn_name == "get_parts_by_supplier") {
    int supplier_id = stoi(args[1]);
    pps::GetPartsBySupplier get_parts_by_supplier(txn_adapter, supplier_id);
    if (!get_parts_by_supplier.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("GetPartsBySupplier Txn - " + get_parts_by_supplier.error());
      return;
    }
  } else if (txn_name == "update_product_part") {
    int product_id = stoi(args[1]);
    pps::UpdateProductPart update_product_part(txn_adapter, product_id);
    if (!update_product_part.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("UpdateProductPart Txn - " + update_product_part.error());
      return;
    }
  } else {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Unknown procedure name");
    return;
  }
  txn.set_status(TransactionStatus::COMMITTED);
  ApplyWrites(txn, sharder_, storage_);
}

}  // namespace slog