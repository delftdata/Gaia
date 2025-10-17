#include <string>

#include "execution/execution.h"
#include "execution/smallbank/transaction.h"

namespace slog {

using std::stoi;
using std::stoll;

SmallBankExecution::SmallBankExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void SmallBankExecution::Execute(Transaction& txn) {
  auto txn_adapter = std::make_shared<smallbank::TxnStorageAdapter>(txn);

  if (txn.code().procedures().empty() || txn.code().procedures(0).args().empty()) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Invalid code");
    LOG(INFO) << "Invalid code";
    return;
  }

  std::ostringstream abort_reason;
  const auto& args = txn.code().procedures(0).args();
  const auto& txn_name = args[0];

  if (txn_name == "getCustomerIdByName") {
    if (args.size() != 2) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("getCustomerIdByName Txn - Invalid number of arguments");
      LOG(INFO) << "getCustomerIdByName Txn - Invalid number of arguments";
      return;
    }
    std::string acount_name = args[1];
    // LOG(INFO) << "Entered getCustomerIdByName1";
    smallbank::GetCustomerIdByNameTxn getCustomerIdByName(txn_adapter, acount_name);
    if (!getCustomerIdByName.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("getCustomerIdByName Txn - " + getCustomerIdByName.error());
      LOG(INFO) << "acount_name ->" << acount_name << " getCustomerIdByName Txn " << getCustomerIdByName.error();
      return;
    }
    // LOG(INFO) << "Entered getCustomerIdByName2";
  } else if (txn_name == "balance") {
    if (args.size() != 3) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("BalanceTxn Txn - Invalid number of arguments");
      LOG(INFO) << "BalanceTxn Txn - Invalid number of arguments";
      return;
    }
    std::string acount_name = args[1];
    int customer_id = stoi(args[2]);
    // LOG(INFO) << "Entered Balance with name: " << acount_name << " and customer_id: " << customer_id;
    smallbank::BalanceTxn balance(txn_adapter, acount_name, customer_id);
    if (!balance.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("BalanceTxn Txn - " + balance.error());
      LOG(INFO) << "customer_id -> " << customer_id << " BalanceTxn Txn " << balance.error();
      return;
    }
  } else if (txn_name == "depositChecking") {
    if (args.size() != 4) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("DepositCheckingTxn Txn - Invalid number of arguments");
      LOG(INFO) << "DepositCheckingTxn Txn - Invalid number of arguments";
      return;
    }
    std::string acount_name = args[1];
    int customer_id = stoi(args[2]);
    int amount = stoi(args[3]);

    smallbank::DepositCheckingTxn depositCheckingTxn(txn_adapter, acount_name, customer_id, amount);
    if (!depositCheckingTxn.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("DepositCheckingTxn Txn - " + depositCheckingTxn.error());
      LOG(INFO) << "customer_id ->" << customer_id << " DepositCheckingTxn Txn " << depositCheckingTxn.error();
      return;
    }
  } else if (txn_name == "transactionSaving") {
    if (args.size() != 4) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("TransactionSavingTxn Txn - Invalid number of arguments");
      LOG(INFO) << "TransactionSavingTxn Txn - Invalid number of arguments";
      return;
    }
    std::string acount_name = args[1];
    int customer_id = stoi(args[2]);
    int amount = stoi(args[3]);

    smallbank::TransactionSavingTxn transactionSavingTxn(txn_adapter, acount_name, customer_id, amount);
    if (!transactionSavingTxn.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("TransactionSavingTxn Txn - " + transactionSavingTxn.error());
      LOG(INFO) << "customer_id ->" << customer_id << " TransactionSavingTxn Txn " << transactionSavingTxn.error();
      return;
    }
  } else if (txn_name == "amalgamate") {
    if (args.size() != 5) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("AmalgamateTxn Txn - Invalid number of arguments");
      LOG(INFO) << "AmalgamateTxn Txn - Invalid number of arguments";
      return;
    }
    std::string first_customer_name = args[1];
    std::string second_customer_name = args[2];
    int first_customer_id = stoi(args[3]);
    int second_customer_id = stoi(args[4]);

    smallbank::AmalgamateTxn amalgamateTxn(txn_adapter, first_customer_name, second_customer_name, first_customer_id,
                                           second_customer_id);
    if (!amalgamateTxn.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("AmalgamateTxn Txn - " + amalgamateTxn.error());
      LOG(INFO) << "first_customer_id ->" << first_customer_id << "second_customer_id" << second_customer_id
                << " AmalgamateTxn Txn " << amalgamateTxn.error();
      return;
    }
  } else if (txn_name == "writecheck") {
    if (args.size() != 4) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("WritecheckTxn Txn - Invalid number of arguments");
      LOG(INFO) << "WritecheckTxn Txn - Invalid number of arguments";
      return;
    }

    std::string acount_name = args[1];
    int customer_id = stoi(args[2]);
    int value = stoi(args[3]);

    smallbank::WritecheckTxn writecheckTxn(txn_adapter, acount_name, customer_id, value);
    if (!writecheckTxn.Execute()) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("WritecheckTxn Txn - " + writecheckTxn.error());
      LOG(INFO) << "customer_id ->" << customer_id << " WritecheckTxn Txn " << writecheckTxn.error();
      return;
    }
  } else {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Unknown procedure name");
    LOG(INFO) << "Unknown procedure name";
    return;
  }
  txn.set_status(TransactionStatus::COMMITTED);
  ApplyWrites(txn, sharder_, storage_);
}

}  // namespace slog