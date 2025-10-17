#pragma once

#include <array>

#include "execution/smallbank/table.h"

namespace slog {
namespace smallbank {

class SmallBankTransaction {
 public:
  virtual ~SmallBankTransaction() = default;
  bool Execute() {
    if (!Read()) {
      return false;
    }
    Compute();
    if (!Write()) {
      return false;
    }
    return true;
  }
  virtual bool Read() = 0;
  virtual void Compute() = 0;
  virtual bool Write() = 0;

  const std::string& error() const { return error_; }

 protected:
  void SetError(const std::string& error) {
    if (error_.empty()) error_ = error;
  }

 private:
  std::string error_;
};

class GetCustomerIdByNameTxn : public SmallBankTransaction {
 public:
  GetCustomerIdByNameTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<AccountsSchema> accounts_;

  // Arguments
  FixedTextScalarPtr a_acount_name_;

  // Read results
  Int32ScalarPtr w_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_second_customer_id = MakeInt32Scalar();

  // Computed values
};

class BalanceTxn : public SmallBankTransaction {
 public:
  BalanceTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name, int customer_id);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<AccountsSchema> accounts_;
  Table<CheckingSchema> checking_;
  Table<SavingsSchema> savings_;

  // Arguments
  FixedTextScalarPtr a_acount_name_;
  Int32ScalarPtr a_customer_id_;

  // Read results
  Int32ScalarPtr w_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_checkings_balance_ = MakeInt32Scalar();
  Int32ScalarPtr w_savings_balance_ = MakeInt32Scalar();

  // Computed values
  Int32ScalarPtr w_total_balance_ = MakeInt32Scalar();
};

class DepositCheckingTxn : public SmallBankTransaction {
 public:
  DepositCheckingTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name, int customer_id, int amount);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<AccountsSchema> accounts_;
  Table<CheckingSchema> checking_;

  // Arguments
  FixedTextScalarPtr a_acount_name_;
  Int32ScalarPtr a_customer_id_;
  Int32ScalarPtr a_amount_;

  // Read results
  Int32ScalarPtr w_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_balance_ = MakeInt32Scalar();

  // Computed values
  Int32ScalarPtr w_new_checking_balance_ = MakeInt32Scalar();
};

class TransactionSavingTxn : public SmallBankTransaction {
 public:
  TransactionSavingTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name, int customer_id, int amount);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<AccountsSchema> accounts_;
  Table<SavingsSchema> savings_;

  // Arguments
  FixedTextScalarPtr a_acount_name_;
  Int32ScalarPtr a_customer_id_;
  Int32ScalarPtr a_amount_;

  // Read results
  Int32ScalarPtr w_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_balance_ = MakeInt32Scalar();

  // Computed values
  Int32ScalarPtr w_new_checking_balance_ = MakeInt32Scalar();
};

class AmalgamateTxn : public SmallBankTransaction {
 public:
  AmalgamateTxn(const StorageAdapterPtr& storage_adapter, std::string first_acount_name, std::string second_acount_name,
                int first_customer_id, int second_customer_id);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<AccountsSchema> accounts_;
  Table<SavingsSchema> savings_;
  Table<CheckingSchema> checking_;

  // Arguments
  FixedTextScalarPtr a_first_acount_name_;
  FixedTextScalarPtr a_second_acount_name_;
  Int32ScalarPtr a_first_customer_id_;
  Int32ScalarPtr a_second_customer_id_;

  // Read results
  Int32ScalarPtr w_first_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_second_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_checking_first_customer_id_ = MakeInt32Scalar();
  Int32ScalarPtr w_savings_first_customer_id_ = MakeInt32Scalar();
  Int32ScalarPtr w_checking_second_customer_id_ = MakeInt32Scalar();

  // Computed values
  Int32ScalarPtr w_new_checking_second_customer_id_ = MakeInt32Scalar();
};

class WritecheckTxn : public SmallBankTransaction {
 public:
  WritecheckTxn(const StorageAdapterPtr& storage_adapter, std::string acount_name, int customer_id, int value);
  bool Read() final;
  void Compute() final;
  bool Write() final;

 private:
  Table<AccountsSchema> accounts_;
  Table<SavingsSchema> savings_;
  Table<CheckingSchema> checking_;

  // Arguments
  FixedTextScalarPtr a_acount_name_;
  Int32ScalarPtr a_customer_id_;
  Int32ScalarPtr a_value_;

  // Read results
  Int32ScalarPtr w_customer_id = MakeInt32Scalar();
  Int32ScalarPtr w_checking_customer_id_ = MakeInt32Scalar();
  Int32ScalarPtr w_savings_customer_id_ = MakeInt32Scalar();

  // Computed values
  Int32ScalarPtr w_updated_balance_ = MakeInt32Scalar();
};
}  // namespace smallbank
}  // namespace slog