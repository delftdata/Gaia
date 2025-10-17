#pragma once

#include <array>

#include "execution/pps/constants.h"
#include "execution/pps/table.h"

namespace slog {
namespace pps {

class PPSTransaction {
 public:
  virtual ~PPSTransaction() = default;
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

class GetProduct : public PPSTransaction {
  public:
    GetProduct (const StorageAdapterPtr& storage_adapter, int product_id);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<ProductSchema> product_;
  
    // Arguments
    Int32ScalarPtr product_id_;
  
    // Read results
    FixedTextScalarPtr product_name_ = MakeFixedTextScalar();
};

class GetPart : public PPSTransaction {
  public:
    GetPart (const StorageAdapterPtr& storage_adapter, int part_id);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<PartSchema> part_;
  
    // Arguments
    Int32ScalarPtr part_id_;
  
    // Read results
    FixedTextScalarPtr part_name_ = MakeFixedTextScalar();
    Int64ScalarPtr part_amount_ = MakeInt64Scalar();
};

class OrderParts : public PPSTransaction {
  public:
    OrderParts (const StorageAdapterPtr& storage_adapter, std::vector<int>& parts_ids);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<PartSchema> part_;
  
    // Arguments
    std::vector<Int32ScalarPtr> parts_ids_;

    // Read results
    std::vector<Int64ScalarPtr> parts_amounts_;

    // Computed values
    std::vector<Int64ScalarPtr> new_parts_amounts_;
};

class OrderProduct : public PPSTransaction {
  public:
    OrderProduct (const StorageAdapterPtr& storage_adapter, int product_id, std::vector<int>& parts_ids);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<PartSchema> part_;
    Table<ProductPartsSchema> product_parts_;
  
    // Arguments
    Int32ScalarPtr product_id_;
    std::vector<Int32ScalarPtr> parts_ids_;

    // Read results
    std::vector<Int64ScalarPtr> parts_amounts_;

    // Computed values
    std::vector<Int64ScalarPtr> new_parts_amounts_;
};

class SupplierRestock : public PPSTransaction {
  public:
    SupplierRestock (const StorageAdapterPtr& storage_adapter, int supplier_id, std::vector<int>& parts_ids);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<PartSchema> part_;
    Table<SupplierPartsSchema> supplier_parts_;
  
    // Arguments
    Int32ScalarPtr suppplier_id_;
    std::vector<Int32ScalarPtr> parts_ids_;

    // Read results
    std::vector<Int64ScalarPtr> parts_amounts_;

    // Computed values
    std::vector<Int64ScalarPtr> new_parts_amounts_;
};

class GetPartsByProduct : public PPSTransaction {
  public:
    GetPartsByProduct (const StorageAdapterPtr& storage_adapter, int product_id);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<ProductPartsSchema> product_parts_;
  
    // Arguments
    Int32ScalarPtr product_id_;

    // Read results
    std::vector<Int32ScalarPtr> parts_ids_;
};

class GetPartsBySupplier : public PPSTransaction {
  public:
    GetPartsBySupplier (const StorageAdapterPtr& storage_adapter, int supplier_id);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<SupplierPartsSchema> supplier_parts_;
  
    // Arguments
    Int32ScalarPtr supplier_id_;

    // Read results
    std::vector<Int32ScalarPtr> parts_ids_;
};

class UpdateProductPart : public PPSTransaction {
  public:
    UpdateProductPart (const StorageAdapterPtr& storage_adapter, int product_id);
    bool Read() final;
    void Compute() final;
    bool Write() final;
  
  private:
    // Accessed tables
    Table<ProductPartsSchema> product_parts_;
  
    // Arguments
    Int32ScalarPtr product_id_;

    // Read results
    Int32ScalarPtr part_id_first_ = MakeInt32Scalar();
    Int32ScalarPtr part_id_last_ = MakeInt32Scalar();
};

}  // namespace pps
}  // namespace slog