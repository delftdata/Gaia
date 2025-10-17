#pragma once

#include <glog/logging.h>
#include <charconv>
#include <cstdint>

#include "common/configuration.h"
#include "common/types.h"

namespace slog {

class Sharder;

using SharderPtr = std::shared_ptr<Sharder>;

class Sharder {
 public:
  static std::shared_ptr<Sharder> MakeSharder(const ConfigurationPtr& config);

  Sharder(const ConfigurationPtr& config);

  bool is_local_key(const Key& key) const;
  uint32_t num_partitions() const;
  uint32_t local_partition() const;

  virtual uint32_t compute_partition(const Key& key) const = 0;

 protected:
  uint32_t local_partition_;
  uint32_t num_partitions_;
};

class HashSharder : public Sharder {
 public:
  HashSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;

 private:
  size_t partition_key_num_bytes_;
};

class SimpleSharder : public Sharder {
 public:
  SimpleSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

class SimpleSharder2 : public Sharder {
 public:
  SimpleSharder2(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;

 private:
  uint32_t num_regions_;
};

class TPCCSharder : public Sharder {
 public:
  TPCCSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

class DSHSharder : public Sharder {
 public:
  DSHSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

class MovieSharder : public Sharder {
 public:
  MovieSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;

 private:
  uint32_t num_regions_;
};

class MovrSharder : public Sharder {
 public:
  MovrSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

class PPSSharder : public Sharder {
 public:
  PPSSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

class SmallBankSharder : public Sharder {
 public:
  SmallBankSharder(const ConfigurationPtr& config);
  uint32_t compute_partition(const Key& key) const final;
};

}  // namespace slog