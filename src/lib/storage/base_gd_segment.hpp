#pragma once

#include <memory>
#include <string>

#include "abstract_encoded_segment.hpp"


namespace opossum {

/**
 * @brief Base class of typed GD segments, exposing type-independent interface
 */
class BaseGdSegment : public AbstractEncodedSegment {
 public:
  using AbstractEncodedSegment::AbstractEncodedSegment;

  EncodingType encoding_type() const override = 0;

  std::optional<CompressedVectorType> compressed_vector_type() const override { return std::nullopt;  }

  // Print internal details of the GDD encoding
  virtual void print() const = 0;

  /**
   * @brief ColumnVsValue TableScan operator
   */
  virtual void segment_vs_value_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& query_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {};

  /**
   * @brief ColumnBetween TableScan operator
   */
  virtual void segment_between_table_scan(
    const PredicateCondition& condition, 
    const AllTypeVariant& left_value, 
    const AllTypeVariant& right_value, 
    const ChunkID chunk_id, 
    RowIDPosList& matches,
    const std::shared_ptr<const AbstractPosList>& position_filter) const {};

  /**
   * @brief Returns encoding specific null value ID
   */
  virtual ValueID null_value_id() const = 0;

  virtual size_t bases_num() const = 0;
  
};
}  // namespace opossum
