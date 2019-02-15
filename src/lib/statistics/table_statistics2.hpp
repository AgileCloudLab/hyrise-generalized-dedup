#pragma once

#include <memory>
#include <optional>
#include <iostream>
#include <vector>

#include "all_type_variant.hpp"
#include "cardinality.hpp"

namespace opossum {

class ChunkStatistics2;

using ChunkStatistics2Set = std::vector<std::shared_ptr<ChunkStatistics2>>;

class TableStatistics2 {
 public:
  Cardinality row_count() const;

  size_t column_count() const;
  DataType column_data_type(const ColumnID column_id);

  std::vector<ChunkStatistics2Set> chunk_statistics_sets;
};

std::ostream& operator<<(std::ostream& stream, const TableStatistics2& table_statistics);
std::ostream& operator<<(std::ostream& stream, const ChunkStatistics2Set& chunk_statistics_set);

}  // namespace opossum
