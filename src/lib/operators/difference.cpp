#include "difference.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/lexical_cast.hpp>

#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {
Difference::Difference(const std::shared_ptr<const AbstractOperator>& left_in,
                       const std::shared_ptr<const AbstractOperator>& right_in)
    : AbstractReadOnlyOperator(OperatorType::Difference, left_in, right_in) {}

const std::string& Difference::name() const {
  static const auto name = std::string{"Difference"};
  return name;
}

std::shared_ptr<AbstractOperator> Difference::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<Difference>(copied_left_input, copied_right_input);
}

void Difference::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Difference::_on_execute() {
  DebugAssert(left_input_table()->column_definitions() == right_input_table()->column_definitions(),
              "Input tables must have same number of columns");

  // 1. We create a set of all right input rows as concatenated strings.

  auto right_input_row_set = std::unordered_set<std::string>(right_input_table()->row_count());

  // Iterating over all chunks and for each chunk over all segments
  const auto chunk_count_right = right_input_table()->chunk_count();
  const auto column_count_right = right_input_table()->column_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_right; ++chunk_id) {
    const auto chunk = right_input_table()->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // creating a temporary row representation with strings to be filled segment-wise
    auto string_row_vector = std::vector<std::stringstream>(chunk->size());
    for (auto column_id = ColumnID{0}; column_id < column_count_right; ++column_id) {
      const auto abstract_segment = chunk->get_segment(column_id);

      // filling the row vector with all values from this segment
      auto row_string_buffer = std::stringstream{};
      const auto segment_size = abstract_segment->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
        // Previously we called a virtual method of the AbstractSegment interface here.
        // It was replaced with a call to the subscript operator as that is equally slow.
        const auto value = (*abstract_segment)[chunk_offset];
        _append_string_representation(string_row_vector[chunk_offset], value);
      }
    }

    // Remove duplicate rows by adding all rows to a unordered set
    std::transform(string_row_vector.cbegin(), string_row_vector.cend(),
                   std::inserter(right_input_row_set, right_input_row_set.end()),
                   [](auto& item) { return item.str(); });
  }

  // 2. Now we check for each chunk of the left input which rows can be added to the output

  std::vector<std::shared_ptr<Chunk>> output_chunks;

  // Iterating over all chunks and for each chunk over all segment
  const auto chunk_count_left = left_input_table()->chunk_count();
  output_chunks.reserve(chunk_count_left);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_left; ++chunk_id) {
    const auto in_chunk = left_input_table()->get_chunk(chunk_id);
    Assert(in_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    Segments output_segments;

    // creating a map to share pos_lists (see table_scan.hpp)
    std::unordered_map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>> out_pos_list_map;

    const auto column_count_left = left_input_table()->column_count();
    for (auto column_id = ColumnID{0}; column_id < column_count_left; ++column_id) {
      const auto abstract_segment = in_chunk->get_segment(column_id);
      // temporary variables needed to create the reference segment
      const auto referenced_segment =
          std::dynamic_pointer_cast<const ReferenceSegment>(in_chunk->get_segment(column_id));
      auto out_column_id = column_id;
      auto out_referenced_table = left_input_table();
      std::shared_ptr<const AbstractPosList> in_pos_list;

      if (referenced_segment) {
        // if the input segment was a reference segment then the output segment must reference the same values/objects
        out_column_id = referenced_segment->referenced_column_id();
        out_referenced_table = referenced_segment->referenced_table();
        in_pos_list = referenced_segment->pos_list();
      }

      // automatically creates the entry if it does not exist
      std::shared_ptr<RowIDPosList>& pos_list_out = out_pos_list_map[in_pos_list];

      if (!pos_list_out) {
        pos_list_out = std::make_shared<RowIDPosList>();
      }

      // creating a ReferenceSegment for the output
      auto out_reference_segment =
          std::make_shared<ReferenceSegment>(out_referenced_table, out_column_id, pos_list_out);
      output_segments.push_back(out_reference_segment);
    }

    // for all offsets check if the row can be added to the output
    const auto in_chunk_size = in_chunk->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < in_chunk_size; ++chunk_offset) {
      // creating string representation off the row at chunk_offset
      auto row_string_buffer = std::stringstream{};
      for (auto column_id = ColumnID{0}; column_id < column_count_left; ++column_id) {
        const auto abstract_segment = in_chunk->get_segment(column_id);

        // Previously a virtual method of the AbstractSegment interface was called here.
        // It was replaced with a call to the subscript operator as that is equally slow.
        const auto value = (*abstract_segment)[chunk_offset];
        _append_string_representation(row_string_buffer, value);
      }
      const auto row_string = row_string_buffer.str();

      // we check if the recently created row_string is contained in the left_input_row_set
      auto search = right_input_row_set.find(row_string);
      if (search == right_input_row_set.end()) {
        for (const auto& pos_list_pair : out_pos_list_map) {
          if (pos_list_pair.first) {
            pos_list_pair.second->emplace_back((*pos_list_pair.first)[chunk_offset]);
          } else {
            pos_list_pair.second->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }
    }

    // Only add chunk if it would contain any tuples
    if (!output_segments.empty() && output_segments[0]->size() > 0) {
      // The difference operator does not change the sorted_by property of chunks. If the chunk was sorted before, it
      // will be sorted after the difference operation as well.
      const auto chunk = std::make_shared<Chunk>(output_segments);
      chunk->finalize();
      const auto& sorted_by = in_chunk->individually_sorted_by();
      if (!sorted_by.empty()) {
        chunk->set_individually_sorted_by(sorted_by);
      }
      output_chunks.emplace_back(chunk);
    }
  }

  return std::make_shared<Table>(left_input_table()->column_definitions(), TableType::References,
                                 std::move(output_chunks));
}

void Difference::_append_string_representation(std::ostream& row_string_buffer, const AllTypeVariant& value) {
  const auto string_value = boost::lexical_cast<std::string>(value);
  const auto length = static_cast<uint32_t>(string_value.length());

  // write value as string
  row_string_buffer << string_value;

  // write byte representation of length
  row_string_buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));
}

}  // namespace opossum
