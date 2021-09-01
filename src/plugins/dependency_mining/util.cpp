#include "util.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/container_hash/hash.hpp>
#include <magic_enum.hpp>

#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {
TableColumnID::TableColumnID(const std::string& init_table_name, const ColumnID init_column_id)
    : table_name(init_table_name), column_id(init_column_id) {}

std::string TableColumnID::description() const {
  return table_name + "." + Hyrise::get().storage_manager.get_table(table_name)->column_name(column_id);
}

bool TableColumnID::operator==(const TableColumnID& other) const {
  if (this == &other) return true;
  return table_name == other.table_name && column_id == other.column_id;
}

bool TableColumnID::operator!=(const TableColumnID& other) const { return !operator==(other); }

std::string TableColumnID::column_name() const {
  return Hyrise::get().storage_manager.get_table(table_name)->column_name(column_id);
}

size_t TableColumnID::hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, column_id);
  return hash;
}

DependencyCandidate::DependencyCandidate(const TableColumnIDs& init_determinants, const TableColumnIDs& init_dependents,
                                         const DependencyType init_type, const size_t init_priority)
    : determinants(init_determinants), dependents(init_dependents), type(init_type), priority(init_priority) {}

bool DependencyCandidate::operator<(const DependencyCandidate& other) const { return priority < other.priority; }

void DependencyCandidate::output_to_stream(std::ostream& stream) const {
  stream << "Type " << magic_enum::enum_name(type) << ", Priority " << priority << ", Columns ";
  std::vector<std::string> determinants_printable;
  std::for_each(determinants.begin(), determinants.end(), [&determinants_printable](auto& determinant) {
    determinants_printable.push_back(determinant.description());
  });
  stream << boost::algorithm::join(determinants_printable, ", ");
  if (!dependents.empty()) {
    std::vector<std::string> dependents_printable;
    std::for_each(dependents.begin(), dependents.end(), [&dependents_printable](auto& dependent) {
      dependents_printable.push_back(dependent.description());
    });
    stream << " --> " << boost::algorithm::join(dependents_printable, ", ");
  }
}

std::ostream& operator<<(std::ostream& stream, const DependencyCandidate& dependency_candidate) {
  dependency_candidate.output_to_stream(stream);
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const TableColumnID& table_column_id) {
  stream << table_column_id.description();
  return stream;
}

TableColumnID resolve_column_expression(const std::shared_ptr<AbstractExpression>& column_expression) {
  Assert(column_expression->type == ExpressionType::LQPColumn, "Expected LQPColumnExpression");
  const auto lqp_column_expression = static_pointer_cast<LQPColumnExpression>(column_expression);
  const auto orig_node = lqp_column_expression->original_node.lock();
  if (orig_node->type != LQPNodeType::StoredTable) {
    return INVALID_TABLE_COLUMN_ID;
  }
  const auto original_column_id = lqp_column_expression->original_column_id;
  if (original_column_id == INVALID_COLUMN_ID) {
    return INVALID_TABLE_COLUMN_ID;
  }
  const auto stored_table_node = static_pointer_cast<const StoredTableNode>(orig_node);
  const auto table_name = stored_table_node->table_name;
  return TableColumnID{table_name, original_column_id};
}

}  // namespace opossum

namespace std {

size_t hash<opossum::TableColumnID>::operator()(const opossum::TableColumnID& table_column_id) const {
  return table_column_id.hash();
}

}  // namespace std
