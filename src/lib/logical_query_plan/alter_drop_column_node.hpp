#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

class AlterDropColumnNode : public EnableMakeForLQPNode<AlterDropColumnNode>, public AbstractNonQueryNode {
 public:
  AlterDropColumnNode(const std::string& init_table_name, const std::string& init_column_name);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const std::string column_name;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
