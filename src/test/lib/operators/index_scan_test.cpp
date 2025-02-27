#include <map>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename DerivedIndex>
class OperatorsIndexScanTest : public BaseTest {
 protected:
  void SetUp() override {
    _index_type = get_index_type_of<DerivedIndex>();

    auto int_int_7 = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
    auto int_int_5 = load_table("resources/test_data/tbl/int_int_shuffled_2.tbl", ChunkOffset{5});

    ChunkEncoder::encode_all_chunks(int_int_7);
    ChunkEncoder::encode_all_chunks(int_int_5);

    _chunk_ids = std::vector<ChunkID>(int_int_7->chunk_count());
    std::iota(_chunk_ids.begin(), _chunk_ids.end(), ChunkID{0});

    _chunk_ids_partly_compressed = std::vector<ChunkID>(int_int_5->chunk_count());
    std::iota(_chunk_ids_partly_compressed.begin(), _chunk_ids_partly_compressed.end(), ChunkID{0});

    _column_ids = std::vector<ColumnID>{ColumnID{0u}};

    for (const auto& chunk_id : _chunk_ids) {
      const auto chunk = int_int_7->get_chunk(chunk_id);
      chunk->template create_index<DerivedIndex>(_column_ids);
    }

    for (const auto& chunk_id : _chunk_ids_partly_compressed) {
      const auto chunk = int_int_5->get_chunk(chunk_id);
      chunk->template create_index<DerivedIndex>(_column_ids);
    }

    _int_int = std::make_shared<TableWrapper>(std::move(int_int_7));
    _int_int->never_clear_output();
    _int_int->execute();

    _int_int_small_chunk = std::make_shared<TableWrapper>(std::move(int_int_5));
    _int_int_small_chunk->never_clear_output();
    _int_int_small_chunk->execute();

    const auto partially_indexed_table = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
    ChunkEncoder::encode_all_chunks(partially_indexed_table);
    const auto second_chunk = partially_indexed_table->get_chunk(ChunkID{1});
    second_chunk->template create_index<DerivedIndex>(std::vector<ColumnID>{ColumnID{0}});
    Hyrise::get().storage_manager.add_table("index_test_table", partially_indexed_table);
  }

  void ASSERT_COLUMN_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                        std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        const auto& segment = *chunk->get_segment(column_id);

        const auto found_value = segment[chunk_offset];
        const auto comparator = [found_value](const AllTypeVariant expected_value) {
          // returns equivalency, not equality to simulate std::multiset.
          // multiset cannot be used because it triggers a compiler / lib bug when built in CI
          return !(found_value < expected_value) && !(expected_value < found_value);
        };

        auto search = std::find_if(expected.begin(), expected.end(), comparator);

        ASSERT_TRUE(search != expected.end());
        expected.erase(search);
      }
    }

    ASSERT_EQ(expected.size(), 0u);
  }

  std::shared_ptr<TableWrapper> _int_int;
  std::shared_ptr<TableWrapper> _int_int_small_chunk;
  std::vector<ChunkID> _chunk_ids;
  std::vector<ChunkID> _chunk_ids_partly_compressed;
  std::vector<ColumnID> _column_ids;
  SegmentIndexType _index_type;
};

typedef ::testing::Types<GroupKeyIndex, AdaptiveRadixTreeIndex, CompositeGroupKeyIndex,
                         BTreeIndex /* add further indexes */>
    SingleSegmentIndexTypes;

TYPED_TEST_SUITE(OperatorsIndexScanTest, SingleSegmentIndexTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanOnDataTable) {
  // we do not need to check for a non existing value, because that happens automatically when we scan the second chunk

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{4}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{9}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {104, 104};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 108, 110, 112, 100, 102, 106, 108, 110, 112};
  tests[PredicateCondition::LessThan] = {100, 102, 100, 102};
  tests[PredicateCondition::LessThanEquals] = {100, 102, 104, 100, 102, 104};
  tests[PredicateCondition::GreaterThan] = {106, 108, 110, 112, 106, 108, 110, 112};
  tests[PredicateCondition::GreaterThanEquals] = {104, 106, 108, 110, 112, 104, 106, 108, 110, 112};
  tests[PredicateCondition::BetweenInclusive] = {104, 106, 108, 104, 106, 108};
  tests[PredicateCondition::BetweenLowerExclusive] = {106, 108, 106, 108};
  tests[PredicateCondition::BetweenUpperExclusive] = {104, 106, 108, 104, 106, 108};
  tests[PredicateCondition::BetweenExclusive] = {106, 108, 106, 108};

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int, this->_index_type, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(this->_int_int_small_chunk, this->_index_type,
                                                        this->_column_ids, test.first, right_values, right_values2);

    scan_small_chunk->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
    this->ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanValueGreaterThanMaxDictionaryValue) {
  const auto all_rows =
      std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{30}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{34}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = all_rows;
  tests[PredicateCondition::LessThanEquals] = all_rows;
  tests[PredicateCondition::GreaterThan] = no_rows;
  tests[PredicateCondition::GreaterThanEquals] = no_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int, this->_index_type, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(this->_int_int_small_chunk, this->_index_type,
                                                        this->_column_ids, test.first, right_values, right_values2);

    scan_small_chunk->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
    this->ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanValueLessThanMinDictionaryValue) {
  const auto all_rows =
      std::vector<AllTypeVariant>{100, 102, 104, 106, 108, 110, 112, 100, 102, 104, 106, 108, 110, 112};
  const auto no_rows = std::vector<AllTypeVariant>{};

  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{-10}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{34}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = no_rows;
  tests[PredicateCondition::NotEquals] = all_rows;
  tests[PredicateCondition::LessThan] = no_rows;
  tests[PredicateCondition::LessThanEquals] = no_rows;
  tests[PredicateCondition::GreaterThan] = all_rows;
  tests[PredicateCondition::GreaterThanEquals] = all_rows;

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int, this->_index_type, this->_column_ids, test.first,
                                            right_values, right_values2);

    scan->execute();

    auto scan_small_chunk = std::make_shared<IndexScan>(this->_int_int_small_chunk, this->_index_type,
                                                        this->_column_ids, test.first, right_values, right_values2);

    scan_small_chunk->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
    this->ASSERT_COLUMN_EQ(scan_small_chunk->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, SingleColumnScanOnlySomeChunks) {
  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{4}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{9}};

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {};
  tests[PredicateCondition::NotEquals] = {100, 102, 106, 108, 110, 112, 106, 110, 112};
  tests[PredicateCondition::LessThan] = {100, 102};
  tests[PredicateCondition::LessThanEquals] = {100, 102};
  tests[PredicateCondition::GreaterThan] = {106, 108, 110, 112, 106, 110, 112};
  tests[PredicateCondition::GreaterThanEquals] = {106, 108, 110, 112, 106, 110, 112};
  tests[PredicateCondition::BetweenInclusive] = {106, 106, 108};
  tests[PredicateCondition::BetweenLowerExclusive] = {106, 108, 106};
  tests[PredicateCondition::BetweenUpperExclusive] = {106, 108, 106};
  tests[PredicateCondition::BetweenExclusive] = {106, 108, 106};

  for (const auto& test : tests) {
    auto scan = std::make_shared<IndexScan>(this->_int_int_small_chunk, this->_index_type, this->_column_ids,
                                            test.first, right_values, right_values2);

    scan->included_chunk_ids = {ChunkID{0}, ChunkID{2}};

    scan->execute();

    this->ASSERT_COLUMN_EQ(scan->get_output(), ColumnID{1u}, test.second);
  }
}

TYPED_TEST(OperatorsIndexScanTest, PosListGuarenteesSingleChunkReference) {
  const auto right_values = std::vector<AllTypeVariant>{AllTypeVariant{4}};
  const auto right_values2 = std::vector<AllTypeVariant>{AllTypeVariant{9}};

  auto scan = std::make_shared<IndexScan>(this->_int_int, this->_index_type, this->_column_ids,
                                          PredicateCondition::Equals, right_values, right_values2);
  scan->execute();

  const auto chunk_count = scan->get_output()->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = scan->get_output()->get_chunk(chunk_id);

    const auto segment = chunk->get_segment(this->_column_ids[0]);
    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    const auto& pos_list = reference_segment->pos_list();
    EXPECT_TRUE(pos_list->references_single_chunk());
  }
}

TYPED_TEST(OperatorsIndexScanTest, OperatorName) {
  const auto right_values = std::vector<AllTypeVariant>(this->_column_ids.size(), AllTypeVariant{0});

  auto scan = std::make_shared<opossum::IndexScan>(this->_int_int, this->_index_type, this->_column_ids,
                                                   PredicateCondition::GreaterThanEquals, right_values);

  EXPECT_EQ(scan->name(), "IndexScan");
}

TYPED_TEST(OperatorsIndexScanTest, InvalidIndexTypeThrows) {
  const auto right_values = std::vector<AllTypeVariant>(this->_column_ids.size(), AllTypeVariant{0});

  auto scan = std::make_shared<opossum::IndexScan>(this->_int_int, SegmentIndexType::Invalid, this->_column_ids,
                                                   PredicateCondition::GreaterThan, right_values);
  EXPECT_THROW(scan->execute(), std::logic_error);
}

TYPED_TEST(OperatorsIndexScanTest, AddedChunk) {
  // We want to make sure that all chunks are covered even if they have been added after SQL translation

  const auto stored_table_node = StoredTableNode::make("index_test_table");
  auto predicate_node = PredicateNode::make(equals_(stored_table_node->get_column("a"), 4), stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;

  const auto pqp = LQPTranslator{}.translate_node(predicate_node);

  // Test correct LQP-Translation. For some reason, only GroupKeyIndexes are currently used.
  if (this->_index_type != SegmentIndexType::GroupKey) return;
  const auto indexed_chunks = std::vector<ChunkID>{ChunkID{1}};

  auto union_op = std::dynamic_pointer_cast<UnionAll>(pqp);
  ASSERT_TRUE(union_op);
  auto index_scan = std::dynamic_pointer_cast<IndexScan>(union_op->mutable_left_input());
  ASSERT_TRUE(index_scan);
  EXPECT_EQ(index_scan->included_chunk_ids, indexed_chunks);
  auto table_scan = std::dynamic_pointer_cast<TableScan>(union_op->mutable_right_input());
  ASSERT_TRUE(table_scan);
  EXPECT_EQ(table_scan->excluded_chunk_ids, indexed_chunks);
  auto get_table = std::dynamic_pointer_cast<GetTable>(table_scan->mutable_left_input());
  ASSERT_TRUE(get_table);

  // Add values:
  const auto table = Hyrise::get().storage_manager.get_table("index_test_table");
  table->append({4, 5});
  EXPECT_EQ(table->chunk_count(), 3);

  // Test correct execution:
  get_table->execute();
  table_scan->execute();
  index_scan->execute();
  union_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(
      union_op->get_output(),
      load_table("resources/test_data/tbl/int_int_shuffled_appended_and_filtered.tbl", ChunkOffset{10}));
}

}  // namespace opossum
