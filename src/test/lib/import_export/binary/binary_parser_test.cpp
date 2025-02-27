#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

class BinaryParserTest : public BaseTest {
 protected:
  const std::string _reference_filepath = "resources/test_data/bin/";
};

class BinaryParserMultiEncodingTest : public BinaryParserTest, public ::testing::WithParamInterface<EncodingType> {};

auto import_binary_formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  auto stream = std::stringstream{};
  stream << info.param;

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(BinaryEncodingTypes, BinaryParserMultiEncodingTest,
                         ::testing::Values(EncodingType::Unencoded, EncodingType::Dictionary, EncodingType::RunLength,
                                           EncodingType::LZ4),
                         import_binary_formatter);

TEST_P(BinaryParserMultiEncodingTest, SingleChunkSingleFloatColumn) {
  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, ChunkOffset{5});
  expected_table->append({5.5f});
  expected_table->append({13.0f});
  expected_table->append({16.2f});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, MultipleChunkSingleFloatColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Float, false);
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
  expected_table->append({5.5f});
  expected_table->append({13.0f});
  expected_table->append({16.2f});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
  EXPECT_EQ(table->chunk_count(), 2u);
  // The binary importer finalizes all chunks
  EXPECT_FALSE(table->get_chunk(ChunkID{0})->is_mutable());
  EXPECT_FALSE(table->get_chunk(ChunkID{1})->is_mutable());
}

TEST_P(BinaryParserMultiEncodingTest, StringSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3}, UseMvcc::Yes);
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, AllTypesSegmentSorted) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, AllTypesSegmentUnsorted) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, AllTypesMixColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
  expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, EmptyStringsSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{10});

  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});
  expected_table->append({""});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, AllTypesNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data);

  expected_table->append({opossum::NULL_VALUE, 1.1f, int64_t{100}, "one", 1.11});
  expected_table->append({2, opossum::NULL_VALUE, int64_t{200}, "two", 2.22});
  expected_table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  expected_table->append({4, 4.4f, int64_t{400}, opossum::NULL_VALUE, 4.44});
  expected_table->append({5, 5.5f, int64_t{500}, "five", opossum::NULL_VALUE});

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, AllTypesAllNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data);
  auto null_values = {opossum::NULL_VALUE, opossum::NULL_VALUE, opossum::NULL_VALUE, opossum::NULL_VALUE,
                      opossum::NULL_VALUE};

  expected_table->append(null_values);
  expected_table->append(null_values);
  expected_table->append(null_values);
  expected_table->append(null_values);
  expected_table->append(null_values);

  const auto reference_filename =
      _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  auto table = BinaryParser::parse(reference_filename);

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, RepeatedInt) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});

  expected_table->append({1});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({1});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_P(BinaryParserMultiEncodingTest, RunNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{10});

  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({1});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({2});
  expected_table->append({2});
  expected_table->append({opossum::NULL_VALUE});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, LZ4MultipleBlocks) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Long, false);
  column_definitions.emplace_back("d", DataType::Float, false);
  column_definitions.emplace_back("e", DataType::Double, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{20'000});

  for (int index = 0; index < 5000; ++index) {
    expected_table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
    expected_table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
    expected_table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
    expected_table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});
  }

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, FixedStringDictionarySingleChunk) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{10});
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, FixedStringDictionaryNullValue) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{10});
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({"test"});
  expected_table->append({opossum::NULL_VALUE});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, FixedStringDictionaryMultipleChunks) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});
  expected_table->append({"This"});
  expected_table->append({"is"});
  expected_table->append({"a"});
  expected_table->append({"test"});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, NullValuesFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});
  expected_table->append({1});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({2});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({5});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, AllNullFrameOfReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({opossum::NULL_VALUE});
  expected_table->append({opossum::NULL_VALUE});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, InvalidEncodingType) {
  auto filename = _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_THROW(BinaryParser::parse(filename), std::exception);
}

TEST_F(BinaryParserTest, InvalidAttributeVectorWidth) {
  auto filename = _reference_filepath + ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin";
  EXPECT_THROW(BinaryParser::parse(filename), std::exception);
}

TEST_F(BinaryParserTest, FileDoesNotExist) { EXPECT_THROW(BinaryParser::parse("not_existing_file"), std::exception); }

TEST_F(BinaryParserTest, TwoColumnsNoValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("FirstColumn", DataType::Int, false);
  column_definitions.emplace_back("SecondColumn", DataType::String, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{30'000});

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(BinaryParserTest, SortColumnDefinitions) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, false);

  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3});
  // Chunk 0: a sorted ascending, b sorted descending
  expected_table->append({1, 3});
  expected_table->append({2, 2});
  expected_table->append({3, 1});
  // Chunk 1: a not sorted, b sorted descending
  expected_table->append({1, 3});
  expected_table->append({2, 2});
  expected_table->append({1, 1});
  // Chunk 2: a and b not sorted
  expected_table->append({1, 1});
  expected_table->append({2, 2});
  expected_table->append({1, 1});

  expected_table->last_chunk()->finalize();

  // Set sorted by information
  const auto chunk_0_sorted_columns = std::vector<SortColumnDefinition>{
      SortColumnDefinition{ColumnID{0}}, SortColumnDefinition{ColumnID{1}, SortMode::Descending}};
  const auto chunk_1_sorted_columns =
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{1}, SortMode::Descending}};
  expected_table->get_chunk(ChunkID{0})->set_individually_sorted_by(chunk_0_sorted_columns);
  expected_table->get_chunk(ChunkID{1})->set_individually_sorted_by(chunk_1_sorted_columns);

  auto table = BinaryParser::parse(_reference_filepath +
                                   ::testing::UnitTest::GetInstance()->current_test_info()->name() + ".bin");

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
  EXPECT_EQ(table->get_chunk(ChunkID{0})->individually_sorted_by(), chunk_0_sorted_columns);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->individually_sorted_by(), chunk_1_sorted_columns);
  EXPECT_TRUE(table->get_chunk(ChunkID{2})->individually_sorted_by().empty());
}

}  // namespace opossum
