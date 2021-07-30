#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"

#include "statistics/statistics_objects/top_k_uniform_distribution_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class TopKUniformDistributionHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl");
    _float2 = load_table("resources/test_data/tbl/float2.tbl");
    _string2 = load_table("resources/test_data/tbl/string2.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _string2;
};

/* Test for strings if the number of values is below K, every value has its own bucket */
TEST_F(TopKUniformDistributionHistogramTest, FromColumnString) {
  StringHistogramDomain default_domain;
  const auto default_domain_histogram =
      TopKUniformDistributionHistogram<pmr_string>::from_column(*_string2, ColumnID{0}, default_domain);

  // _string2 has 11 distinct values
  ASSERT_EQ(default_domain_histogram->bin_count(), 11u);
  EXPECT_EQ(default_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "aa", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("b", "b", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("birne", "birne", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{3}), HistogramBin<pmr_string>("bla", "bla", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{4}), HistogramBin<pmr_string>("bums", "bums", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{5}), HistogramBin<pmr_string>("ttt", "ttt", 2, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{6}), HistogramBin<pmr_string>("uuu", "uuu", 2, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{7}), HistogramBin<pmr_string>("www", "www", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{8}), HistogramBin<pmr_string>("xxx", "xxx", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{9}), HistogramBin<pmr_string>("yyy", "yyy", 1, 1));
  EXPECT_EQ(default_domain_histogram->bin(BinID{10}), HistogramBin<pmr_string>("zzz", "zzz", 3, 1));

}


/* Test for ints if the number of values is below K, every value has its own bucket */
TEST_F(TopKUniformDistributionHistogramTest, FromColumnInt) {
  const auto hist = TopKUniformDistributionHistogram<int32_t>::from_column(*_int_float4, ColumnID{0});

  // _int_float_4 has 4 distinct values
  ASSERT_EQ(hist->bin_count(), 4u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<int32_t>(12, 12, 1, 1));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<int32_t>(123, 123, 1, 1));
  EXPECT_EQ(hist->bin(BinID{2}), HistogramBin<int32_t>(12345, 12345, 2, 1));
  EXPECT_EQ(hist->bin(BinID{3}), HistogramBin<int32_t>(123456, 123456, 3, 1));
}


/* Test for floats if the number of values is below K, every value has its own bucket */
TEST_F(TopKUniformDistributionHistogramTest, FromColumnFloat) {
  auto hist = TopKUniformDistributionHistogram<float>::from_column(*_float2, ColumnID{0});

  // _float_2 has 10 distinct values
  ASSERT_EQ(hist->bin_count(), 10u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<float>(0.5f, 0.5f, 1, 1));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<float>(1.1f, 1.1f, 1, 1));
  EXPECT_EQ(hist->bin(BinID{2}), HistogramBin<float>(1.9f, 1.9f, 1, 1));
  EXPECT_EQ(hist->bin(BinID{3}), HistogramBin<float>(2.2f, 2.2f, 1, 1));
  EXPECT_EQ(hist->bin(BinID{4}), HistogramBin<float>(2.5f, 2.5f, 3, 1));
  EXPECT_EQ(hist->bin(BinID{5}), HistogramBin<float>(3.1f, 3.1f, 1, 1));
  EXPECT_EQ(hist->bin(BinID{6}), HistogramBin<float>(3.3f, 3.3f, 2, 1));
  EXPECT_EQ(hist->bin(BinID{7}), HistogramBin<float>(3.6f, 3.6f, 1, 1));
  EXPECT_EQ(hist->bin(BinID{8}), HistogramBin<float>(4.4f, 4.4f, 2, 1));
  EXPECT_EQ(hist->bin(BinID{9}), HistogramBin<float>(6.1f, 6.1f, 1, 1));
}

}  // namespace opossum
