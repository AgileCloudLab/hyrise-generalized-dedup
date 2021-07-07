#include "calibration_benchmark_runner.hpp"
#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "hyrise.hpp"
#include "operators/pqp_utils.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

void execute_calibration(const std::string& data_path, const std::shared_ptr<BenchmarkConfig>& config,
                         const float scale_factor);

int main(int argc, char** argv) {
  // Data generation settings
  const std::map<std::string, BenchmarkType> BENCHMARK_TYPES{
      {"tpch", BenchmarkType::TPC_H}, {"tpcds", BenchmarkType::TPC_DS}, {"tpcc", BenchmarkType::TPC_C},
      {"jcch", BenchmarkType::JCC_H}, {"job", BenchmarkType::JOB},      {"calibration", BenchmarkType::Calibration}};

  // create benchmark config
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Calibration Data Generator");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"))
    ("b,benchmark", "Benchmark to run. Choose one of tpch, tpcds, tpcc, jcch, job", cxxopts::value<std::string>())
    ("clustering", "Clustering of TPC-H data. The default of --clustering=None means the data is stored as generated "
                   "by the TPC-H data generator. With --clustering=\"Pruning\", the two largest tables 'lineitem' "
                   "and 'orders' are sorted by 'l_shipdate' and 'o_orderdate' for improved chunk pruning. Both are "
                   "legal TPC-H input data.", cxxopts::value<std::string>()->default_value("None"));
  // clang-format on

  const auto cli_parse_result = cli_options.parse(argc, argv);

  const auto benchmark_name = cli_parse_result["benchmark"].as<std::string>();
  const auto& benchmark_type = BENCHMARK_TYPES.at(benchmark_name);

  auto config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));
  config->cache_binary_tables = false;

  constexpr int NUMBER_BENCHMARK_EXECUTIONS = 1;
  const auto scale_factor = cli_parse_result["scale"].as<float>();
  constexpr bool SKEW_JCCH = false;

  // Export directory with scale factor, remove decimals if possible
  const auto sf_string = ceilf(scale_factor) == scale_factor ? std::to_string(static_cast<int>(scale_factor))
                                                             : std::to_string(scale_factor);

  auto clustering_configuration = ClusteringConfiguration::None;
  if (cli_parse_result.count("clustering")) {
    auto clustering_configuration_parameter = cli_parse_result["clustering"].as<std::string>();
    if (clustering_configuration_parameter == "Pruning") {
      clustering_configuration = ClusteringConfiguration::Pruning;
    } else if (clustering_configuration_parameter != "None") {
      Fail("Invalid clustering config: '" + clustering_configuration_parameter + "'");
    }

    std::cout << "- Clustering with '" << magic_enum::enum_name(clustering_configuration) << "' configuration"
              << std::endl;
  }
  const std::string clustering_string =
      benchmark_type == BenchmarkType::Calibration || clustering_configuration == ClusteringConfiguration::None
          ? ""
          : "_clustering-" + std::string(magic_enum::enum_name(clustering_configuration));
  const std::string DATA_PATH = "./data/" + benchmark_name + "_sf-" + sf_string + clustering_string;
  std::filesystem::create_directories(DATA_PATH);
  auto start = std::chrono::system_clock::now();

  std::cout << "Generating data" << std::endl;
  if (benchmark_type != BenchmarkType::Calibration) {
    // Execute benchmark
    auto benchmark_runner = CalibrationBenchmarkRunner(DATA_PATH, config, clustering_configuration, SKEW_JCCH);
    std::cout << "Run " << magic_enum::enum_name(benchmark_type) << std::endl;
    benchmark_runner.run_benchmark(benchmark_type, scale_factor, NUMBER_BENCHMARK_EXECUTIONS);
  } else {
    execute_calibration(DATA_PATH, config, scale_factor);
  }

  const auto test_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start).count();
  std::cout << "Generated data in " << test_duration << " s" << std::endl;
}

void execute_calibration(const std::string& data_path, const std::shared_ptr<BenchmarkConfig>& config,
                         const float scale_factor) {
  std::cout << "Run calibration" << std::endl;
  const std::set<DataType> TABLE_DATA_TYPES = {DataType::Double, DataType::Float,  DataType::Int,
                                               DataType::Long,   DataType::String, DataType::Null};
  const std::set<EncodingType> COLUMN_ENCODING_TYPES = {EncodingType::Dictionary};
  const std::vector<ColumnDataDistribution> COLUMN_DATA_DISTRIBUTIONS = {
      ColumnDataDistribution::make_uniform_config(0.0, 300'000.0)};
  const std::set<ChunkOffset> CHUNK_SIZES = {config->chunk_size, ChunkOffset{1'500'000}};
  std::set<int> ROW_COUNTS = {100'000, 6'000'000};
  if (scale_factor == 10.0) {
    ROW_COUNTS.emplace(60'000'000);
  }
  const auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
      TABLE_DATA_TYPES, COLUMN_ENCODING_TYPES, COLUMN_DATA_DISTRIBUTIONS, CHUNK_SIZES, ROW_COUNTS, scale_factor});
  std::cout << " - Generating tables" << std::endl;
  auto table_generator = CalibrationTableGenerator(table_config);
  auto tables = table_generator.generate();

  std::cout << " - Generating LQPS" << std::endl;
  auto feature_exporter = OperatorFeatureExporter(data_path);
  auto lqp_generator = CalibrationLQPGenerator();
  auto table_exporter = TableFeatureExporter(data_path);
  for (const auto& table : tables) {
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());
    lqp_generator.generate(OperatorType::TableScan, table);
  }
  const auto num_scans = lqp_generator.lqps().size();
  lqp_generator.generate_joins(tables, scale_factor);
  const auto num_joins = lqp_generator.lqps().size() - num_scans;
  //lqp_generator.generate_aggregates(tables);
  const auto lqps = lqp_generator.lqps();
  const auto lqp_count = lqps.size();
  const auto num_aggregates = lqp_count - num_scans - num_joins;

  std::cout << " - Generated " << lqp_count << " LQPS (" << num_scans << " scans, " << num_joins << " joins, "
            << num_aggregates << " aggregates)" << std::endl;

  std::cout << " - Running LQPs" << std::endl << "   ";
  size_t latest_percentage = 0;
  size_t current_lqp = 1;
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);

  for (size_t i{0}; i < 50; ++i) {
    std::cout << "-";
  }
  std::cout << std::endl << "   ";

  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    pqp->set_transaction_context_recursively(transaction_context);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    pqp->clear_output();

    // Export PQP directly after execution
    feature_exporter.export_to_csv(pqp);

    const auto percentage = (static_cast<float>(current_lqp) / static_cast<float>(lqp_count)) * 100;
    if (percentage >= latest_percentage + 2) {
      latest_percentage += 2;
      std::cout << "|" << std::flush;
    }
    ++current_lqp;
  }

  std::cout << std::endl << "Exporting data for calibration" << std::endl;
  feature_exporter.flush();
  for (const auto& table : tables) {
    table_exporter.export_table(table);
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }
  table_exporter.flush();
}
