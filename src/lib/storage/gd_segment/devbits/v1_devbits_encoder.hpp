#pragma once

#include "storage/base_segment_encoder.hpp"
#include "storage/gd_segment_v1_fwd.hpp" 
#include "_gd_segment_v1_devbits.hpp" 
#include "types.hpp"
#include "utils/enum_constant.hpp"
#include "performance_test.hpp"
#include "config_parser.hpp"
#include "result_writer.hpp"
#include "segment_performance.hpp"

#include <memory>
#include <algorithm>
#include <iostream>

using namespace opossum;
using namespace gdsegment;

class GdV1Encoder : public SegmentEncoder<GdV1Encoder> {

public:

    static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::GdV1>;
    static constexpr auto _uses_vector_compression = false;  // see base_segment_encoder.hpp for details
    
    template <typename T>
    std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator,
                                                     const std::string& table_col_name, 
                                                     const int chunk_index)
    {
        // Extract the values from the segment iterable
        std::vector<T> values;
        segment_iterable.with_iterators([&](auto segment_it, auto segment_end) {
            values.reserve(std::distance(segment_it, segment_end));
            while(segment_it != segment_end) {
                const auto segment_item = *segment_it;
                if (!segment_item.is_null()) {
                    const auto segment_value = segment_item.value();
                    values.push_back(segment_value);
                } else {
                    throw new std::runtime_error("Gd Segment V1 does not support NULLs");
                }
                ++segment_it;
            }
        });

        // Load the evaluation config
        const auto config = config_parser::read_config("./gd_segment_prefs.txt");
        if(config.fixed_dev_bits > 0){
            // No need to measure anything
            cout << table_col_name + " chunk #" + to_string(chunk_index) + " using fixed " + to_string(config.fixed_dev_bits) + " bit deviations." << endl;
            return std::make_shared<GdSegmentV1<T>>(values, config.fixed_dev_bits);
        }

        if(!config.weights_ok()){
            config.print();
            throw new std::runtime_error("Weights not OK");
        }
        
        // Measure performance
        //cout << "GdSegmentV1 performance test for " << table_col_name << " chunk #" << chunk_index << ": " << values.size() << " rows" << endl;
        
        vector<SegmentPerformance> perf_results;
        const string perf_results_filename = "gd_segment_results.json";
        bool json_load_successful = false;
        
        { // Try to load results from a file
            GdResultReader results_reader(perf_results_filename);
            if(results_reader.file_exists()){
                const auto json_string = "[" + results_reader.get_contents() + "]";
                GdResultParser parser(json_string);
                // query the measurements for this segment (assuming the same measurements had been done)
                perf_results = parser.get_measurements(table_col_name, chunk_index, values.size());
                json_load_successful = !perf_results.empty();
            }

            if(json_load_successful){
                cout << table_col_name + " chunk #" + to_string(chunk_index) + " perf measurements loaded from JSON: "+to_string(perf_results.size())+"\n";
            }
            else{
                cout << table_col_name + " chunk #" + to_string(chunk_index) + " perf measurements not found in JSON\n";
            }
        }

        if(perf_results.empty()){
            // results were not loaded from json, run the tests
            const auto max_bits = config.max_dev_bits == 0 ? (sizeof(T)*8 - 2) : config.max_dev_bits;
            perf_results = perf_test::test_v1<T>(
                values, 
                config.min_dev_bits, max_bits,
                config.random_access_rows_fraction,
                config.tablescan_values_fraction,
                
                config.weight_random_access > 0.0, // do not measure random access if its weight is zero
                config.weight_sequential_access > 0.0, // do not measure seq access if its weight is zero
                config.weight_tablescan > 0.0 // do not measure tablescan if its weight is zero
            );
        }
        
        if(!json_load_successful) { 
            // Store results to json
            auto result_writer = GdResultWriter::getinstance(perf_results_filename);
            result_writer->write_measurements(perf_results, table_col_name, chunk_index, values.size());
        }

        // Decide which deviation size is the best using the relative importance of attributes from the config
        const unsigned best_deviation_size = perf_test::evaluate_perf_results(perf_results, config);

        // Create the segment
        std::cout << table_col_name + " chunk #" + std::to_string(chunk_index) + " best deviation size: " + std::to_string(best_deviation_size) + " bits\n";
        const auto segment_ptr = gdsegment::make_gd_segment_v1<T>(values, best_deviation_size);

        return std::make_shared<GdSegmentV1<T>>(segment_ptr);
    }
};