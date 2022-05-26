#pragma once

#include "storage/base_segment_encoder.hpp"
#include "storage/gd_segment_v1.hpp" 
#include "types.hpp"
#include "utils/enum_constant.hpp"
#include "performance_test.hpp"
#include "config_parser.hpp"
#include "result_writer.hpp"

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
            return std::make_shared<GdSegmentV1<T>>(values, config.fixed_dev_bits);
        }

        if(!config.weights_ok()){
            config.print();
            throw new std::runtime_error("Weights not OK");
        }

        const auto max_bits = config.max_dev_bits == 0 ? (sizeof(T)*8 - 2) : config.max_dev_bits;
        // Measure performance
        //cout << "GdSegmentV1 performance test for " << table_col_name << " chunk #" << chunk_index << ": " << values.size() << " rows" << endl;
        
        // TODO try to load results from a file

        const auto perf_results = perf_test::test_v1<T>(
            values, 
            config.min_dev_bits, max_bits,
            config.random_access_rows_fraction,
            config.tablescan_values_fraction,
             
            config.weight_random_access > 0.0, // do not measure random access if its weight is zero
            config.weight_sequential_access > 0.0, // do not measure seq access if its weight is zero
            config.weight_tablescan > 0.0 // do not measure tablescan if its weight is zero
        );
        
        { // Store results as a file
            const string filename = "gd_segment_results.json";
            string data;
            for(const auto& results_per_devbits : perf_results){
                data += "{\"name\":\""+table_col_name+"\", \"chunk_idx\": "+std::to_string(chunk_index)+", \"rows\":"+std::to_string(values.size())+", \"results\": ";
                data += results_per_devbits.to_json_obj();
                data += "},\n";
            }
            auto result_writer = GdResultWriter::getinstance(filename);
            result_writer->write(data);
        }

        
        // @TODO decide which one is the best and return a shared pointer to it
        const unsigned best_deviation_size = perf_test::evaluate_perf_results(perf_results, config);

        // Create the segment
        std::cout << table_col_name + " chunk #" + std::to_string(chunk_index) + " best deviation size: " + std::to_string(best_deviation_size) + " bits\n";
        return std::make_shared<GdSegmentV1<T>>(values, best_deviation_size);
    }
};