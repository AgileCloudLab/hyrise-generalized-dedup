#pragma once

#include <vector>
#include <random>
#include <chrono>
#include "storage/gd_segment_v1.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "v1_iterable.hpp"
#include "config_parser.hpp"
#include "segment_performance.hpp"


namespace gdsegment
{
    using namespace std;
    using namespace std::chrono;
    using namespace opossum; 
    using namespace config_parser;

    namespace helpers 
    {

        template<typename T>
        float avg(const vector<T>& vec){
            const auto sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            return sum/(float)vec.size();
        }

        
        // Scale all values of a signal from their original domain to [0-1]
        template<typename T>
        vector<float> normalize_signal(const vector<T>& signal){
            const T min = *std::min_element(signal.begin(), signal.end()), 
                        max = *std::max_element(signal.begin(), signal.end());
            const auto range = max - min;
            
            if(range == 0) {
                // All values are equal
                return vector<float>(signal.size(), 0.5f);
            }

            vector<float> normalized(signal.size());
            for(auto i=0U ; i<signal.size() ; ++i){
                normalized[i] = (signal[i] - min) / (float) range;
            }

            return normalized;
        }

        // Finds the signal index that is minimal with the given weights
        unsigned find_best_weighted_signal(const vector<vector<float>>& signals_normalized, const vector<float>& weights) {
            if(signals_normalized.size() != weights.size()){
                throw new std::runtime_error("Weights must have the same length as the number of signals");
            }

            // 
            vector<float> weighted_sums_per_devbit;
            float sum;
            for(auto i=0U ; i<signals_normalized[0].size() ; ++i) {
                // Calculate the weighted sum of signals at the current dev bits
                sum = 0.0;
                for(auto signal_idx=0U ; signal_idx < signals_normalized.size() ; signal_idx++){
                    sum += weights[signal_idx] * signals_normalized[signal_idx][i];
                }
                weighted_sums_per_devbit.push_back(sum);
            }

            // The index (devbits-1) with the smallest weighted sum is the best
            const auto smallest_weighted_sum = *std::min_element(weighted_sums_per_devbit.begin(), weighted_sums_per_devbit.end());
            const auto smallest_weighted_sum_it = std::find(weighted_sums_per_devbit.begin(), weighted_sums_per_devbit.end(), smallest_weighted_sum);
            return std::distance(weighted_sums_per_devbit.begin(), smallest_weighted_sum_it);
        }

        template<typename IntType=int>
        vector<IntType> _generate_int_data(size_t size, IntType min, IntType max){
            vector<IntType> data(size);

            std::random_device rd{};
            std::mt19937 rng(rd());
            std::uniform_int_distribution<IntType> generator(min, max);
            std::generate(data.begin(), data.end(), [&](){
                return generator(rng);
            });
            
            return data;
        }

        template<typename T>
        unsigned long test_random_access(const GdSegmentV1<T>& segment, const vector<uint32_t>& indexes, const vector<T>& orig_data) {
            auto segment_iterable = create_iterable_from_segment(segment);
            // Fill the position filter
            
            pmr_vector<RowID> rowids;
            rowids.reserve(indexes.size());
            const ChunkID chunk_id_zero{0};
            for(const auto& idx : indexes){
                rowids.emplace_back(RowID{chunk_id_zero, ChunkOffset{idx}});
            }
            auto pos_filter_ptr = std::make_shared<RowIDPosList>(std::move(rowids));
            pos_filter_ptr->guarantee_single_chunk();

            vector<T> results;
            results.reserve(indexes.size());

            const auto before = high_resolution_clock::now();
            segment_iterable.with_iterators(pos_filter_ptr, [&](auto segment_begin, auto segment_end) {
                // Dereference values 
                while(segment_begin != segment_end){
                    results.push_back( (*segment_begin).value() );
                    segment_begin++;
                }
            });
            const auto after = high_resolution_clock::now();
            const auto total_time = chrono::duration_cast<chrono::nanoseconds>(after - before).count();
            const auto average_time = round(total_time / (float)indexes.size());

            // Verify
            DebugAssert(results.size() == indexes.size(), "Results: "+std::to_string(results.size())+", random access indexes: "+std::to_string(indexes.size()));
            auto results_it = results.begin();
            for(const auto& idx : indexes){
                DebugAssert(orig_data[idx] == *results_it, " ERROR GdSegmentV1@"+std::to_string(segment.get_dev_bits())+" Random Access error at index #"+std::to_string(idx) + ". Data: " + std::to_string(orig_data[idx]) + ", result: " + std::to_string(*results_it));
                results_it++;
            }

            return average_time;
        }

        
        template<typename T>
        bool evaluate_predicate(const PredicateCondition& condition, const T& left, const T& right){
            switch(condition){
                case PredicateCondition::Equals:          return left == right;
                case PredicateCondition::NotEquals:       return left != right;
                case PredicateCondition::LessThan:        return left < right;
                case PredicateCondition::LessThanEquals:  return left <= right;
                case PredicateCondition::GreaterThan:     return left > right;
                case PredicateCondition::GreaterThanEquals: return left >= right;
                default: return false;
            }
        };

        std::string predicate_to_string(const PredicateCondition& p){
            switch(p){
                case PredicateCondition::Equals: return "Equals";
                case PredicateCondition::NotEquals: return "NotEquals";
                case PredicateCondition::LessThan: return "LessThan";
                case PredicateCondition::LessThanEquals: return "LessThanEquals";
                case PredicateCondition::GreaterThan: return "GreaterThan";
                case PredicateCondition::GreaterThanEquals: return "GreaterThanEquals";
                default: return "UNKNOWN PREDICATE " + std::to_string((int)p);
            }
        }

        template<typename T>
        unsigned long test_sequential_access(const GdSegmentV1<T>& segment, const vector<T>& orig_data) {
            auto segment_iterable = create_iterable_from_segment(segment);
            
            // Position filter = entire chunk
            //auto pos_filter_ptr = std::make_shared<EntireChunkPosList>(ChunkID{0}, ChunkOffset{segment.rows_num()});

            vector<T> results;
            results.reserve(orig_data.size());

            const auto before = high_resolution_clock::now();
            segment_iterable.with_iterators([&](auto segment_begin, auto segment_end) {
                // Dereference values 
                while(segment_begin != segment_end){
                    results.push_back( (*segment_begin).value() );
                    segment_begin++;
                }
            });
            const auto after = high_resolution_clock::now();
            const auto total_time = chrono::duration_cast<chrono::nanoseconds>(after - before).count();
            const auto average_time = round(total_time / (float)segment.size());

            // Verify
            DebugAssert(results.size() == segment.rows_num(), "SeqAccess: Different number of results than segment size! Segment: "+std::to_string(segment.rows_num())+", results: "+std::to_string(results.size()));
            DebugAssert(std::equal(results.begin(), results.end(), orig_data.begin()), "SeqAccess: Results different than original data");

            return average_time;
        }

        const std::vector<PredicateCondition> all_predicates = {
            PredicateCondition::Equals,
            PredicateCondition::NotEquals,
            PredicateCondition::LessThan,
            PredicateCondition::LessThanEquals,
            PredicateCondition::GreaterThan,
            PredicateCondition::GreaterThanEquals
        };

        template<typename T>
        unsigned long test_table_scan(const GdSegmentV1<T>& segment, const vector<T>& query_values, const vector<T>& orig_data) {
            
            // @TODO allow the caller specify which predicates they want tested

            high_resolution_clock::time_point before, after;
            vector<unsigned long> ts_times;
            ts_times.reserve(all_predicates.size() * query_values.size());
            
            RowIDPosList correct_results;
            bool are_results_incorrect = false;

            for(const auto& condition : all_predicates) {
                for(const auto& query_value : query_values) {
                    RowIDPosList results;

                    before = high_resolution_clock::now();
                    segment.segment_vs_value_table_scan(
                        condition,
                        AllTypeVariant(query_value),
                        ChunkID{0},
                        results,
                        nullptr
                    );
                    after = high_resolution_clock::now();
                    ts_times.push_back(chrono::duration_cast<chrono::nanoseconds>(after - before).count());

                    // Verify
                    if(false) {
                        correct_results.clear();
                        correct_results.reserve(orig_data.size());
                        for(auto i=0U ; i<orig_data.size() ; ++i){
                            if(evaluate_predicate<T>(condition, orig_data[i], query_value)){
                                correct_results.push_back(RowID{ChunkID{0}, ChunkOffset{i}});
                            }
                        }

                        if(!std::equal(correct_results.begin(), correct_results.end(), results.begin())){
                            cout << "Wrong TS results at " << predicate_to_string(condition) << " " << query_value << endl;
                            are_results_incorrect = true;
                        }
                    }
                }
            } // end tablescans

            if(are_results_incorrect){
                throw new std::runtime_error("Wrong TS results");
            }
            const auto average_time = round(avg(ts_times));
            return average_time;
        }

        template<typename T>
        unsigned long test_table_scan_valuesegment(const GdSegmentV1<T>& segment, const vector<T>& query_values, const vector<T>& orig_data) {
            // Same as test_table_scan but with a ValueSegment (for comparison of our custom TS implementation in GdSegment)
            
            pmr_vector<T> pmr_values(orig_data.begin(), orig_data.end());
            ValueSegment<T> value_segment(std::move(pmr_values));

            auto segment_iterable = create_iterable_from_segment(segment);
            RowIDPosList value_results;

            high_resolution_clock::time_point before, after;
            vector<unsigned long> value_ts_times;
            value_ts_times.reserve(all_predicates.size() * query_values.size());
            uint32_t chunk_offset;

            for(const auto& condition : all_predicates) {
                for(const auto& query_value : query_values) {
                    value_results.clear();

                    before = high_resolution_clock::now();
                    segment_iterable.with_iterators([&](auto segment_it, auto segment_end) {
                        // Dereference values 
                        chunk_offset = 0;
                        while(segment_it != segment_end){
                            if(evaluate_predicate<T>(condition, (*segment_it).value(), query_value)){
                                value_results.push_back(RowID{ChunkID{0}, ChunkOffset{chunk_offset}});
                            }
                            segment_it++;
                            chunk_offset++;
                        }
                    });
                    after = high_resolution_clock::now();
                    value_ts_times.push_back(chrono::duration_cast<chrono::nanoseconds>(after - before).count());
                }
            }

            const auto value_average_time = round(avg(value_ts_times));
            return value_average_time;
        }
    }

    namespace perf_test
    {

        template<typename T>
        vector<SegmentPerformance> test_v1(
            const vector<T>& data, 
            const unsigned min_dev_bits,
            const unsigned max_dev_bits,
            const float random_access_fraction,
            const float tablescan_fraction,

            const bool measure_random_access=true, 
            const bool measure_sequential_access=true, 
            const bool measure_table_scan=true
        ) {

            vector<SegmentPerformance> results;

            // Generate random row indexes for all random access tests
            const auto random_access_indexes = measure_random_access ? helpers::_generate_int_data<uint32_t>( max<size_t>(1, (size_t) data.size() * random_access_fraction), 0, data.size()-1) : vector<uint32_t>();

            // Generate random query values for all TableScans
            const auto tablescan_query_values = measure_table_scan ? helpers::_generate_int_data<T>(max<size_t>(1, (size_t) data.size() * tablescan_fraction), *std::min_element(data.begin(), data.end()), *std::max_element(data.begin(), data.end())) : vector<T>();
            
            // Run the requested tests
            for(unsigned dev_bits=min_dev_bits ; dev_bits <= max_dev_bits ; ++dev_bits) {
                
                const auto segment = GdSegmentV1<T>(data, dev_bits);

                SegmentPerformance perf;
                perf.dev_bits = segment.get_dev_bits();
                perf.compression_gain = segment.get_compression_gain();

                if(measure_random_access){
                    perf.random_access_time = helpers::test_random_access(segment, random_access_indexes, data);
                }

                if(measure_sequential_access) {
                    perf.sequential_access_time = helpers::test_sequential_access(segment, data);
                }

                if(measure_table_scan){
                    perf.table_scan_time = helpers::test_table_scan(segment, tablescan_query_values, data);
                    perf.table_scan_time_valuesegment = helpers::test_table_scan_valuesegment(segment, tablescan_query_values, data);
                }

                //perf.print();

                results.push_back(perf);
            }

            return results;
        }

        // Returns the deviation size to use, based on the measured performance and selection preferences
        unsigned evaluate_perf_results(
            const vector<SegmentPerformance>& measured_perfs, 
            const config_parser::GdSelectionConfig& config
        ) {
            if(config.fixed_dev_bits > 0){
                // Fixed deviation size is requested
                return config.fixed_dev_bits;
            }

            const vector<float> weights = {
                config.weight_comprate,
                config.weight_random_access,
                config.weight_sequential_access,
                config.weight_tablescan
            };
            vector<vector<float>> signals = {{}, {}, {}, {}};
            
            for(const auto& perf : measured_perfs){
                signals[0].push_back(1 - perf.compression_gain);
                signals[1].push_back(perf.random_access_time);
                signals[2].push_back(perf.sequential_access_time);
                signals[3].push_back(perf.table_scan_time);
            }

            // normalize signals
            signals[0] = helpers::normalize_signal(signals[0]);
            signals[1] = helpers::normalize_signal(signals[1]);
            signals[2] = helpers::normalize_signal(signals[2]);
            signals[3] = helpers::normalize_signal(signals[3]);

            const auto best_idx = helpers::find_best_weighted_signal(signals, weights);
            const auto best_dev_bits = measured_perfs[best_idx].dev_bits;
            return best_dev_bits;

        }
    }

}