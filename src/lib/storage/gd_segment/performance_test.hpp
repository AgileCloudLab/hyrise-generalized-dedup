#include <vector>
#include <random>
#include <chrono>
#include "storage/gd_segment_v1.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "v1_iterable.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"

namespace gdsegment
{
    using namespace std;
    using namespace std::chrono;
    using namespace opossum; 

    namespace helpers 
    {
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
        unsigned long test_random_access(const GdSegmentV1<T>& segment, const vector<size_t>& indexes, const vector<T>& orig_data) {
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
    }

    namespace perf_test
    {

        // Record the speed and compression rate of a GdSegment at a specific deviation size
        struct SegmentPerformance {
            unsigned dev_bits;
            float compression_gain;

            unsigned long random_access_time;
            unsigned long sequential_access_time;
            unsigned long table_scan_time;

            void print() const {
                cout << dev_bits << " bits\t" << round(compression_gain*100) << "%\t"<< random_access_time << " ns\t" << sequential_access_time << " ns\n";
            }
        };

        
        template<typename T>
        vector<SegmentPerformance> test_v1(const vector<T>& data) {

            // TODO Read config from local file ()


            cout << "GdSegmentV1 performance test, segment size: " << data.size() << endl;

            vector<SegmentPerformance> results;
            // How many random accesses and tablescans are performed, as a percentage of the data size
            const auto tests_length_percent = 10.0;
            auto num_indexes = data.size() * (tests_length_percent/100.0);

            // Generate random row indexes for all random access tests
            const auto random_access_indexes = helpers::_generate_int_data<size_t>(num_indexes, 0, data.size()-1);
            /*
            cout << "Random access indexes: ";
            for(const auto& idx : random_access_indexes) { cout << idx << " "; }
            cout << endl;
            */

            // Generate random query values for all TableScans
            const auto tablescan_query_values = helpers::_generate_int_data<T>(num_indexes, *std::min_element(data.begin(), data.end()), *std::max_element(data.begin(), data.end()));
            
            const auto type_width_bits = sizeof(T) * 8;
            for(unsigned dev_bits=1 ; dev_bits <= 10 ; ++dev_bits) {
                
                const auto segment = GdSegmentV1<T>(data, dev_bits);

                SegmentPerformance perf;
                perf.dev_bits = segment.get_dev_bits();
                perf.compression_gain = segment.get_compression_gain();
                //cout << " Segment created, compression: " << round(perf.compression_gain*100) << "%" << endl;

                // Random Access
                perf.random_access_time = helpers::test_random_access(segment, random_access_indexes, data);
                //cout << "Random access time of GdSegmentV1@" << dev_bits << "dev: " << perf.random_access_time << " ns\n";

                // Sequential Access
                perf.sequential_access_time = helpers::test_sequential_access(segment, data);
                //cout << "Sequential access time of GdSegmentV1@" << dev_bits << "dev: " << perf.sequential_access_time << " ns\n";

                perf.print();

                results.push_back(perf);
            }

            return results;
        }

        
    }

}