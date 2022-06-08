
#include "_gd_segment_v1_devbits.hpp"
#include "gdd_lsb.hpp"
#include "helpers.hpp"
#include <vector>

using namespace opossum;
using namespace std;
using namespace gdsegment;

template<typename T, unsigned DevBits>
GdSegmentV1DevBits<T, DevBits>::GdSegmentV1DevBits(const vector<T>& data) : 
    BaseGdSegment(data_type_from_type<T>())
{
    std::vector<T> std_bases;
    std::vector<unsigned> std_deviations;
    std::vector<size_t> std_base_indexes;
    gdd_lsb::ct::encode<T, DevBits>(data, std_bases, std_deviations, std_base_indexes);

    // Fill compact vectors
    bases.resize(std_bases.size());
    for(auto i=0 ; i<bases.size() ; ++i){
        bases[i] = std_bases[i];
    }

    deviations.resize(std_deviations.size());
    for(auto i=0 ; i<deviations.size() ; ++i){
        deviations[i] = std_deviations[i];
    }

    const auto max_base_index = bases.size() - 1;
    const auto bits = (max_base_index == 0) ? 1 : gd_helpers::num_bits(max_base_index);
    //@TODO if we wanted byte-aliged compact vector, round up 'bits' to the next multiple of 8
    compact::vector<size_t> recon_list(bits, std_base_indexes.size());
    for(auto i=0 ; i<std_base_indexes.size() ; ++i){
        recon_list[i] = std_base_indexes[i];
    }
    reconstruction_list = make_shared<decltype(recon_list)>(recon_list);

    // Fill min and max
    segment_min = *std::min_element(data.begin(), data.end());
    segment_max = *std::max_element(data.begin(), data.end());
}

template<typename T, unsigned DevBits>
void GdSegmentV1DevBits<T, DevBits>::segment_vs_value_table_scan(
        const PredicateCondition& condition, 
        const AllTypeVariant& query_value_atv, 
        const ChunkID chunk_id, 
        RowIDPosList& results,
        const std::shared_ptr<const AbstractPosList>& position_filter) const
{
    // Convert AllTypeVariant to the raw type
    const T query_value = boost::get<T>(query_value_atv);

    { // Step 1: early exit based on segment range
        switch(condition) {

            case PredicateCondition::Equals:  
            {
                // If query value is out of range: no matches
                if (query_value < segment_min || query_value > segment_max){
                    return;
                }
                break;
            }

            case PredicateCondition::NotEquals: 
            {
                // If query value is out of range: all matches
                if (query_value < segment_min || query_value > segment_max){
                    // Add all indexes (chunk offsets)
                    _all_to_matches(chunk_id, results, position_filter);
                    return;
                }
                break;
            }

            case PredicateCondition::GreaterThan:
            {
                if (query_value >= segment_max){
                    // No match
                    return;
                }
                else if (query_value < segment_min){
                    // All
                    _all_to_matches(chunk_id, results, position_filter);
                    return;
                }
                break;
            }

            case PredicateCondition::GreaterThanEquals:
            {
                if (query_value > segment_max){
                    // No match
                    return;
                }
                else if (query_value <= segment_min){
                    // All
                    _all_to_matches(chunk_id, results, position_filter);
                    return;
                }
                break;
            }

            case PredicateCondition::LessThan:
            {
                if (query_value <= segment_min){
                    // No match
                    return;
                }
                else if (query_value > segment_max){
                    // All
                    _all_to_matches(chunk_id, results, position_filter);
                    return;
                }
                break;
            }

            case PredicateCondition::LessThanEquals:
            {
                if (query_value < segment_min){
                    // No match
                    return;
                }
                else if (query_value >= segment_max){
                    // All
                    _all_to_matches(chunk_id, results, position_filter);
                    return;
                }
                break;
            }
            
            default: break;
        }
    }

    // Check if the query base is present
    const T query_base = gdd_lsb::ct::make_base<T, DevBits>(query_value);
    const unsigned query_deviation = gdd_lsb::ct::make_deviation<unsigned, DevBits>(query_value);

    const auto lower_it = std::lower_bound(bases.cbegin(), bases.cend(), query_base);

    const bool is_query_base_present = std::distance(bases.cbegin(), lower_it) <= bases.size() && (*lower_it == query_base);
    //const bool is_query_base_present = (lower_it != bases.cend()) && (*lower_it == query_base);
    const size_t query_value_base_idx = std::distance(bases.cbegin(), lower_it);

    // Allocate results eagerly
    results.reserve(results.size() + (position_filter ? position_filter->size() : size()));

    switch(condition) {
        case PredicateCondition::Equals:
        {
            if(is_query_base_present) {
                // Add values where both the base and the deviation equals
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx == query_value_base_idx) && (deviations[rowidx] == query_deviation);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
            }
            else {
                // Else: no results
                return;
            }
            break;
        }
        case PredicateCondition::NotEquals:
        {
            if(is_query_base_present) {
                // Add values where either the base or the deviation are different
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx != query_value_base_idx) || (deviations[rowidx] != query_deviation);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);

            }
            else {
                // Else: all rowindexes are match
                _all_to_matches(chunk_id, results, position_filter, true);
                return;
            }
            break;
        }
        case PredicateCondition::GreaterThan:
        case PredicateCondition::GreaterThanEquals:
        {
            if(is_query_base_present){
                // Add values of higher bases
                // Add values of the query base where the deviation is > or >= than the query deviation
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx > query_value_base_idx) || 
                        (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThan && deviations[rowidx] > query_deviation) ||
                        (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThanEquals && deviations[rowidx] >= query_deviation);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                
            }
            else {
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx >= query_value_base_idx);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
            }
            break;
        }

        case PredicateCondition::LessThan:
        case PredicateCondition::LessThanEquals:
        {
            if(is_query_base_present) {
                // Add values of the query base where deviation is less or lessequal than the query deviation
                // Add values of lower bases
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx < query_value_base_idx) || 
                        (base_idx == query_value_base_idx && condition == PredicateCondition::LessThan && deviations[rowidx] < query_deviation) ||
                        (base_idx == query_value_base_idx && condition == PredicateCondition::LessThanEquals && deviations[rowidx] <= query_deviation);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
            }
            else {
                // Query base not present, add lower bases
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx < query_value_base_idx);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
            }
            break;
        }

        default: throw new std::runtime_error("Unexpected predicate in GD TableScan");
    }
}

template<typename T, unsigned DevBits>
template<typename Functor>
inline void GdSegmentV1DevBits<T, DevBits>::_add_matches(
    const ChunkID chunk_id, 
    RowIDPosList& results,
    const std::shared_ptr<const AbstractPosList>& position_filter,
    Functor base_idx_qualifies) const
{
    const auto recon_list = *reconstruction_list;
    if(position_filter) {
        // Check only the position filter
        size_t pf_idx = 0U, rowidx, base_idx;
        for(const auto& pf : *position_filter){
            rowidx = pf.chunk_offset;
            base_idx = recon_list[rowidx];
            if (base_idx_qualifies(base_idx, rowidx)) {
                // Add position filter index if the rowidx qualifies
                results.push_back(RowID{chunk_id, ChunkOffset{pf_idx}});
            }
            ++pf_idx;
        }
    }
    else {
        size_t base_idx;
        for(auto rowidx=ChunkOffset{0} ; rowidx<recon_list.size() ; ++rowidx){
            base_idx = recon_list[rowidx];
            if (base_idx_qualifies(base_idx, rowidx)) {
                results.push_back(RowID{chunk_id, rowidx});
            }
        }
    }
}

template<typename T, unsigned DevBits>
inline void GdSegmentV1DevBits<T, DevBits>::_all_to_matches(
    const ChunkID& chunk_id, 
    RowIDPosList& results, 
    const std::shared_ptr<const AbstractPosList>& position_filter,
    const bool results_preallocated) const 
{   
    // Preallocate if it has not happened already
    if(!results_preallocated){
        results.reserve(results.size() + (position_filter ? position_filter->size() : size()));
    }

    if(position_filter){
        // Add all PF indexes
        for(auto i=ChunkOffset{0} ; i<position_filter->size() ; ++i){
            results.push_back(RowID{chunk_id, i});
        }
    }
    else{
        // Add all row indexes
        for(auto rowidx=ChunkOffset{0} ; rowidx < size() ; ++rowidx){
            results.push_back(RowID{chunk_id, rowidx});
        }
    }
}

template<typename T, unsigned DevBits>
void GdSegmentV1DevBits<T, DevBits>::decompress(vector<T>& data) const {
    const auto recon_list = *reconstruction_list;
    data.resize(recon_list.size());
    for(auto rowidx=0U ; rowidx < recon_list.size() ; ++rowidx) {
        data[rowidx] = gdd_lsb::ct::reconstruct_value<T, DevBits>(bases[recon_list[rowidx]], deviations[rowidx]);
    }
}

template<typename T, unsigned DevBits>
ChunkOffset GdSegmentV1DevBits<T, DevBits>::size() const {
    return ChunkOffset{ reconstruction_list->size() };
}

template<typename T, unsigned DevBits>
size_t GdSegmentV1DevBits<T, DevBits>::memory_usage(const MemoryUsageCalculationMode mode) const {
    const auto bases_size = bases.bytes();
    const auto deviations_size = deviations.bytes();
    const auto reconstruction_list_size = reconstruction_list->bytes();
    return bases_size + deviations_size + reconstruction_list_size + 8; 
}

template<typename T, unsigned DevBits>
std::shared_ptr<AbstractSegment> GdSegmentV1DevBits<T, DevBits>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    vector<T> data;
    decompress(data);
    return make_shared<GdSegmentV1DevBits<T, DevBits>>(data);
}

template<typename T, unsigned DevBits>
float GdSegmentV1DevBits<T, DevBits>::get_compression_gain() const {
    const auto orig_data_size_bytes = sizeof(T) * size();
    return helpers::compression_gain(orig_data_size_bytes, memory_usage());
}