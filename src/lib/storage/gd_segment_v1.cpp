#include "base_gd_segment.hpp"
#include "gd_segment_v1.hpp"
#include "gd_segment/gdd_lsb.hpp"

#include <bit>
#include <iostream>
#include <memory>
#include <limits>
#include <vector>

namespace opossum {

using namespace std;

namespace helpers {

    // Returns the minimum number of bits needed to represent every element
    // in a signed int array
    template<typename T>
    size_t int_vec_num_bits(const std::vector<T>& data){
        auto max_bits = 0U;
        auto curr_bits = max_bits;

        const auto base = sizeof(unsigned) * 8;
        for(const auto& d : data) {
            curr_bits = base - std::countl_zero((unsigned) abs(d));
            if(curr_bits > max_bits){
                max_bits = curr_bits;
            }
        }
        // Add an extra bit for the sign
        return max_bits + 1;
    }

}



template <typename T, typename U>
GdSegmentV1<T, U>::GdSegmentV1(const std::vector<T>& data, const uint8_t dev_bits, const std::vector<bool>& null_values) : 
    BaseGdSegment(data_type_from_type<T>()), 
    dev_bits(dev_bits),
    segment_min(*std::min_element(data.begin(), data.end())),
    segment_max(*std::max_element(data.begin(), data.end()))
{
    std::vector<T> std_bases;
    std::vector<unsigned> std_deviations;
    std::vector<size_t> std_base_indexes;
    gdd_lsb::rt::encode<T>(data, std_bases, std_deviations, std_base_indexes, dev_bits);

    //cout << "GdSegmentV1 ctor, bases: " + to_string(std_bases.size()) << ", deviation size: " << (int)dev_bits << " bits.\n";

    // Insert null value markers to base indexes
    if(!null_values.empty()){
        // NULLs are represented by the largest base index + 1
        const size_t null_value_base_index = std_bases.size();

        for(auto i=0u ; i<null_values.size() ; ++i) {
            if(null_values[i]){
                // There is a NULL at position 'i'
                std_base_indexes.insert(std_base_indexes.begin() + i, null_value_base_index);
                // Deviations must have the same size as base indexes, therefore we have to
                // add a dummy value (zero) as the deviations of NULLs
                std_deviations.insert(std_deviations.begin() + i, 0u);
                // Mark the instance flag for nulls
                nulls = true;
            }
        }
        // Make sure we added all NULLs
        DebugAssert(std_base_indexes.size() == null_values.size(), "Reconstruction list missing NULLs!");
        DebugAssert(std_deviations.size() == null_values.size(), "Reconstruction list missing NULLs!");
    }
    

    // Returns the min number of bits required to represent the given unsigned 
    auto num_bits_unsigned = [&](const size_t& value) -> size_t {
        return std::numeric_limits<size_t>::digits - std::countl_zero(value);
    };


    /**
     * Make compact vectors
     */ 

    // Bases: determine the number of bits from the values (bases are signed ints!)
    const auto bases_bits_num = helpers::int_vec_num_bits<T>(std_bases);
    auto bases_cv = compact::vector<T>(bases_bits_num, std_bases.size());
    for(auto i=0U ; i<std_bases.size() ; ++i){
        bases_cv[i] = std_bases[i];
    }
    bases_ptr = std::make_shared<decltype(bases_cv)>(bases_cv);

    // Use fixed 'dev_bits' for the deviations
    // @TODO use the min bits instead?
    auto deviations_cv = compact::vector<unsigned>(dev_bits, std_deviations.size());
    for(auto i=0U ; i<std_deviations.size() ; ++i){
        deviations_cv[i] = std_deviations[i];
    }
    deviations_ptr = std::make_shared<decltype(deviations_cv)>(deviations_cv);

    // The largest value in base indexes is either:
    //  std_bases.size()-1 if there are no NULLs, or
    //  std_bases.size() in case there are any
    const auto max_base_index = *std::max_element(std_base_indexes.begin(), std_base_indexes.end());
    //std::cout << "Max base index: " << max_base_index << endl;
    const auto recon_list_bits = (max_base_index == 0) ? 1 : num_bits_unsigned(max_base_index);
    auto recon_list_cv = compact::vector<size_t>(recon_list_bits, std_base_indexes.size());
    for(auto i=0U ; i<std_base_indexes.size() ; ++i){
        recon_list_cv[i] = std_base_indexes[i];
    }
    reconstruction_list = make_shared<decltype(recon_list_cv)>(recon_list_cv);
}


template <typename T, typename U>
void GdSegmentV1<T, U>::segment_vs_value_table_scan(
      const PredicateCondition& condition, 
      const AllTypeVariant& query_value_atv, 
      const ChunkID chunk_id, 
      RowIDPosList& results,
      const std::shared_ptr<const AbstractPosList>& position_filter) const
{
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
    const T query_base = gdd_lsb::rt::make_base<T>(query_value, dev_bits);
    const unsigned query_deviation = gdd_lsb::rt::make_deviation<unsigned>(query_value, dev_bits);

    const auto lower_it = std::lower_bound(bases_ptr->cbegin(), bases_ptr->cend(), query_base);

    const bool is_query_base_present = std::distance(bases_ptr->cbegin(), lower_it) <= bases_ptr->size() && (*lower_it == query_base);
    //const bool is_query_base_present = (lower_it != bases_ptr->cend()) && (*lower_it == query_base);
    const size_t query_value_base_idx = std::distance(bases_ptr->cbegin(), lower_it);

    // Allocate results eagerly
    results.reserve(results.size() + (position_filter ? position_filter->size() : size()));

    switch(condition) {
        case PredicateCondition::Equals:
        {
            if(is_query_base_present) {
                // Add values where both the base and the deviation equals
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx == query_value_base_idx) && (deviations_ptr->at(rowidx) == query_deviation);
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
                if(nulls){
                    // Filter out NULLs
                    const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                        return (base_idx != query_value_base_idx && base_idx != null_value_id()) || (deviations_ptr->at(rowidx) != query_deviation);
                    };
                    _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                }
                else{
                    const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                        return (base_idx != query_value_base_idx) || (deviations_ptr->at(rowidx) != query_deviation);
                    };
                    _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                }

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
                
                if(nulls){
                    // filter out nulls
                    const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                        return (base_idx > query_value_base_idx && base_idx != null_value_id()) || 
                            (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThan && deviations_ptr->at(rowidx) > query_deviation) ||
                            (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThanEquals && deviations_ptr->at(rowidx) >= query_deviation);
                    };
                    _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                    
                }
                else{
                    const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                        return (base_idx > query_value_base_idx) || 
                            (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThan && deviations_ptr->at(rowidx) > query_deviation) ||
                            (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThanEquals && deviations_ptr->at(rowidx) >= query_deviation);
                    };
                    _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                }
                
            }
            else {
                if(nulls){
                    // Filter out nulls
                    const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                        return (base_idx >= query_value_base_idx && base_idx != null_value_id());
                    };
                    _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                }
                else{
                    const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                        return (base_idx >= query_value_base_idx);
                    };
                    _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                }
            }
            break;
        }

        case PredicateCondition::LessThan:
        case PredicateCondition::LessThanEquals:
        {
            if(is_query_base_present) {
                // Add values of the query base where deviation is less or lessequal than the query deviation
                // Add values of lower bases
                // (no need to care about NULLs, since the null base index would never qualify here)
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx < query_value_base_idx) || 
                        (base_idx == query_value_base_idx && condition == PredicateCondition::LessThan && deviations_ptr->at(rowidx) < query_deviation) ||
                        (base_idx == query_value_base_idx && condition == PredicateCondition::LessThanEquals && deviations_ptr->at(rowidx) <= query_deviation);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
                return;
            }
            // Query base not present, add lower bases
            const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                return (base_idx < query_value_base_idx);
            };
            _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);
            return;
        }

        default: 
            std::cout << "Unexpected predicate in GD TableScan: " << condition << std::endl;
            throw new std::runtime_error("Unexpected predicate in GD TableScan");
    }
}


template <typename T, typename U>
float GdSegmentV1<T, U>::get_compression_gain() const {
    const float orig_data_size = sizeof(T) * reconstruction_list->size();
    const auto compressed_data = memory_usage(MemoryUsageCalculationMode::Full);
    return 1 - (compressed_data / orig_data_size);
}

template <typename T, typename U>
void GdSegmentV1<T, U>::print() const {
    cout << "Idx\tBase\tDev\tVal\n";
    for(auto i=0U ; i<reconstruction_list->size() ; ++i)  {
        const auto base_idx = reconstruction_list->at(i);
        const auto base = bases_ptr->at(base_idx);
        const auto dev = deviations_ptr->at(i);
        cout << "["<<i<<"]\t" << base << "\t" << dev << "\t" << gdd_lsb::rt::reconstruct_value<T>(base, dev, dev_bits) << '\n';
    }
}

template <typename T, typename U>
template <typename Functor>
void GdSegmentV1<T, U>::_add_matches(
    const ChunkID chunk_id, 
    RowIDPosList& results,
    const std::shared_ptr<const AbstractPosList>& position_filter,
    Functor base_idx_qualifies) const 
{
    const auto recon_list = *reconstruction_list;
    if(position_filter) {
        // Check only the position filter
        ChunkOffset pf_idx{0};
        size_t rowidx, base_idx;
        #pragma omp simd
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
        #pragma omp simd
        for(auto rowidx=ChunkOffset{0} ; rowidx<recon_list.size() ; ++rowidx){
            base_idx = recon_list[rowidx];
            if (base_idx_qualifies(base_idx, rowidx)) {
                results.push_back(RowID{chunk_id, rowidx});
            }
        }
    }
}

template <typename T, typename U>
void GdSegmentV1<T, U>::_all_to_matches(
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
        if(nulls) {
            // Filter out nulls
            const auto null_value_base_idx = null_value_id();
            #pragma omp simd
            for(auto i=ChunkOffset{0} ; i<position_filter->size() ; ++i){
                if(!isnull(i)){
                    results.push_back(RowID{chunk_id, i});
                }
            }

        }
        else{
            // Add all PF indexes
            #pragma omp simd
            for(auto i=ChunkOffset{0} ; i<position_filter->size() ; ++i){
                results.push_back(RowID{chunk_id, i});
            }
        }
    }
    else {
        // No position filter

        if(!nulls){
            // Add all row indexes
            #pragma omp simd
            for(auto rowidx=ChunkOffset{0} ; rowidx < size() ; ++rowidx){
                results.push_back(RowID{chunk_id, rowidx});
            }
        }
        else {
            // There are nulls, filter them out
            const auto null_value_base_idx = null_value_id();
            #pragma omp simd
            for(auto rowidx=ChunkOffset{0} ; rowidx < reconstruction_list->size() ; ++rowidx){
                const auto base_idx = reconstruction_list->at(rowidx);
                if(base_idx != null_value_base_idx){
                    results.push_back(RowID{chunk_id, rowidx});
                }
                ++rowidx;
            }
        }
    }
}


template <typename T, typename U>
ValueID GdSegmentV1<T, U>::null_value_id() const { 
    // NULLs are represented by the max base index + 1
    return static_cast<ValueID>(bases_ptr->size()); 
}

template <typename T, typename U>
bool GdSegmentV1<T, U>::isnull(const ChunkOffset& chunk_offset) const { 
    // NULL when the base index is the null value ID (max base index + 1)
    return reconstruction_list->at(chunk_offset) == bases_ptr->size(); 
}

template <typename T, typename U>
size_t GdSegmentV1<T, U>::memory_usage(const MemoryUsageCalculationMode mode) const {
    const auto bases_size = bases_ptr->bytes();
    const auto deviations_size = deviations_ptr->bytes();
    const auto reconstruction_list_size = reconstruction_list->bytes();
    return bases_size + deviations_size + reconstruction_list_size + 8; 
}

template class GdSegmentV1<int32_t>;

}