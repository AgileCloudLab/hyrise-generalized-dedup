#include "gd_segment_v1.hpp"
#include "base_gd_segment.hpp"
#include "gd_segment/gdd_lsb.hpp"

#include <bit>
#include <iostream>
#include <memory>
#include <vector>

namespace opossum {

template <typename T, typename U>
GdSegmentV1<T, U>::GdSegmentV1(const std::vector<T>& data, const uint8_t dev_bits) : 
    BaseGdSegment(data_type_from_type<T>()), 
    dev_bits(dev_bits),
    segment_min(*std::min_element(data.begin(), data.end())),
    segment_max(*std::max_element(data.begin(), data.end()))
{
    std::vector<T> std_bases;
    std::vector<unsigned> std_deviations;
    std::vector<size_t> std_base_indexes;
    gdd_lsb::rt::encode<T>(data, std_bases, std_deviations, std_base_indexes, dev_bits);

    auto num_bits = [](const size_t& value){
        return std::numeric_limits<decltype(value)>::digits - std::countl_zero(value);
    };

    // Make compact vectors
    const auto bases_bits = (std_bases.size() == 1) ? 1 : num_bits(std_bases.size());
    auto bases_cv = compact::vector<T>(bases_bits, std_bases.size());
    for(auto i=0 ; i<std_bases.size() ; ++i){
        bases_cv[i] = std_bases[i];
    }
    bases_ptr = std::make_shared<decltype(bases_cv)>(bases_cv);

    const auto devs_bits = (std_deviations.size() == 1) ? 1 : num_bits(std_deviations.size());
    auto deviations_cv = compact::vector<unsigned>(devs_bits, std_deviations.size());
    for(auto i=0 ; i<std_deviations.size() ; ++i){
        deviations_cv[i] = std_deviations[i];
    }
    deviations_ptr = std::make_shared<decltype(deviations_cv)>(deviations_cv);

    const auto recon_list_bits = (std_bases.size() == 1) ? 1 : num_bits(std_bases.size());
    auto recon_list_cv = compact::vector<size_t>(recon_list_bits, std_base_indexes.size());
    for(auto i=0 ; i<std_base_indexes.size() ; ++i){
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
                const auto base_idx_qualifies = [&](const size_t& base_idx, const size_t& rowidx) -> bool {
                    return (base_idx != query_value_base_idx) || (deviations_ptr->at(rowidx) != query_deviation);
                };
                _add_matches<decltype(base_idx_qualifies)>(chunk_id, results, position_filter, base_idx_qualifies);

            }
            else {
                // Else: all rowindexes are match
                _all_to_matches<true>(chunk_id, results, position_filter);
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
                        (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThan && deviations_ptr->at(rowidx) > query_deviation) ||
                        (base_idx == query_value_base_idx && condition == PredicateCondition::GreaterThanEquals && deviations_ptr->at(rowidx) >= query_deviation);
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
                        (base_idx == query_value_base_idx && condition == PredicateCondition::LessThan && deviations_ptr->at(rowidx) < query_deviation) ||
                        (base_idx == query_value_base_idx && condition == PredicateCondition::LessThanEquals && deviations_ptr->at(rowidx) <= query_deviation);
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



template class GdSegmentV1<int32_t>;

}