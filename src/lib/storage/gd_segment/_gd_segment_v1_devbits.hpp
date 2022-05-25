#pragma once

#include <memory>
#include <vector>
#include <compact_vector.hpp>
#include "storage/base_gd_segment.hpp"
#include "gdd_lsb.hpp"

namespace gdsegment {

using namespace std;
using namespace opossum;

// Fwd declare the v1 generator
shared_ptr<BaseGdSegment> make_gd_segment_v1(const vector<int>& data, const unsigned& dev_bits);

template<unsigned DevBits>
struct GdSegmentV1DevBits : public BaseGdSegment {

    const int segment_min, segment_max;
    shared_ptr<compact::vector<int>> bases_ptr;
    shared_ptr<compact::vector<unsigned>> deviations_ptr;
    shared_ptr<compact::vector<size_t>> reconstruction_list;

    GdSegmentV1DevBits(const vector<int>& data);

    void print() const;

    // TableScan
    void segment_vs_value_table_scan(
        const PredicateCondition& condition, 
        const AllTypeVariant& query_value_atv, 
        const ChunkID chunk_id, 
        RowIDPosList& results,
        const std::shared_ptr<const AbstractPosList>& position_filter) const 
    {

        const int query_value = boost::get<int>(query_value_atv);

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
        const int query_base = gdd_lsb::ct::make_base<int, DevBits>(query_value);
        const unsigned query_deviation = gdd_lsb::ct::make_deviation<unsigned, DevBits>(query_value);

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

    template<typename Functor>
    inline void _add_matches(
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

    // Add all row indexes to the results
    template<bool results_preallocated=false>
    inline void _all_to_matches(
        const ChunkID& chunk_id, 
        RowIDPosList& results, 
        const std::shared_ptr<const AbstractPosList>& position_filter) const 
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

    // Get the value at position 
    int get(const ChunkOffset& rowidx) const {
        const auto base_idx = reconstruction_list->at(rowidx);
        return gdd_lsb::ct::reconstruct_value<int, DevBits>(*bases_ptr[base_idx], deviations_ptr->at(rowidx));
    }

    // Decompress the whole segment
    void decompress(vector<int>& data) const {
        const auto recon_list = *reconstruction_list;
        data.resize(recon_list.size());
        for(auto rowidx=0U ; rowidx < recon_list.size() ; ++rowidx) {
            data[rowidx] = gdd_lsb::ct::reconstruct_value<int, DevBits>(*bases_ptr[recon_list[rowidx]], deviations_ptr->at(rowidx));
        }
    }

    ChunkOffset size() const {
        return ChunkOffset{ deviations_ptr->size() };
    }

    // Calculate total memory use
    size_t total_memory_use(size_t& bases_size, size_t& deviations_size, size_t& reconstruction_list_size) const {
        bases_size = bases_ptr->bytes();
        deviations_size = deviations_ptr->bytes();
        reconstruction_list_size = reconstruction_list->bytes();
        return bases_size + deviations_size + reconstruction_list_size + 8; 
    }

    size_t memory_usage(const MemoryUsageCalculationMode mode) const {
        size_t a,b,c;
        return total_memory_use(a,b,c);
    }

    size_t bases_num() const {
        return bases_ptr->size();
    }


    ValueID null_value_id() const {
        // Last valid base index + 1
        return ValueID{static_cast<ValueID::base_type>(bases_ptr->size())};
    }

    EncodingType encoding_type() const { return EncodingType::GdV1; };

    AllTypeVariant operator[](const ChunkOffset chunk_offset) const {
        PerformanceWarning("operator[] used");
        DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
        
        const auto typed_value = get_typed_value(chunk_offset);
        if (!typed_value) {
            return NULL_VALUE;
        }
        return *typed_value;
    }

    std::optional<int> get_typed_value(const ChunkOffset chunk_offset) const {
        //std::cout << "Gdd get_typed_value #" << chunk_offset << std::endl;
        // performance critical - not in cpp to help with inlining
        
        // Return nullopt if the value at chunk_offset is a NULL
        if(isnull(chunk_offset)) return std::nullopt;
        // Reconstruct the actual value otherwise
        return get(chunk_offset);
    }

    bool isnull(const ChunkOffset& chunk_offset) const { return false; }

    std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
        vector<int> data;
        decompress(data);
        return make_shared<GdSegmentV1DevBits<DevBits>>(data);
    }

};


} // namespace gdsegment