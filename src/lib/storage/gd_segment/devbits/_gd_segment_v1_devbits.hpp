#pragma once

#include <memory>
#include <vector>
#include <compact_vector.hpp>
#include "storage/base_gd_segment.hpp"
#include "gdd_lsb.hpp"
#include "helpers.hpp"


namespace gdsegment {

using namespace std;
using namespace opossum;

// Fwd declare the v1 generator
template<typename T>
shared_ptr<BaseGdSegment> make_gd_segment_v1(const vector<T>& data, const unsigned& dev_bits);


template<typename T, unsigned DevBits>
struct GdSegmentV1DevBits : public BaseGdSegment {

    T segment_min, segment_max;
    compact::vector<T, 32-DevBits> bases;
    compact::vector<unsigned, DevBits> deviations;
    shared_ptr<compact::vector<size_t>> reconstruction_list;

    GdSegmentV1DevBits(const vector<T>& data);

    // TableScan
    void segment_vs_value_table_scan(
        const PredicateCondition& condition, 
        const AllTypeVariant& query_value_atv, 
        const ChunkID chunk_id, 
        RowIDPosList& results,
        const std::shared_ptr<const AbstractPosList>& position_filter) const;
    

    template<typename Functor>
    inline void _add_matches(
        const ChunkID chunk_id, 
        RowIDPosList& results,
        const std::shared_ptr<const AbstractPosList>& position_filter,
        Functor base_idx_qualifies) const;
    

    // Add all row indexes to the results
    inline void _all_to_matches(
        const ChunkID& chunk_id, 
        RowIDPosList& results, 
        const std::shared_ptr<const AbstractPosList>& position_filter,
        const bool results_preallocated=false) const;

    // Decompress the whole segment
    void decompress(vector<T>& data) const;

    ChunkOffset size() const;
    size_t memory_usage(const MemoryUsageCalculationMode mode) const;

    std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
        // performance critical - not in cpp to help with inlining
        const auto base_idx = reconstruction_list->at(chunk_offset);
        return gdd_lsb::ct::reconstruct_value<T, DevBits>(bases[base_idx], deviations[chunk_offset]);
    }
    AllTypeVariant operator[](const ChunkOffset chunk_offset) const {
        PerformanceWarning("operator[] used");
        DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
        
        const auto typed_value = get_typed_value(chunk_offset);
        return *typed_value;
    }

    // Null values are not supported
    bool isnull(const ChunkOffset& chunk_offset) const { return false; }
    ValueID null_value_id() const {
        // Last valid base index + 1
        // (actually, null values are not supported. If they were, this value would represent them)
        return ValueID{static_cast<ValueID::base_type>(bases.size())};
    }

    constexpr unsigned get_dev_bits() const noexcept { return DevBits; }
    float get_compression_gain() const;

    std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;
    EncodingType encoding_type() const { return EncodingType::GdV1; };
    std::string type() const { return "GdSegmentV1DevBits"; }
};

template<typename T>
shared_ptr<BaseGdSegment> make_gd_segment_v1(const vector<T>& data, const unsigned& dev_bits){
    switch(dev_bits) {
        case 1: return make_shared<GdSegmentV1DevBits<T, 1>>(data);
        case 2: return make_shared<GdSegmentV1DevBits<T, 2>>(data);
        case 3: return make_shared<GdSegmentV1DevBits<T, 3>>(data);
        
        case 4: return make_shared<GdSegmentV1DevBits<T, 4>>(data);
        case 5: return make_shared<GdSegmentV1DevBits<T, 5>>(data);
        case 6: return make_shared<GdSegmentV1DevBits<T, 6>>(data);
        case 7: return make_shared<GdSegmentV1DevBits<T, 7>>(data);
        case 8: return make_shared<GdSegmentV1DevBits<T, 8>>(data);
        case 9: return make_shared<GdSegmentV1DevBits<T, 9>>(data);
        case 10: return make_shared<GdSegmentV1DevBits<T, 10>>(data);

        case 11: return make_shared<GdSegmentV1DevBits<T, 11>>(data);
        case 12: return make_shared<GdSegmentV1DevBits<T, 12>>(data);
        case 13: return make_shared<GdSegmentV1DevBits<T, 13>>(data);
        case 14: return make_shared<GdSegmentV1DevBits<T, 14>>(data);
        case 15: return make_shared<GdSegmentV1DevBits<T, 15>>(data);
        case 16: return make_shared<GdSegmentV1DevBits<T, 16>>(data);
        case 17: return make_shared<GdSegmentV1DevBits<T, 17>>(data);
        case 18: return make_shared<GdSegmentV1DevBits<T, 18>>(data);
        case 19: return make_shared<GdSegmentV1DevBits<T, 19>>(data);
        case 20: return make_shared<GdSegmentV1DevBits<T, 20>>(data);

        case 21: return make_shared<GdSegmentV1DevBits<T, 21>>(data);
        case 22: return make_shared<GdSegmentV1DevBits<T, 22>>(data);
        case 23: return make_shared<GdSegmentV1DevBits<T, 23>>(data);
        case 24: return make_shared<GdSegmentV1DevBits<T, 24>>(data);
        case 25: return make_shared<GdSegmentV1DevBits<T, 25>>(data);
        case 26: return make_shared<GdSegmentV1DevBits<T, 26>>(data);
        case 27: return make_shared<GdSegmentV1DevBits<T, 27>>(data);
        case 28: return make_shared<GdSegmentV1DevBits<T, 28>>(data);
        case 29: return make_shared<GdSegmentV1DevBits<T, 29>>(data);
        case 30: return make_shared<GdSegmentV1DevBits<T, 30>>(data);
        /*
        */
        default: throw out_of_range("Deviation bits out of range");
    }
}
} // namespace gdsegment