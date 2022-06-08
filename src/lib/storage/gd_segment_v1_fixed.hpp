#pragma once

#include "base_gd_segment.hpp"
#include "types.hpp"
#include <type_traits>
#include <memory>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "compact_vector.hpp"
#include "gd_segment/gdd_lsb.hpp"

namespace opossum {

/**
 * GD Segment V1 with fixed 1-byte deviations, 
 * using compile-time optimized compact vectors
 * 
 * Segment implementing GDD compression in the following way:
 * - Bases are deduplicated
 * - Deviations are not deduplicated
 * - Reconstruction list consists of base indexes
 */
template <typename T, typename = std::enable_if_t<encoding_supports_data_type(
                          enum_c<EncodingType, EncodingType::GdV1>, hana::type_c<T>)>>
class GdSegmentV1 : public BaseGdSegment {

private:
  const T segment_min, segment_max;
  constexpr uint8_t DEV_BITS = 8u;
  constexpr uint8_t BASE_BITS = sizeof(T) * 8 - DEV_BITS;

  std::shared_ptr<const compact::vector<BASE_BITS, T>> bases_ptr;
  std::shared_ptr<const compact::vector<DEV_BITS, unsigned>> deviations_ptr;
  std::shared_ptr<const compact::vector<size_t>> reconstruction_list;

public:

    GdSegmentV1(const std::vector<T>& data);

    EncodingType encoding_type() const { return EncodingType::GdV1; };
    std::shared_ptr<const compact::vector<BASE_BITS, T>> get_bases() const { return bases_ptr; };
    std::shared_ptr<const compact::vector<DEV_BITS, unsigned>> get_deviations() const { return deviations_ptr; };
    std::shared_ptr<const compact::vector<size_t>> get_reconstruction_list() const { return reconstruction_list; };
    constexpr unsigned get_dev_bits() const noexcept { return DEV_BITS; }
    

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
    
    // Get the value at position 
    T get(const ChunkOffset& rowidx) const;

    // Decompress the whole segment
    void decompress(std::vector<T>& data) const;

    size_t memory_usage(const MemoryUsageCalculationMode mode) const;

    float get_compression_gain() const;
    void print() const;

    size_t bases_num() const { return bases_ptr->size(); }
    ChunkOffset size() const { return static_cast<ChunkOffset>(reconstruction_list->size()); }
    ValueID null_value_id() const { return ValueID{0}; }
    bool isnull(const ChunkOffset& chunk_offset) const { return false; }

    AllTypeVariant operator[](const ChunkOffset chunk_offset) const {
        PerformanceWarning("operator[] used");
        DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
        
        const auto typed_value = get_typed_value(chunk_offset);
        if (!typed_value) {
            return NULL_VALUE;
        }
        return *typed_value;
    }

    std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
        //std::cout << "Gdd get_typed_value #" << chunk_offset << std::endl;
        // performance critical - not in cpp to help with inlining
        
        // Return nullopt if the value at chunk_offset is a NULL
        if(isnull(chunk_offset)) return std::nullopt;
        // Reconstruct the actual value otherwise
        return get(chunk_offset);
    }

    std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
        std::cout << "GD Segment copy called" << std::endl;
        throw new std::runtime_error("Unexpected segment copy");
        std::vector<T> data;
        decompress(data);
        return std::make_shared<GdSegmentV1<T>>(data, dev_bits);
    }
};

//EXPLICITLY_DECLARE_DATA_TYPES(GddSegmentV1Fixed);
extern template class GdSegmentV1<int32_t>;

} // namespace opossum