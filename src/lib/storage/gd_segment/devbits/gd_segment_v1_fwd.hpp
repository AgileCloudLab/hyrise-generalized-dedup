#pragma once

#include "base_gd_segment.hpp"
#include "storage/gd_segment/_gd_segment_v1_devbits.hpp"
#include "types.hpp"

#include "compact_vector.hpp"

namespace opossum {

using namespace gdsegment;

/**
 * GD Segment V1 FWD
 * Forward all calls to an underlying, GdSegmentV1DevBits<T, DevBits> instance
 * which uses compile-time optimized compact vectors
 * 
 * Segment implementing GDD compression in the following way:
 * - Bases are deduplicated
 * - Deviations are not deduplicated
 * - Reconstruction list consists of base indexes
 */
template <typename T, typename=std::enable_if_t<encoding_supports_data_type(enum_c<EncodingType, EncodingType::GdV1>, hana::type_c<T>)>>
class GdSegmentV1 : public BaseGdSegment {

private:
  const std::shared_ptr<BaseGdSegment> concrete_segment;

public:

    GdSegmentV1(const std::shared_ptr<BaseGdSegment> segment_ptr);

    // Forward all methods to the concrete segment
    EncodingType encoding_type() const { return concrete_segment->encoding_type(); };
    ValueID null_value_id() const { return concrete_segment->null_value_id(); };
    AllTypeVariant operator[](const ChunkOffset chunk_offset) const { return (*concrete_segment)[chunk_offset]; }
    ChunkOffset size() const { return concrete_segment->size(); }
    std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const { 
        constexpr auto dev_bits = concrete_segment->get_dev_bits();
        const auto typed_segment = dynamic_cast<GdSegmentV1DevBits<T, dev_bits>*>(&*concrete_segment);
        return typed_segment->get_typed_value(chunk_offset); 
    }
    std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const { return concrete_segment->copy_using_allocator(alloc); }
    size_t memory_usage(const MemoryUsageCalculationMode mode) const { return concrete_segment->memory_usage(mode); }

    float get_compression_gain() const { return concrete_segment->get_compression_gain(); }
    constexpr unsigned get_dev_bits() const noexcept { return concrete_segment->get_dev_bits(); }

    void segment_vs_value_table_scan(
      const PredicateCondition& condition, 
      const AllTypeVariant& query_value, 
      const ChunkID chunk_id, 
      RowIDPosList& matches,
      const std::shared_ptr<const AbstractPosList>& position_filter) const;


};

//EXPLICITLY_DECLARE_DATA_TYPES(GddSegmentV1Fixed);
extern template class GdSegmentV1<int32_t>;

} // namespace opossum