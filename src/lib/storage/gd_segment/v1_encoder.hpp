#pragma once

#include "storage/base_segment_encoder.hpp"
#include "types.hpp"
#include <memory>
#include "gd_segment_v1_devbits.hpp" 
#include "storage/gd_segment_v1.hpp" 

using namespace opossum;

class GdV1Encoder : public SegmentEncoder<GdV1Encoder>{

public:

    static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::GdV1>;
    static constexpr auto _uses_vector_compression = false;  // see base_segment_encoder.hpp for details
    
    template <typename T>
    std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator)
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

        /**
         * @TODO test GdSegmentV1 with all deviation sizes, 
         * @TODO decide which one is the best and return a shared pointer to it
         */  
        

        // Create the segment
        const auto segment_ptr = gdsegment::make_gd_segment_v1(values, 1U);
        segment_ptr->print();
        return std::make_shared<GdSegmentV1<T>>(segment_ptr);
    }
};