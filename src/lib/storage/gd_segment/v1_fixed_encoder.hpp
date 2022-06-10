#pragma once

#include "storage/base_segment_encoder.hpp"
#include "storage/gd_segment_v1_fixed.hpp" 
#include "types.hpp"
#include "utils/enum_constant.hpp"
#include "performance_test.hpp"
#include "config_parser.hpp"
#include "result_writer.hpp"
#include "segment_performance.hpp"

#include <memory>
#include <algorithm>
#include <iostream>

using namespace opossum;
using namespace gdsegment;

class GdV1FixedEncoder : public SegmentEncoder<GdV1FixedEncoder> {

public:

    static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::GdV1Fixed>;
    static constexpr auto _uses_vector_compression = false;  // see base_segment_encoder.hpp for details
    
    template <typename T>
    std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator,
                                                     const std::string& table_col_name, 
                                                     const int chunk_index)
    {
        // Extract the values from the segment iterable
        std::vector<T> values;
        // Bitmap of NULLs
        std::vector<bool> null_values;

        segment_iterable.with_iterators([&](auto segment_it, auto segment_end) {
            const auto segment_size = std::distance(segment_it, segment_end);
            values.reserve(segment_size);
            null_values.resize(segment_size, false); // fill null_values with false

            for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
                const auto segment_item = *segment_it;
                if (!segment_item.is_null()) {
                    const auto segment_value = segment_item.value();
                    values.push_back(segment_value);
                }
                else{
                    // mark NULL
                    null_values[current_position] = true;
                }
            }
        });

        // Create the segment
        return std::make_shared<GdSegmentV1Fixed<T>>(values, null_values);
    }
};