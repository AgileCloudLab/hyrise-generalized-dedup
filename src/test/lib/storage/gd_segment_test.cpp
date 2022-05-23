#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/gd_segment_v1.hpp"
#include "storage/gd_segment/v1_encoder.hpp"

#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

class StorageGdSegmentV1Test : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<int>> vs_int = std::make_shared<ValueSegment<int>>(false);
};

template <typename T>
std::shared_ptr<GdSegmentV1<T>> compress(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = ChunkEncoder::encode_segment(segment, data_type, SegmentEncodingSpec{EncodingType::GdV1});
  return std::dynamic_pointer_cast<GdSegmentV1<T>>(encoded_segment);
}

TEST_F(StorageGdSegmentV1Test, ConstructSegment) {
  for(auto i=10U ; i<100 ; ++i){
    vs_int->append(i);
  }

  auto gd_int_segment = compress(vs_int, DataType::Int);

  EXPECT_EQ(gd_int_segment->size(), vs_int->size());
  
}

}