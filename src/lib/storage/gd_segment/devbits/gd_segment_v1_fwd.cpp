#include <iostream>
#include <memory>
#include "gd_segment_v1_fwd.hpp"

using namespace opossum;
using namespace gdsegment;

template <typename T, typename U>
GdSegmentV1<T, U>::GdSegmentV1(const std::shared_ptr<GdSegmentV1DevBits> segment_ptr) : 
    BaseGdSegment(data_type_from_type<T>()), 
    concrete_segment(segment_ptr) 
{}


template <typename T, typename U>
void GdSegmentV1<T, U>::segment_vs_value_table_scan(
      const PredicateCondition& condition, 
      const AllTypeVariant& query_value, 
      const ChunkID chunk_id, 
      RowIDPosList& matches,
      const std::shared_ptr<const AbstractPosList>& position_filter) const
{
    concrete_segment->segment_vs_value_table_scan(condition, query_value, chunk_id, matches, position_filter);
};