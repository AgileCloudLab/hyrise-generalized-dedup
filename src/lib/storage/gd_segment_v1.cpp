#include <iostream>
#include <memory>
#include "gd_segment_v1.hpp"

using namespace opossum;

template <typename T, typename U>
GdSegmentV1<T, U>::GdSegmentV1(const std::shared_ptr<BaseGdSegment> segment_ptr) : 
    BaseGdSegment(data_type_from_type<T>()), 
    concrete_segment(segment_ptr) 
{
    std::cout << "GdSegmentV1 created, underlying segment: "; 
    concrete_segment->print();
}


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