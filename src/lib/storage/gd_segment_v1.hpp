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
  const uint8_t dev_bits;

  std::shared_ptr<const compact::vector<T>> bases_ptr;
  std::shared_ptr<const compact::vector<unsigned>> deviations_ptr;
  std::shared_ptr<const compact::vector<size_t>> reconstruction_list;

public:

    GdSegmentV1(const std::vector<T>& data, const uint8_t dev_bits);

    EncodingType encoding_type() const { return EncodingType::GdV1; };
    std::shared_ptr<const compact::vector<T>> get_bases() const { return bases_ptr; };
    std::shared_ptr<const compact::vector<unsigned>> get_deviations() const { return deviations_ptr; };
    std::shared_ptr<const compact::vector<size_t>> get_reconstruction_list() const { return reconstruction_list; };
    unsigned get_dev_bits() const { return dev_bits; }
    
    float get_compression_gain() const;
    void print() const;


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
        Functor base_idx_qualifies) const
    {
        const auto recon_list = *reconstruction_list;
        if(position_filter) {
            // Check only the position filter
            ChunkOffset pf_idx{0};
            size_t rowidx, base_idx;
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
    T get(const ChunkOffset& rowidx) const {
        const auto base_idx = reconstruction_list->at(rowidx);
        return gdd_lsb::rt::reconstruct_value<T>(bases_ptr->at(base_idx), deviations_ptr->at(rowidx), dev_bits);
    }

    // Decompress the whole segment
    void decompress(std::vector<T>& data) const {
        const auto recon_list = *reconstruction_list;
        data.resize(recon_list.size());
        for(auto rowidx=0U ; rowidx < recon_list.size() ; ++rowidx) {
            data[rowidx] = gdd_lsb::rt::reconstruct_value<T>(bases_ptr->at(recon_list[rowidx]), deviations_ptr->at(rowidx), dev_bits);
        }
    }

    ChunkOffset size() const {
        return static_cast<ChunkOffset>(reconstruction_list->size());
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

    bool isnull(const ChunkOffset& chunk_offset) const { return false; }

    std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
        std::vector<T> data;
        decompress(data);
        return std::make_shared<GdSegmentV1<T>>(data, dev_bits);
    }
};

//EXPLICITLY_DECLARE_DATA_TYPES(GddSegmentV1Fixed);
extern template class GdSegmentV1<int32_t>;

} // namespace opossum