
#include "gd_segment_v1_devbits.hpp"
#include "gdd_lsb.hpp"
#include <vector>

using namespace opossum;
using namespace std;
using namespace gdsegment;

template<unsigned DevBits>
GdSegmentV1DevBits<DevBits>::GdSegmentV1DevBits(const vector<int>& data) : BaseGdSegment(data_type_from_type<int32_t>())
{
    std::vector<int> std_bases;
    std::vector<unsigned> std_deviations;
    std::vector<size_t> std_base_indexes;
    gdd_lsb::ct::encode<int, DevBits>(data, std_bases, std_deviations, std_base_indexes);

    // Make compact vectors
    bases.resize(std_bases.size());
    for(auto i=0 ; i<bases.size() ; ++i){
        bases[i] = std_bases[i];
    }

    deviations.resize(std_deviations.size());
    for(auto i=0 ; i<deviations.size() ; ++i){
        deviations[i] = std_deviations[i];
    }

    const auto max_base_index = bases.size() - 1;
    const auto bits = (max_base_index == 0) ? 1 : gd_helpers::num_bits(max_base_index);
    compact::vector<size_t> recon_list(bits, std_base_indexes.size());
    for(auto i=0 ; i<std_base_indexes.size() ; ++i){
        recon_list[i] = std_base_indexes[i];
    }
    reconstruction_list = make_shared<decltype(recon_list)>(recon_list);

    // Fill min and max
    segment_min = *std::min_element(data.begin(), data.end());
    segment_max = *std::max_element(data.begin(), data.end());
}

template<unsigned DevBits>
void GdSegmentV1DevBits<DevBits>::print() const {
    cout << "Bases: " << bases.size() << endl;
    for(auto base_idx=0U ; base_idx<bases.size() ; ++base_idx){
        cout << "["<<base_idx<<"] " << bases[base_idx] << " -> [ ";
        
        for(auto rowidx=0 ; rowidx<reconstruction_list->size() ; ++rowidx){
            if(reconstruction_list->at(rowidx) == base_idx){
                cout << deviations[rowidx] << "("<<rowidx<<") ";
            }
        }
        cout << "]\n";
    }
    cout << endl;
}