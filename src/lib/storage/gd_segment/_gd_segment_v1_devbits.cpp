
#include "gd_segment_v1_devbits.hpp"
#include "gdd_lsb.hpp"
#include <vector>
#include <bit> // for std::countl_zero

namespace gdsegment {

using namespace opossum;
using namespace std;

template<unsigned DevBits>
GdSegmentV1DevBits<DevBits>::GdSegmentV1DevBits(const vector<int>& data) : BaseGdSegment(data_type_from_type<int32_t>())
{
    std::vector<int> std_bases;
    std::vector<unsigned> std_deviations;
    std::vector<size_t> std_base_indexes;
    gdd_lsb::ct::encode<int, DevBits>(data, std_bases, std_deviations, std_base_indexes);

    auto num_bits = [](const size_t& value){
        return std::numeric_limits<decltype(value)>::digits - std::countl_zero(value);
    };

    // Make compact vectors
    const auto bases_bits = (std_bases.size() == 1) ? 1 : num_bits(std_bases.size());
    auto bases_cv = compact::vector<int>(bases_bits, std_bases.size());
    for(auto i=0 ; i<std_bases.size() ; ++i){
        bases_cv[i] = std_bases[i];
    }
    bases_ptr = std::make_shared<decltype(bases_cv)>(bases_cv);

    const auto devs_bits = (std_deviations.size() == 1) ? 1 : num_bits(std_deviations.size());
    auto deviations_cv = compact::vector<int>(devs_bits, std_deviations.size());
    for(auto i=0 ; i<std_deviations.size() ; ++i){
        deviations_cv[i] = std_deviations[i];
    }
    deviations_ptr = std::make_shared<decltype(deviations_cv)>(deviations_cv);

    const auto recon_list_bits = (std_bases.size() == 1) ? 1 : num_bits(std_bases.size());
    auto recon_list_cv = size_t_compact_vector(recon_list_bits, std_base_indexes.size());
    for(auto i=0 ; i<std_base_indexes.size() ; ++i){
        recon_list_cv[i] = std_base_indexes[i];
    }
    reconstruction_list = make_shared<decltype(recon_list_cv)>(recon_list_cv);

    // Fill min and max
    segment_min = *std::min_element(data.begin(), data.end());
    segment_max = *std::max_element(data.begin(), data.end());
}

template<unsigned DevBits>
void GdSegmentV1DevBits<DevBits>::print() const {
    std::cout << "GdSegmentV1DevBits with "<< DevBits << " deviation bits" << std::endl;
    /*
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
    */
}

shared_ptr<BaseGdSegment> make_gd_segment_v1(const vector<int>& data, const unsigned& dev_bits){
    switch(dev_bits) {
        case 1: return make_shared<GdSegmentV1DevBits<1>>(data);
        case 2: return make_shared<GdSegmentV1DevBits<2>>(data);
        
        case 3: return make_shared<GdSegmentV1DevBits<3>>(data);
        case 4: return make_shared<GdSegmentV1DevBits<4>>(data);
        case 5: return make_shared<GdSegmentV1DevBits<5>>(data);
        case 6: return make_shared<GdSegmentV1DevBits<6>>(data);
        case 7: return make_shared<GdSegmentV1DevBits<7>>(data);
        case 8: return make_shared<GdSegmentV1DevBits<8>>(data);
        case 9: return make_shared<GdSegmentV1DevBits<9>>(data);
        case 10: return make_shared<GdSegmentV1DevBits<10>>(data);

        case 11: return make_shared<GdSegmentV1DevBits<11>>(data);
        case 12: return make_shared<GdSegmentV1DevBits<12>>(data);
        case 13: return make_shared<GdSegmentV1DevBits<13>>(data);
        case 14: return make_shared<GdSegmentV1DevBits<14>>(data);
        case 15: return make_shared<GdSegmentV1DevBits<15>>(data);
        case 16: return make_shared<GdSegmentV1DevBits<16>>(data);
        case 17: return make_shared<GdSegmentV1DevBits<17>>(data);
        case 18: return make_shared<GdSegmentV1DevBits<18>>(data);
        case 19: return make_shared<GdSegmentV1DevBits<19>>(data);
        case 20: return make_shared<GdSegmentV1DevBits<20>>(data);

        case 21: return make_shared<GdSegmentV1DevBits<21>>(data);
        case 22: return make_shared<GdSegmentV1DevBits<22>>(data);
        case 23: return make_shared<GdSegmentV1DevBits<23>>(data);
        case 24: return make_shared<GdSegmentV1DevBits<24>>(data);
        case 25: return make_shared<GdSegmentV1DevBits<25>>(data);
        case 26: return make_shared<GdSegmentV1DevBits<26>>(data);
        case 27: return make_shared<GdSegmentV1DevBits<27>>(data);
        case 28: return make_shared<GdSegmentV1DevBits<28>>(data);
        case 29: return make_shared<GdSegmentV1DevBits<29>>(data);
        case 30: return make_shared<GdSegmentV1DevBits<30>>(data);
        
        default: throw out_of_range("Deviation bits out of range");
    }
}

} // ns gdsegment