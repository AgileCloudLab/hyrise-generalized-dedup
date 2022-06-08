#pragma once

#include <vector>
#include <type_traits>
#include "permute.hpp"

#include <iostream>

namespace gdd_lsb
{
    using namespace std;

    // Deviation size given in runtime as a function parameter
    namespace rt {

        template<typename T> 
        static inline T make_base(const T& value, const unsigned& dev_bits){
            return value >> dev_bits;
        }

        template<typename T> 
        static inline T make_deviation(const T& value, const unsigned& dev_bits){
            return value & ((1 << dev_bits) - 1);
        }

        template<typename T> 
        static inline T reconstruct_value(const T& base, const unsigned& deviation, const unsigned& dev_bits){
            return (base << dev_bits) | deviation;
        }

         // Encode with runtime deviation size
        template<typename T> 
        static void encode(const std::vector<T>& data, 
                            std::vector<T>& bases_out, 
                            std::vector<unsigned>& deviations_out,
                            std::vector<size_t>& base_indexes_out,
                            const unsigned& dev_bits)
        {
            {   // Fill deviations std vector
                deviations_out.clear();
                deviations_out.resize(data.size());
                size_t dev_idx = 0;
                const unsigned shift_offset = (sizeof(T)*8) - dev_bits;
                for(const auto& d : data){
                    // Add only the last DEV_BITS bits of the deviation
                    // (shifting left then right clears all bits before the last DEV_BITS)
                    unsigned dev = d << shift_offset;
                    dev = dev >> shift_offset;
                    deviations_out[dev_idx++] = dev;
                }
            }

            
            {   // Fill bases vector, shift out deviation bits
                bases_out.clear();
                bases_out.resize(data.size());
                size_t base_idx = 0;
                for(const auto& d : data){
                    bases_out[base_idx++] = make_base<T>(d, dev_bits);
                }
            }
            
            
            {   // Remove duplicate bases and sort
                std::sort(bases_out.begin(), bases_out.end());
                bases_out.erase(std::unique(bases_out.begin(), bases_out.end()), bases_out.cend());
                bases_out.shrink_to_fit();
            }

            base_indexes_out.clear();
            base_indexes_out.resize(data.size());
            {   // Fill base indexes
                T base;
                size_t base_index_offset = 0;
                for(const auto& el : data){
                    // Generate the base of this data element
                    base = make_base<T>(el, dev_bits);

                    // Find base in bases_out and record its index 
                    base_indexes_out[base_index_offset++] = std::distance(
                        bases_out.cbegin(), 
                        std::lower_bound(bases_out.cbegin(), bases_out.cend(), base)
                    );
                }
            }
        }



    }

    // Deviation size given in compile time as a template parameter
    namespace ct {

        template<typename T, unsigned DEV_BITS> 
        static inline T make_base(const T& value){
            return value >> DEV_BITS;
        }

        template<typename T, unsigned DEV_BITS> 
        static inline T make_deviation(const T& value){
            return value & ((1 << DEV_BITS) - 1);
        }


        template<typename T, unsigned DEV_BITS> 
        static inline T reconstruct_value(const T& base, const unsigned& deviation){
            return (base << DEV_BITS) | deviation;
        }

        // Encode with compile-time deviation size
        template<typename T, unsigned DEV_BITS> 
        static void encode(const std::vector<T>& data, 
                            std::vector<T>& bases_out, 
                            std::vector<unsigned>& deviations_out,
                            std::vector<size_t>& base_indexes_out)
        {
            {   // Fill deviations std vector
                deviations_out.clear();
                deviations_out.resize(data.size());
                size_t dev_idx = 0;
                auto dev = 0ULL;
                const uint8_t shift_offset = (sizeof(T)*8) - DEV_BITS;

                if(shift_offset >= (sizeof(T)*8) ){
                    std::cout << "Too many deviation bits! Type width: " << (sizeof(T)*8) << ", dev bits: " << shift_offset << std::endl;
                    throw new std::runtime_error("Too many deviation bits");
                }
                else{
                    for(const auto& d : data){
                        // Add only the last DEV_BITS bits of the deviation
                        // (shifting left then right clears all bits before the last DEV_BITS)
                        dev = d << shift_offset;
                        dev = dev >> shift_offset;
                        deviations_out[dev_idx++] = dev;
                    }
                }
            }

            
            {   // Fill bases vector, shift out deviation bits
                bases_out.clear();
                bases_out.resize(data.size());
                size_t base_idx = 0;
                for(const auto& d : data){
                    bases_out[base_idx++] = make_base<T, DEV_BITS>(d);
                }
            }
            
            
            {   // Remove duplicate bases and sort
                std::sort(bases_out.begin(), bases_out.end());
                bases_out.erase(std::unique(bases_out.begin(), bases_out.end()), bases_out.cend());
                bases_out.shrink_to_fit();
            }

            base_indexes_out.clear();
            base_indexes_out.resize(data.size());
            {   // Fill base indexes
                T base;
                size_t base_index_offset = 0;
                for(const auto& el : data){
                    // Generate the base of this data element
                    base = make_base<T, DEV_BITS>(el);

                    // Find base in bases_out and record its index 
                    base_indexes_out[base_index_offset++] = std::distance(
                        bases_out.cbegin(), 
                        std::lower_bound(bases_out.cbegin(), bases_out.cend(), base)
                    );
                }
            }
        }

    
    }
}
