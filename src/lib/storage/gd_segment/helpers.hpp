#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <random>
#include <bit>

namespace gd_helpers {

    using namespace std;

    // Convert a float to an unsigned
    uint32_t ftou(float f){
        uint32_t u = *reinterpret_cast<uint32_t*>(&f);
        return u;
    }

    uint32_t ftou_scale(float f, size_t frac_digits){
        // Scale up and convert
        return (uint32_t) (f * pow(10, frac_digits));
    }

    // Convert a double to an unsigned long
    uint64_t dtoul(double d){
        return *reinterpret_cast<uint64_t*>(&d);
    }

    template<typename T>
    float avg(const vector<T>& vec){
        const auto sum = std::accumulate(vec.begin(), vec.end(), 0.0);
        return sum/(float)vec.size();
    }

    float percent_of(unsigned a, unsigned b){
        return 100 * (a/(float)b);
    }

    // Returns how much val_2 is larger than val_1 
    // (may be negative if val_2 is smaller)
    int diff_percent(const int& val_1, const int& val_2) { 
        return round(100 * ((val_1 - val_2) / (float)val_1)); 
    };


    template <typename T> 
    T quantile(const vector<T>& x, const float q) {
        assert(q >= 0.0 && q <= 1.0);

        vector<T> sorted(x);
        std::sort(sorted.begin(), sorted.end());
        
        const auto n  = sorted.size();
        const auto id = (n - 1) * q;
        const auto lo = floor(id);
        const auto hi = ceil(id);
        const auto qs = sorted[lo];
        const auto h  = (id - lo);
        
        return (1.0 - h) * qs + h * sorted[hi];
    }

    template<typename T>
    vector<T> filter_outliers(const vector<T>& arr, const float threshold_percent) {
        const auto min = quantile(arr, threshold_percent/100.0);
        const auto max = quantile(arr, ((100 - threshold_percent) /100.0));

        vector<T> filtered;
        filtered.reserve(arr.size());
        for(const auto& el : arr) {
            if(el >= min && el <= max){
                filtered.push_back(el);
            }
        }

        return filtered;
    };

    // Returns the minimum number of bits needed to represent every element
    // in a signed int array
    uint8_t int_vec_num_bits(const vector<int>& data) {
        uint8_t max_bits = 0;
        uint8_t curr_bits;

        const auto base = sizeof(unsigned) * 8;
        for(const auto& d : data) {
            curr_bits = base - __countl_zero((unsigned) abs(d)) ;
            if(curr_bits > max_bits){
                max_bits = curr_bits;
            }
        }
        // Add an extra bit for the sign
        return max_bits + 1;
    }

    template <typename T>
    string join(const T& v, const string& delim) {
        ostringstream s;
        for (const auto& i : v) {
            if (&i != &v[0]) {
                s << delim;
            }
            s << i;
        }
        return s.str();
    }

    std::vector<std::string> split(std::string text, char delim=',') {
        std::string line;
        std::vector<std::string> vec;
        std::stringstream ss(text);
        while(std::getline(ss, line, delim)) {
            vec.push_back(line);
        }
        return vec;
    }

    template<typename T>
    void transpose(std::vector<std::vector<T>> &b)
    {
        if (b.size() == 0)
            return;

        std::vector<std::vector<T>> trans_vec(b[0].size(), vector<T>());

        for (int i = 0; i < b.size(); i++) {
            for (int j = 0; j < b[i].size(); j++) {
                trans_vec[j].push_back(b[i][j]);
            }
        }

        b = trans_vec;
    }

    template<typename T>
    size_t num_bits(T x){
        return std::numeric_limits<decltype(x)>::digits - __countl_zero(x);
    }

    // Determine the max number of fractional digits in a vector of floats,
    // e.g. {0.1, 1, 1.212} -> 3 because of the last one
    //      { 1, 2.1, 2 } -> 1
    size_t get_significant_frac_digits(const vector<float>& data){
        size_t max_digits = 0;
        
        size_t current_digits = 0;
        std::stringstream sstream;
        std::string str;
        for(const auto& d : data){
            // Convert to string
            sstream.str("");
            sstream << d;
            str = sstream.str();

            // Count characters from the back until the decimal point
            const auto dot_it = std::find(str.begin(), str.end(), '.');
            if(dot_it != str.end()){
                // There are some fractional digits
                current_digits = str.size() - (dot_it - str.begin()) - 1;

                // Update max_digits
                if(current_digits > max_digits){
                    max_digits = current_digits;
                }
            }
        }
        return max_digits;
    }

    vector<int> generate_int_data(size_t size, int min, int max){
        vector<int> data(size);

        std::random_device rd{};
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> generator(min, max);
        std::generate(data.begin(), data.end(), [&](){
            return generator(rng);
        });
        
        return data;
    }

    vector<float> generate_float_data(size_t size, float fmin = -999.99, float fmax = 999.99, unsigned decimals=2){
        vector<float> data(size);

        // Scale up the float range to int range and use an uniform int generator
        const auto scaler = pow(10, decimals);
        const int range_min = fmin * scaler;
        const int range_max = fmax * scaler;

        std::random_device rd{};
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> generator(range_min, range_max);
        std::generate(data.begin(), data.end(), [&](){
            return generator(rng) / (float) scaler;
        });
        
        return data;
    }

}
