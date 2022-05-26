#pragma once
#include <iostream>
#include <string>
#include <cmath>

namespace gdsegment
{

    using namespace std;

    namespace helpers
    {
        // Returns how much val_2 is larger than val_1 
        // (may be negative if val_2 is smaller)
        int diff_percent(int val_1, int val_2) { 
            return round(100 * ((val_1 - val_2) / (float)val_1)); 
        };
    }

    // Record the speed and compression rate of a GdSegment at a specific deviation size
    struct SegmentPerformance {
        unsigned dev_bits;
        float compression_gain;

        unsigned long random_access_time;
        unsigned long sequential_access_time;
        unsigned long table_scan_time;
        unsigned long table_scan_time_valuesegment;

        void print() const {
            cout << dev_bits << " bits\t | " << round(compression_gain*100) << "%\t | "<< random_access_time << " ns\t | " << sequential_access_time << " ns\t | " << (table_scan_time/1000) << " µs";

            if(table_scan_time_valuesegment) {
                cout << " (VS:" ;
                const int diff = helpers::diff_percent(table_scan_time, table_scan_time_valuesegment);
                const bool is_positive = diff > 0;
                if(is_positive) { cout << " \e[92m+"; } 
                else { cout << " \e[91m"; }
                cout << diff << "%\e[0m)";
            }
            cout << '\n';
        }

        std::string to_json_obj() const {
            string json("{");
            json += "\"dev_bits\":"+to_string(dev_bits)+",";
            json += "\"compression_gain\":"+to_string(compression_gain)+",";
            json += "\"random_access_time\":"+to_string(random_access_time)+",";
            json += "\"table_scan_time\":"+to_string(table_scan_time)+",";
            json += "\"table_scan_time_valuesegment\":"+to_string(table_scan_time_valuesegment)+"";
            json += "}";
            return json;
        }
    };
}