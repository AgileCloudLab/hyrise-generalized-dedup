#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <cmath>

namespace config_parser
{
    using namespace std;

    namespace helpers {

        std::vector<std::string> split(std::string text, char delim='=') {
            std::string line;
            std::vector<std::string> vec;
            std::stringstream ss(text);
            while(std::getline(ss, line, delim)) {
                vec.push_back(line);
            }
            return vec;
        }
    }

    struct GdSelectionConfig {
        // When set to >0, this deviation size should be used without any perf measurements
        unsigned fixed_dev_bits=0U;

        // Deviation size range for the measurements
        unsigned min_dev_bits = 1;
        unsigned max_dev_bits = 0; // 0 = based on the segment data type

        // Relative importance of compression rate and speeds of the segment
        float weight_comprate = 0.0;
        float weight_random_access = 0.0;
        float weight_sequential_access = 0.0;
        float weight_tablescan = 0.0;

        // Length of random access and tablescan tests, in a fraction of the segment size
        float random_access_rows_fraction = 0.1;
        float tablescan_values_fraction = 0.1;

        bool weights_ok() const { return weight_comprate + weight_random_access + weight_sequential_access + weight_tablescan == 1.0; }

        void print() const {
            if(!fixed_dev_bits){
                cout << "Fixed deviation size at " << fixed_dev_bits << " bits\n";
                return;
            }
            cout << "Devs: " << min_dev_bits << " -> " << (max_dev_bits==0 ? "auto" : std::to_string(max_dev_bits)) << ", weights: " 
                << round(weight_comprate*100) << "%, " << round(weight_random_access*100) << "%, "
                << round(weight_sequential_access*100) << "%, " << round(weight_tablescan*100) << "%\n";
            if(!weights_ok()){
                cout << "WEIGHTS ERROR!" << endl;
            }
        }
    };

    GdSelectionConfig read_config(const string& filepath="gd_segment_prefs.txt") {
        GdSelectionConfig conf;

        ifstream f(filepath);
        string line;
        while(std::getline(f, line)){
            if(line == "" || line[0] == '#'){
                continue;
            }
            const auto parts = helpers::split(line, '=');
            if(parts.size() != 2){
                continue;
            }

            const auto key = parts[0];
            const auto value = parts[1];

            if(key == "fixed_dev_bits"){ conf.fixed_dev_bits = stoi(value); }
            else if(key == "min_dev_bits"){ conf.min_dev_bits = stoi(value); }
            else if(key == "max_dev_bits"){ conf.max_dev_bits = stoi(value); }
            else if(key == "weight_comprate"){ conf.weight_comprate = stof(value); }
            else if(key == "weight_random_access"){ conf.weight_random_access = stof(value); }
            else if(key == "weight_sequential_access"){ conf.weight_sequential_access = stof(value); }
            else if(key == "weight_tablescan"){ conf.weight_tablescan = stof(value); }
            else if(key == "random_access_rows_fraction"){ conf.random_access_rows_fraction = stof(value); }
            else if(key == "tablescan_values_fraction"){ conf.tablescan_values_fraction = stof(value); }
            else{ cout << "Unexpected key in GD config file: " << key << endl; }
        }
        
        if(!conf.weights_ok()) {
            std::cout << "GD parameter weights do not add up to 1.0!" << std::endl;
            throw new std::runtime_error("GD parameter weights do not add up to 1.0!");
        }

        return conf;
    }



}