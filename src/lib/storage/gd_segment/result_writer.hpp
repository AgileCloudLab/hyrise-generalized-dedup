#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>

#include "performance_test.hpp"

namespace gdsegment
{
    using namespace std;
    using json = nlohmann::json;
    using namespace perf_test;

// Write to a single file from multiple threads
class GdResultWriter
{   
    protected:
        GdResultWriter(const std::string file_path): _file_path(file_path)
        {}
        static GdResultWriter* instance;

    public:
        GdResultWriter(GdResultWriter &other) = delete;
        void operator=(const GdResultWriter &) = delete;
        static GdResultWriter *getinstance(const std::string& path) {
            /**
             * This is a safer way to create an instance. instance = new Singleton is
             * dangeruous in case two instance threads wants to access at the same time
             */
            if(instance==nullptr){
                instance = new GdResultWriter(path);
            }
            return instance;
        }

        void write(const string& data) {
            // Ensure that only one thread can execute at a time
            std::lock_guard<std::mutex> lock(_writerMutex);
            auto f = ofstream(_file_path, ios::app);
            f << data;
            f.close();
        }

        void write_measurements(const vector<SegmentPerformance>& perf_results, const string& table_col_name, const int chunk_index, const size_t rows) {
            // Serialize the measurements
            string data;
            for(const auto& results_per_devbits : perf_results){
                data += "{\"name\":\""+table_col_name+"\", \"chunk_idx\": "+std::to_string(chunk_index)+", \"rows\":"+std::to_string(rows)+", \"results\": ";
                data += results_per_devbits.to_json_obj();
                data += "},\n";
            }
            write(data);
        }
    
    private:

        std::mutex _writerMutex;
        std::string _file_path;
};
GdResultWriter* GdResultWriter::instance=nullptr;


class GdResultReader
{   
    public:

        GdResultReader(const string& file_path) : _file_path(file_path) {}

        // Check if the file exists
        bool file_exists() const {
            ifstream f(_file_path);
            return f.good();
        }

        string get_contents() const {
            
            ifstream f(_file_path);
            if(!f.good()){
                return "";
            }

            string contents, line;
            while(getline(f, line)){
                contents += line + '\n';
            }
            // cut off last comma but keep the newline
            contents.pop_back();
            contents.pop_back();
            return contents;
        }

    private:
        std::string _file_path;
};

class GdResultParser {
    public:

        GdResultParser(const string& json_string) {
            parsed_json = json::parse(json_string);
            //cout << "Parsed json type: " << parsed_json.type_name() << ", size: " + to_string(parsed_json.size()) + "\n";
            DebugAssert(parsed_json.is_array(), "Parsed JSON is not an array!");
        }

        vector<SegmentPerformance> get_measurements(const std::string& table_col_name, const int chunkidx, const size_t rows){
            vector<SegmentPerformance> res;
            // See internal structure of the JSON in SegmentPerformance::to_json() and GdResultWriter::write_measurements()
            DebugAssert(parsed_json.is_array(), "Parsed JSON is not an array!");
            for(auto& record : parsed_json) {
                if(!record.contains("name")){
                    // skip last row
                    continue;
                }
                //cout << "'Record' is a " << record.type_name() << ", size: " << record.size() <<std::endl;
                //DebugAssert(record.is_object(), "'Record' is not a JSON object!");
                
                if(record["name"] == table_col_name && record["chunk_idx"] == chunkidx && record["rows"] == rows) {
                    SegmentPerformance pf;
                    auto results = record["results"];
                    //DebugAssert(results.is_object(), "'Results' is not a JSON object!");

                    pf.dev_bits = results["dev_bits"];
                    pf.compression_gain = results["compression_gain"];
                    pf.random_access_time = results["random_access_time"];
                    pf.table_scan_time = results["table_scan_time"];
                    if(results.contains("table_scan_time_valuesegment")){
                        pf.table_scan_time_valuesegment = results["table_scan_time_valuesegment"];
                    }
                    res.push_back(pf);
                }              
            }

            return res;
        }

    private:
        json parsed_json;
};

}