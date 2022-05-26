#include <string>
#include <fstream>
#include <mutex>

namespace gdsegment
{
    using namespace std;

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
    
    private:

    std::mutex _writerMutex;
    std::string _file_path;
};

GdResultWriter* GdResultWriter::instance=nullptr;


}