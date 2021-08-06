#include "NBTree.h"

#include <fstream>

#include <climits>
#include <sstream>

#include <iostream>
#include <iomanip>
#include <string>
#include <exception>
#include <fstream>
#include <vector>
#include <algorithm>
 
extern Cache* cache;

struct conf {
    long no_ops;
    long no_insertions;
    float insert_prop;
    bool prop_workload;
    bool perform_query;
    bool zipfian_query;
    bool measure_insertion_rate;
    bool measure_insertion_time;
    int insert_per_second;
    int expected_query_match;
    long query_freq;
    bool print_query_result;
    int total_queries;
    long measure_freq;
    long max_key;

};
 
struct operation {
    bool is_query;
    bool is_range;
    bool is_insertion;
    KEY key;
    KEY high;
    VALUE val;
}; 
class Executor{
public:
    Executor(string conf_file_name)
    {
        this->random_seed=(int)time(NULL);
        configs = new conf;
        read_config(conf_file_name, configs);

        srand(this->random_seed);

        nbtree = NULL;
        this->queries = NULL;
        if (configs->perform_query)
        {
            cout << configs->total_queries << endl;
            this->queries = new KEY[configs->total_queries];
            for (int i = 0; i < configs->total_queries; this->queries[i++]=0);
        }

        //fp_input = fopen("/home/localtmp/sshpass/bin/out.txt", "r");
        fp_input = NULL;
        if (configs->zipfian_query)
            fp_input = fopen("/home/localtmp/zipfian.txt", "r");

        no_inserted = 1;
        no_queried = 0;
    }
    
    void execute_workload();

    ~Executor()
    {
        delete this->configs;
        this->configs = NULL;
        delete [] this->queries;
        this->queries = NULL;
    }

private:
    operation* get_next_op(long op_no);
    void read_config(string conf_file_name, conf* out);
    string getIOStat();
    operation get_op_from_file();
    string get_configs();
    operation enforce_insertion_rate(Time* insert_rate_t, ofstream* fp_rate);
    operation* get_query();
    bool next_is_insert();
    operation* get_insertion();
    operation* get_next_op();
    void perform_op(operation* op);

    conf* configs;
    KEY* queries;
    FILE * fp_input;
    NBTree* nbtree;
    long no_inserted;
    long no_queried;
    int random_seed;
};


