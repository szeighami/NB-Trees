/*
 * NBTree.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef NBTREE_H_
#define NBTREE_H_
#include "MemoryManager.h"
#include <thread>
#include <pthread.h>
#define BGP

#include <boost/chrono/chrono.hpp>
#include <stdio.h>
#include <sched.h>
#include <time.h>

#include <sstream>
#include "BPTree.h"
#include "Murmurhash3.h"
#include  <future>
#include <boost/functional/hash.hpp>

#include <atomic>
#include "Merger.h"
#include "FileManager.h"
#include <algorithm>
#include <vector>
#include "btree/btree_map.h"
#include <fstream>
#include <sstream>
#include <sys/stat.h>


class Record;
class FileManager;
class BPTree;
class MemoryWriter;
class Merger;
class InMemoryOuterNode;
class InternalSkeletonNode;



extern KEY buff_key[10000];
extern char* buff_val[10000];
extern int buff_cap;
extern atomic_int buff_read;
extern atomic_int buff_wrote;
extern atomic_bool is_merging;

class NodeStatus
{
public:
	NodeStatus()
	{
		oversized_count = 0;
		largest_index = 0;
		largest_size = 0;
	}
	void addOversizedNode(uint32_t node_index, uint64_t size_written)
	{

		if (oversized_count >= oversized_index.size())
			oversized_index.resize(oversized_count+1);
		oversized_index[oversized_count] = node_index;
		oversized_count++;
        
        if (size_written>largest_size)
        {
            largest_size = size_written;
            largest_index = node_index;
        }
	}


	uint32_t getOversizedCount() {return oversized_count;}
	uint32_t getOSNodeIndexAt(uint32_t i)	{		return oversized_index[i];	}
    uint32_t getLargestIndex(){return largest_index;}

	~NodeStatus(){	}

	bool hasOverSizedNodes(){return oversized_count != 0;}

private:
	vector<uint32_t> oversized_index;
	uint32_t oversized_count;
	uint32_t largest_index;
	uint64_t largest_size;
};



class NBTree
{
public:

	NBTree(uint32_t inner_node_size, uint64_t snode_size, uint8_t fanout, uint64_t mem_usage, bool only_largest, bool use_bloom_filters, char* data_prefix, bool merge_in_background);
    NBTree(string config_file);

	~NBTree();

	void insertAtRoot(KEY key, char* value);
	VALUE find(KEY key);
    void find(KEY low, KEY high, vector<KEY>* result);


    string get_configs();

	void print();
	void countCheck(int& no_nodes, int& no_elements);

    uint64_t getMaxRKeysInSNode();
    uint64_t getMaxSizeSNode();
	uint8_t getFanout(){return fanout;}
	uint32_t getRNodeSize(){return rnode_size;}



	void flushDownMemory();

	void LoadFromRoot();
	void saveRoot();
    bool mergeLargestOnly(){return only_largest;}

	void getSNodeSizes(std::vector<long>* result);
    FileManager* getFileManager(){return file_manger;}

	void finishOperations()
	{
#ifdef BGP
		//if (is_merging) rc = pthread_join(thread, &status);
        while (is_merging)
            usleep(1000);

	    cache->deleteRedundantPages();
	    file_manger->actuallyDeleteFile();
#endif
	}
    float getResetMergeTime(){float t = merge_time; merge_time=0; return t;}
    float getResetCPUMergeTime(){float t = cpu_merge_time; cpu_merge_time=0; return t;}
    float getResetSysMergeTime(){float t = sys_merge_time; sys_merge_time=0; return t;}
    void increaseMergeTime(float t){merge_time+=t;}
    void increaseCPUMergeTime(float t){cpu_merge_time+=t;}
    void increaseSysMergeTime(float t){sys_merge_time+=t;}
    std::string getIOStats();

    long getSize(){return sizeof(*this)+ sizeof(MemoryManager) + memManager->getTotalSize();}

    uint64_t getBufferSize(){return buffer_size;} 

    //bool isMerging(){return is_merging;}
    //void finishedMerge() {is_merging = false;}
    uint64_t getMemLeafBytes(){return mem_leaf_bytes;}
    void setMemCap(long mem_cap){mem_no_rkeys_cap = mem_cap;}
    void changeBuffer(){memManager->changeBuffer();}
    bool isMergeProbablistic(){return probablistic_merge;}
    uint64_t getMergeTimeEstimate();

    InternalSkeletonNode* root_sn;
private:

    void moveMemToDisk(MapNode* rkeys_to_merge);
    void mergeDownMem(MapNode* rkeys_to_merge);
    void read_config(string conf_file_name);
    void initialize();

    
	FileManager* file_manger;
    MemoryManager* memManager;

    float merge_time;
    float cpu_merge_time;
    float sys_merge_time;
    char* data_prefix;
    btree_type* insertion_node;

	uint8_t fanout;
    //This is in no rnodes
	uint64_t snode_size;
    //This is in bytes
	uint32_t rnode_size;
    //Mem size used to store rkeys in bytes
	uint64_t mem_usage;
	uint64_t mem_leaf_bytes;
    bool use_bloom_filters;
	bool only_largest;
	bool merge_in_background;
    uint32_t bloom_bpe;
    uint64_t buffer_size;
    bool probablistic_merge;

    long merge_time_estimate;
    long mem_no_rkeys_cap;
    //long micro_s_per_ins;
    uint64_t no_inserted;
    uint64_t bytes_per_second;
    uint32_t counter;
    std::chrono::high_resolution_clock::time_point start_s;
    std::chrono::high_resolution_clock::time_point end_s;
    int t;

    /*KEY* buff_key;
    char** buff_val;
    int buff_cap;
    int* buff_read;
    int* buff_wrote;*/

    //std::future<void*> f;
    pthread_t thread;
    int rc;
    void *status;

};




#endif /* BPTREE_H_ */
