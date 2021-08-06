#ifndef MEMORY_MANAGER
#define MEMORY_MANAGER
#include "KeyValueType.h"
#include <atomic>
#include <unistd.h>
#include <mutex>
#include <pthread.h> 
#include "btree/btree_map.h"
#include <vector>
#include <map>

using namespace std;
//typedef btree::btree_map<KEY, char*, std::less<KEY>,std::allocator<std::pair<KEY, char*> >, 512> btree_type;
//typedef btree::btree_map<KEY, char*> btree_type;
//typedef std::map<KEY, char*> btree_type;
typedef map<KEY, char*> btree_type;

extern uint32_t MAX_VAL_SIZE;

class MapNode
{
public:

	MapNode(long max_size){this->max_size = max_size; this->data = NULL; i = 0; curr_size = 0;}

    void setLock(){mlock.lock();}
    void unsetLock(){ mlock.unlock(); }

    bool canInsert(){ bool out = (getBytesUsed() < max_size) ; return out; }

    btree_type::iterator getMapBegin()
    {
        return getData()->begin();
    }

    btree_type::iterator getMapEnd()
    {
        return getData()->end();
    }

    void find(KEY low, KEY high, vector<KEY>* result)
    {
        btree_type::iterator it = getData()->find(low); 
        while (it != getData()->end() && it->first <= high)
        {
            result->push_back(it->first);
            ++it;
        }
    }

    VALUE find(KEY key)
    {
        btree_type::iterator result = getData()->find(key); 
        if (result != getData()->end())
        {
            char* val = result->second;
            VALUE res;
            memcpy(&res, val, sizeof(VALUE));
            return res; 
        }

        return -1; 
    }

    void clear()
    {
        i = 0; curr_size = 0;
        data->clear();
        delete data;
        data = NULL;
    }

    ~MapNode()
    {
        auto rkeys_iter = getMapBegin();
        for (; rkeys_iter != getMapEnd();rkeys_iter++)
        {
            delete[] rkeys_iter->second;
            rkeys_iter->second = NULL;
        }
        data->clear();
        delete data;
    }
    unsigned long getNoElements()
    {
        return getData()->size();
    }

    bool isClear(){return data == NULL;}

    unsigned long getBytesUsed()
    {
        return getNoElements()*(MAX_VAL_SIZE+16);
        uint64_t freq = 100000;
        if (i % freq == 0)
        {
        //    curr_size = getData()->bytes_used();
        }
        i++;
        return curr_size + getData()->size()*MAX_VAL_SIZE;
        //std::cout << data->bytes_used() << std::endl;
        //return data->size();
    }

    btree_type* getData(){if (data == NULL) data = new btree_type; return data;}
        
    

private:
    btree_type* data;
    int i;
    uint64_t curr_size;
    long max_size;
    std::mutex mlock;
};

class MemoryManager
{
public:
    //TODO: Add the option of selecting or not selecting a buffer
    MemoryManager(long node_size);

    bool cleared();

    void insertInMemory(KEY key, char* value);
    bool canInsert();
    long get_max_size(){return node_size;}

    //Need to set the lock for the node received from this function
    //One of the nodes has to empty at this function call
    MapNode* getLargerNode();
    MapNode* getNonInsertionNode();
    btree_type* getInsertionNode()
    {
        if (insert_in_1)
            return node1->getData();
        return node2->getData();
    }

    long getTotalSize(){return (node1->getBytesUsed() + node2->getBytesUsed());}

   
    void changeBuffer();

    VALUE find(KEY key);
    void find(KEY low, KEY high, vector<KEY>* result);
     ~MemoryManager();


    /*KEY* get_buff_key(){return buff_key;}
    char** get_buff_val(){return buff_val;}
    int get_buff_cap(){return buff_cap;}
    int* get_buff_read(){return &buff_read;}
    int* get_buff_wrote(){return &buff_wrote;}*/

private:
    MapNode* node1;
    MapNode* node2;
    int counter;

    pthread_t thread;
    int rc;
    void* status;

    bool insert_in_1;
    long node_size;
};

#endif //MEMORY_MANAGER
