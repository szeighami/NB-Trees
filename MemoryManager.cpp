#include "MemoryManager.h"

/*KEY* MemoryManager::buff_key;
char** MemoryManager::buff_val;
int MemoryManager::buff_cap;
int MemoryManager::buff_read;
int MemoryManager::buff_wrote;
*/

KEY buff_key[10000];
char* buff_val[10000];
int buff_cap=10000;
int buff_read;
int buff_wrote;
extern atomic_bool is_merging;
extern atomic_int no_inserted;

void* insertInBackground(void* arg)
{
    int s;
    //struct sched_param params;
    //params.sched_priority = sched_get_priority_max(SCHED_FIFO);
    cpu_set_t cpuset;
    pthread_t thread;
    thread = pthread_self();
    CPU_ZERO(&cpuset);
    CPU_SET(2, &cpuset);
    s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    //s = pthread_setschedparam(thread, SCHED_FIFO, &params);
    MemoryManager* mem_manager = (MemoryManager*)arg;
    long mem_leaf_bytes = mem_manager->get_max_size();
    /*KEY* buff_key = mem_manager->get_buff_key();
    char** buff_val = mem_manager->get_buff_val();
    int buff_cap = mem_manager->get_buff_cap();
    int* buff_read = mem_manager->get_buff_read();
    int* buff_wrote = mem_manager->get_buff_wrote();*/


    int dist;
    while (true)
    {
        dist = buff_wrote-buff_read;
        if (dist < 0)
            dist = dist + buff_cap;
        //if ((buff_wrote<buff_read)||(buff_wrote-buff_read>50) )
        //if ((buff_wrote!=(buff_read)) && (buff_wrote%100 == 0))
        if (dist > 5)
        {
            if (is_merging)
            {
                if (!(no_inserted*(MAX_VAL_SIZE+16) < mem_leaf_bytes))
                {
                    while (is_merging)
                        usleep(1000);
                }
            }
            KEY key = buff_key[buff_read];
            char* value = buff_val[buff_read];

            auto insertion_node = mem_manager->getInsertionNode();
            insertion_node->insert(std::make_pair(key, value));
            
            buff_read = (buff_read+1)%buff_cap;
            no_inserted++;
        }
        else
            usleep(20);
    }

}

MemoryManager::MemoryManager(long node_size)
{
   node1 = new MapNode(node_size);
   node2 = new MapNode(node_size);
   insert_in_1 = true;
   this->node_size = node_size;
   /*buff_cap = 1000;
   buff_key = new KEY[buff_cap];
   buff_val = new char*[buff_cap];
   buff_read = 0;
   buff_wrote = 0;*/
   counter = 0;
   rc = pthread_create(&thread, NULL, insertInBackground, (void*)this);
}

MemoryManager::~MemoryManager()
{
    delete node1;
    delete node2;
}


MapNode* MemoryManager::getLargerNode()
{
    if (node1->getNoElements() > node2->getNoElements())
        return node1;
    else
        return node2;
}
MapNode* MemoryManager::getNonInsertionNode()
{
    if (insert_in_1)
        return node2;
    return node1;
}

bool MemoryManager::cleared()
{
    return node1->isClear() || node2->isClear();
}

bool MemoryManager::canInsert()
{
    if (insert_in_1)
        return node1->canInsert();
    return node2->canInsert();
}


void MemoryManager::changeBuffer()
{
    insert_in_1 = !insert_in_1;
}
void MemoryManager::insertInMemory(KEY key, char* value)
{

    //usleep(100);
    for (int i = 0; i < 500; i++)
        int j = i*23/2;
//    counter = 0;

//    counter++;

    return;
    buff_key[buff_wrote] = key;
    buff_val[buff_wrote] = value;

    if (buff_wrote == buff_cap - 1)
        buff_wrote = 0;
    else
        buff_wrote++;


}

void MemoryManager::find(KEY low, KEY high, vector<KEY>* result)
{
    node1->setLock();
    node1->find(low, high, result);
    node1->unsetLock();
    
    node2->setLock();
    node2->find(low, high, result);
    node2->unsetLock();
}

VALUE MemoryManager::find(KEY key)
{
    node1->setLock();
    VALUE val = node1->find(key);
    node1->unsetLock();
    if (val != -1)
        return val;
    
    node2->setLock();
    val = node2->find(key);
    node2->unsetLock();
    return val;
}



