#include "NBTree.h"

//d-node structure:
//											4 byte for total no. elements in the node (the number is written as a negative number)
//											sizeof(KEY)+sizeof(POINTER) byte for each key and its pointer
//											sizeof(POINTER) byte pointer pointing to the next level in the B+tree
//								Leaf nodes: 0 or 4 bytes for an integer 0 to denote the beginning of the outer node
//											4 byte for total no. elements in the node
//											sizeof(KEY)+sizeof(VALUE) byte key value pairs for each element 
//								End of d-tree:
//								            16 bytes each equal to 255


double get_page_time = 0;
long bloom_size = 0;
double search_BPT_time = 0;
int cache_misses = 0;
int pages_read = 0;
int no_false_positive = 0;
int no_bloom_searches = 0;

uint32_t MAX_VAL_SIZE = 0;
uint32_t PAGE_SIZE = 0;
Cache* cache = NULL;
atomic_bool is_merging;
atomic_int micro_s_per_ins;
atomic_int no_inserted;

void* mergeInBackground(void* nbtree)
{
    struct sched_param param;                                                                              
    param.sched_priority = 0;                                                                              

    int s;
    cpu_set_t cpuset;
    pthread_t thread;
    thread = pthread_self();
    CPU_ZERO(&cpuset);
    CPU_SET(1, &cpuset);
    s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);

    NBTree* nbt = (NBTree*)nbtree;
    uint64_t mem_leaf_bytes = nbt->getMemLeafBytes();

    while (true)
    {
        if (!(no_inserted*(MAX_VAL_SIZE+16) < mem_leaf_bytes))
        {
            nbt->setMemCap(no_inserted);
            no_inserted = 0;
            nbt->changeBuffer();
            is_merging = true;
            nbt->flushDownMemory();
            is_merging = false;
            micro_s_per_ins = 0;
        }
    }
    return NULL;
}

NBTree::~NBTree()
{
    flushDownMemory();

    delete cache;
	root_sn->deleteAncestors();
	delete root_sn;
    delete[] data_prefix;
    file_manger->actuallyDeleteFile();
    delete file_manger;
    delete memManager;
}

void NBTree::read_config(string conf_file_name) {
    std::ifstream in;
    in.open(conf_file_name.c_str());
    std::string str;

	data_prefix = new char[20];
	rnode_size = 4096;
	snode_size = 4;
	fanout = 4;
	mem_usage = -1;
    only_largest = false;
    probablistic_merge = false;
    bloom_bpe = 7;
    use_bloom_filters  = false;
    merge_in_background = false;
    MAX_VAL_SIZE = 128;
    bytes_per_second = 110 * (1L << 20);

    while(!in.eof()) {
        while(getline(in,str)) {
            std::string::size_type begin = str.find_first_not_of(" \f\t\v");
            //Skips blank lines
            if(begin == std::string::npos)
                continue;
            //Skips #
            if(std::string("#").find(str[begin]) != std::string::npos)
                continue;
            std::string firstWord;
            try {
                firstWord = str.substr(0,str.find("="));
            }
            catch(std::exception& e) {
                firstWord = str.erase(str.find_first_of(" "),str.find_first_not_of(" "));
            }
            std::transform(firstWord.begin(),firstWord.end(),firstWord.begin(), ::toupper);
            if(firstWord == "DATA_PREFIX")
            {
                strcpy(data_prefix, str.substr(str.find("=")+1,str.length()).c_str());
            }
            if(firstWord == "RNODE_SIZE")
                rnode_size = std::stoi(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "SNODE_SIZE")
                snode_size = std::stoi(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "BLOOM_BPE")
                bloom_bpe = std::stoi(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "FANOUT")
                fanout = std::stoi(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "MAX_VAL_SIZE")
                MAX_VAL_SIZE = std::stoi(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "MEM_USE")
                mem_usage = std::stoi(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "ONLY_LARGEST")
                only_largest = std::stoi(str.substr(str.find("=")+1,str.length())) == 1;
            if(firstWord == "PROBABLISTIC_MERGE")
                probablistic_merge = std::stoi(str.substr(str.find("=")+1,str.length())) == 1;
            if(firstWord == "BUFFER_SIZE")
                buffer_size = std::stol(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "BYTES_PER_SECOND")
                bytes_per_second = std::stol(str.substr(str.find("=")+1,str.length()));
            if(firstWord == "USE_BLOOM_FILTERS")
                use_bloom_filters = std::stoi(str.substr(str.find("=")+1,str.length())) == 1;
            if(firstWord == "MERGE_IN_BACKGROUND")
                merge_in_background = std::stoi(str.substr(str.find("=")+1,str.length())) == 1;
        }
    }
    if (mem_usage == -1)
    {
        mem_usage = (static_cast<uint64_t>(rnode_size)*(snode_size-1)*2L*12L)/10;
    }

    struct stat buffer;
    if (stat (data_prefix, &buffer) != 0)
    {
        char cmd[25];
        sprintf(cmd, "mkdir %s", data_prefix);
        system(cmd);
    }
    sprintf(data_prefix, "%s/data", data_prefix);
}

string NBTree::get_configs()
{
    stringstream conf_str;
    conf_str << "DATA_PREFIX: " << data_prefix << "\n";
    conf_str << "RNODE_SIZE: " << rnode_size << "\n";
    conf_str << "SNODE_SIZE: " << snode_size<< "\n";
    conf_str << "FANOUT: " << (int)fanout << "\n";
    conf_str << "MEM_USE: " << mem_usage << "\n";
    conf_str << "ONLY_LARGEST: " << only_largest << "\n";
    conf_str << "USE_BLOOM_FILTERS: " << use_bloom_filters << "\n";
    conf_str << "MAX_VAL_SIZE: " << MAX_VAL_SIZE << "\n";
    conf_str << "BLOOM_BPE: " << bloom_bpe << "\n";
    conf_str << "MERGE_IN_BACKGROUND: " << merge_in_background << "\n";
    conf_str << "PROBABLISTIC_MERGE: " << probablistic_merge << "\n";
    conf_str << "BUFFER_SIZE: " << buffer_size << "\n";

    return conf_str.str();

}

void NBTree::initialize()
{
    File::setDiskPrefix(this->data_prefix);

    File::setFileNo(1);
	File::initWSeekT();
	File::initRSeekT();
	File::initWSeqT();
	File::initRSeqT();


    file_manger = new FileManager;
    mem_leaf_bytes = (9*mem_usage)/20;
    memManager = new MemoryManager(mem_leaf_bytes);

    micro_s_per_ins = 0;

    thread = 0;
    no_inserted = 0;
    rc = -1;
    is_merging = false;
    merge_time = 0;
    cpu_merge_time = 0;

    PAGE_SIZE = this->rnode_size;
    cache = new Cache;
    this->root_sn = new InternalSkeletonNode(getMaxRKeysInSNode(), this->use_bloom_filters, this, false, this->rnode_size, this->bloom_bpe);
    root_sn->updatePointer(0, NULL);

    rc = pthread_create(&thread, NULL, mergeInBackground, (void*)this);
    counter = 0;

    int s;
    cpu_set_t cpuset;
    pthread_t thread;
    thread = pthread_self();
    CPU_ZERO(&cpuset);
    CPU_SET(3, &cpuset);
    s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}




std::string NBTree::getIOStats()
{
    std::string content = "";
	content += "write seek time: " + to_string(File::getWSeekT()) + "\n";
	content += "write no seek : " + to_string(File::getWSeekNo())+ "\n";
	content += "write seq. time: " + to_string(File::getWSeqT())+ "\n";
	content += "read seek time: " + to_string(File::getRSeekT())+ "\n";
    content += "read no seek: " + to_string(File::getRSeekNo())+ "\n";
	content += "read seq. time: " + to_string(File::getRSeqT())+ "\n";
    return content;
}

NBTree::NBTree(string config_file)
{
    read_config(config_file);
    initialize();
}

NBTree::NBTree(uint32_t inner_node_size, uint64_t snode_size, uint8_t fanout, uint64_t mem_usage, bool only_largest, bool use_bloom_filters, char* data_prefix, bool merge_in_background)
{

	this->snode_size = snode_size;
	this->fanout = fanout;
	this->rnode_size = inner_node_size;
	this->mem_usage = mem_usage;
    this->only_largest = only_largest;
    this->data_prefix = new char[10];
    this->use_bloom_filters = use_bloom_filters;
    this->bloom_bpe = 7;
    this->merge_in_background = merge_in_background;
    strcpy(this->data_prefix, data_prefix);
    
    initialize();
}

uint64_t NBTree::getMaxRKeysInSNode()
{
    return ((uint64_t)snode_size*rnode_size)/(sizeof(KEY)+MAX_VAL_SIZE);
}

uint64_t NBTree::getMaxSizeSNode()
{
    return (uint64_t)snode_size;
}

void NBTree::moveMemToDisk(MapNode* rkeys_to_merge)
{
    InternalSkeletonNode* new_root_sn = new InternalSkeletonNode(root_sn);
    if (root_sn->getNoElements() == 0)
    {
        root_sn->turnOffBloomFilters();
        root_sn->setNoPages(snode_size+1);
    }

    Merger* merger = new Merger(this, rnode_size, snode_size, buffer_size);
    new_root_sn->updatePointer(0, root_sn);
    root_sn->setNoRKeys(rkeys_to_merge->getNoElements());
    this->root_sn = new_root_sn;
    merger->merge(new_root_sn, rkeys_to_merge, true);
    rkeys_to_merge->setLock();
    auto rkeys_iter = rkeys_to_merge->getMapBegin();
    for (; rkeys_iter != rkeys_to_merge->getMapEnd();rkeys_iter++)
    {
        delete[] rkeys_iter->second;
        rkeys_iter->second = NULL;
    }
    rkeys_to_merge->clear();
    rkeys_to_merge->unsetLock();

    delete merger;
}


void NBTree::mergeDownMem(MapNode* rkeys_to_merge)
{
    Merger* merger = new Merger(this, rnode_size, snode_size, buffer_size);
	merger->merge(root_sn, rkeys_to_merge, false);
    delete merger;
}

void NBTree::flushDownMemory()
{
    merge_time_estimate = getMergeTimeEstimate(); 
    micro_s_per_ins = (merge_time_estimate*1000000)/mem_no_rkeys_cap+5;

    cache->deleteRedundantPages();
    file_manger->actuallyDeleteFile();

    MapNode* rkeys_to_merge = memManager->getNonInsertionNode();

    if (root_sn->getNoElements() == 0)
    {
        moveMemToDisk(rkeys_to_merge);
        return;
	}

    mergeDownMem(rkeys_to_merge);
	
    if (root_sn->getNoElements() > fanout-1)
        moveMemToDisk(rkeys_to_merge);

}


void NBTree::countCheck(int& no_nodes, int& no_elements)
{
    throw "Not implemented";
}

void NBTree::print()
{
    root_sn->print(0);


}


void NBTree::find(KEY low, KEY high, vector<KEY>* result)
{
    InternalSkeletonNode* root = root_sn;
	memManager->find(low, high, result);
    root->setLock();
    root->findFromRoot(low, high, result);
    root->unsetLock();
}

VALUE NBTree::find(KEY key)
{

    for (int i = 0;  i < buff_cap;i++)
    {   
        int indx = (buff_wrote-1-i);
        if (indx < 0)
            indx += buff_cap;

        if (buff_wrote > buff_read && (indx < buff_read||indx > buff_wrote))
            break;
        else if (buff_wrote < buff_read && (indx < buff_read&&indx > buff_wrote))
            break;
        else if (buff_wrote == buff_read)
            break;

        if (buff_key[indx] == key)
        {
            VALUE res;
            if (buff_val[indx]== NULL)
                break;
            memcpy(&res, buff_val[indx], sizeof(VALUE));
            return res;
        }
    }   

    InternalSkeletonNode* root = root_sn;
	VALUE value_found = memManager->find(key);
	if (value_found == -1)
    {
        root->setLock();
		VALUE val = root->findFromRoot(key);
        root->unsetLock();
		return val;
    }

	return value_found;

}

void NBTree::saveRoot()
{
    throw "Not Implemented";
}

void NBTree::LoadFromRoot()
{
    throw "Not Implemented";
}


uint64_t NBTree::getMergeTimeEstimate()
{
   uint64_t total_merge_size = root_sn->getMergeSize();
   return total_merge_size*rnode_size/this->bytes_per_second;
}




void NBTree::insertAtRoot(KEY key, char* value)
{
    auto start_s =chrono::high_resolution_clock::now();
    int buff_read_tmp = buff_read.load();

    if ((buff_wrote+1)%buff_cap == buff_read_tmp)
    {
        while ((buff_wrote+1)%buff_cap == buff_read);
    }
    buff_key[buff_wrote] = key;
    buff_val[buff_wrote] = value;
    if (buff_wrote == buff_cap - 1)
        buff_wrote = 0;
    else
        buff_wrote.fetch_add(1);

    auto end_s =chrono::high_resolution_clock::now();
    int t = chrono::duration_cast<chrono::microseconds>(end_s-start_s).count();
    start_s =chrono::high_resolution_clock::now();

    int rem = (buff_read_tmp - buff_wrote)%buff_cap;
    if (rem < 0)
        rem += buff_cap;

    int wait_time = micro_s_per_ins+200/(rem/5+1);
    

    while (t< wait_time)
    {   
        end_s =chrono::high_resolution_clock::now();
        t = chrono::duration_cast<chrono::microseconds>(end_s-start_s).count();
    }   
    

    return;
}

void NBTree::getSNodeSizes(std::vector<long>* result)
{
    root_sn->getSNodeSize(result);
}

