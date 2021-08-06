#include "InternalSkeletonNode.h"

InternalSkeletonNode::InternalSkeletonNode(unsigned long no_elements_snode, bool use_bloom_filters, NBTree* tree, bool is_leaf, int rnode_size, int bloom_bpe)
{
    this->sizes = NULL;
    this->element_counts = NULL;
    this->has_tree = false;
	this->keys = NULL;
	this->no_elements = 0;
	this->pointers = new InternalSkeletonNode*[1];
    this->tree = tree;
    this->max_no_rkeys_snode = no_elements_snode;
    this->bloom_filter_error_rate = 0.5;
    this->rnode_size = rnode_size;
    this->tree_starts_from_begin = true;
    dataBeginPointer = new Pointer(0, 0, 1);
    dataRootPointer = new Pointer(0, 0, 1);
    this->use_bloom_filters = use_bloom_filters;
    this->bloom_bpe = bloom_bpe;
    //if (is_leaf)
    //    this->use_bloom_filters = false;
    if (!this->use_bloom_filters)
        bloomFilter = NULL;
    else
        bloomFilter = new BloomFilter(max_no_rkeys_snode, bloom_filter_error_rate, cache, bloom_bpe);
    bufferBloomFilter = NULL;
}

InternalSkeletonNode::InternalSkeletonNode(InternalSkeletonNode* other)
{
    this->sizes = NULL;
    this->element_counts = NULL;
	this->keys = NULL;
    this->has_tree = false;
	this->no_elements = 0;
	this->pointers = new InternalSkeletonNode*[1];
    this->no_pages = 0;
    this->no_rkeys_in_snode = 0;
    this->tree_starts_from_begin = other->tree_starts_from_begin;
    this->tree = other->tree;
    this->max_no_rkeys_snode = other->max_no_rkeys_snode;
    this->bloom_filter_error_rate = other->bloom_filter_error_rate;
    this->rnode_size = other->rnode_size;
    dataBeginPointer = new Pointer(other->dataBeginPointer);
    dataRootPointer = new Pointer(other->dataRootPointer);
    this->use_bloom_filters = other->use_bloom_filters;
    this->bloom_bpe = other->bloom_bpe;
    if (!this->use_bloom_filters)
        bloomFilter = NULL;
    else
        bloomFilter = new BloomFilter(max_no_rkeys_snode, bloom_filter_error_rate, cache, other->bloom_bpe);
    bufferBloomFilter = NULL;
}

void InternalSkeletonNode::resetBloomFilter()
{
    if (!use_bloom_filters)
        return;
    if (bufferBloomFilter == NULL)
    {
        bufferBloomFilter = new BloomFilter(max_no_rkeys_snode, bloom_filter_error_rate, cache, bloom_bpe);
    }

}

void InternalSkeletonNode::getSNodeSize(std::vector<long>* result)
{
    result->push_back(no_pages);

    if (isLeaf())
        return;

    for (int i = 0; i <= no_elements; i++)
        getPointerAt(i)->getSNodeSize(result);

}

uint64_t InternalSkeletonNode::getMergeSize()
{
    if (isLeaf())
        return 0;

    uint64_t max = 0;
    uint64_t merge_size =  2*tree->getMaxSizeSNode();
    for (int i = 0; i <= no_elements; i++)
    {
        merge_size += 2*getPointerAt(i)->no_pages;
        uint64_t size = 0;
        if (sizes == NULL )
        {   
            size = getPointerAt(i)->getMergeSize();
        }   
        else
        {   
            if (getPointerAt(i)->no_pages+sizes[i]+5 > tree->getMaxSizeSNode())
                size = getPointerAt(i)->getMergeSize();
        }   
        if (size > max)
            max = size;
    }
    return merge_size + max;

}

void InternalSkeletonNode::print(int no_tabs)
{
    if (getNoElements() == 0)
    {
        for (int i = 0; i < no_tabs; i++)cout << "\t";
        cout << "LEAF " << no_rkeys_in_snode << "," << no_pages << endl;
        for (int i = 0; i < no_tabs; i++)cout << "\t";
        cout << "+++++++++++++++++++++++++++" << endl;
		BPTree* bptree = new BPTree(dataRootPointer, rnode_size);
		bptree->print(no_tabs);
		delete bptree;
        for (int i = 0; i < no_tabs; i++)cout << "\t";
        cout << "+++++++++++++++++++++++++++" << endl;		
        return;
    }
    for (int i = 0; i < no_tabs; i++)cout << "\t";
    cout << "NOT LEAF " << no_rkeys_in_snode << "," << no_pages << endl;
    if (no_tabs!=0)
    {
        for (int i = 0; i < no_tabs; i++)cout << "\t";
        cout << "---------------------------" << endl;
        BPTree* bptree = new BPTree(dataRootPointer, rnode_size);
        bptree->print(no_tabs);
        delete bptree;
        for (int i = 0; i < no_tabs; i++)cout << "\t";
        cout << "----------------------------" << endl;		
    }
    for (int i = 0; i < getNoElements();i++)
    {
        ((InternalSkeletonNode*)getPointerAt(i))->print(no_tabs+1);
        for (int i = 0; i < no_tabs; i++)cout << "\t";
        cout << getKeyAt(i) << endl;
    }
    ((InternalSkeletonNode*)getPointerAt(getNoElements()))->print(no_tabs+1);

}

BloomFilter* InternalSkeletonNode::getBloomFilter()
{
    if (!use_bloom_filters)
        return NULL;
    if (bufferBloomFilter != NULL)
        return bufferBloomFilter;
    return bloomFilter;
}

void InternalSkeletonNode::addToBloomFilter(KEY key)
{
    if (!use_bloom_filters)
        return;
    if (bufferBloomFilter != NULL)
        bufferBloomFilter->addElement(key);
    else
        
        bloomFilter->addElement(key);
}

void InternalSkeletonNode::writeBloomFilter()
{
    if (!use_bloom_filters)
        return;
    bool is_leaf_snode = getNoElements() == 0;
    if (bufferBloomFilter != NULL)
    {
        BloomFilter* temp = bloomFilter;
        bloomFilter = bufferBloomFilter;
        delete temp;
        bufferBloomFilter = NULL;
    }
    bloomFilter->writeToFile(!is_leaf_snode);

}

void InternalSkeletonNode::insert(KEY key, InternalSkeletonNode* nodePointer)
{
    KEY* new_keys = new KEY[no_elements+1];
    InternalSkeletonNode** new_pointers = new InternalSkeletonNode*[no_elements+2];
    int written = 0;
    for (int i = 0; i < no_elements; )
    {
        if (key < keys[i] && written == 0)
        {
            new_keys[i+written] = key;
            new_pointers[i+written] = nodePointer;
            written = 1;
        }
        else
        {
            new_keys[i+written] = keys[i];
            new_pointers[i+written] = pointers[i];
            i++;
        }
    }
    if (written == 1)
        new_pointers[no_elements+1] = pointers[no_elements];
    else
    {
        new_keys[no_elements] = key;
        new_pointers[no_elements] = nodePointer;
        new_pointers[no_elements+1] = pointers[no_elements];
    }
    delete[] this->keys;
    this->keys = new_keys;
    delete[] this->pointers;
    this->pointers = new_pointers;
    no_elements++;
}

VALUE InternalSkeletonNode::find(KEY key)
{
    if (has_tree && (!use_bloom_filters || bloomFilter->exists(key)))
    {
        Time* time = new Time;
        Util::startTime(time);

        BPTree* bptree = new BPTree(dataRootPointer, rnode_size);
        VALUE val = bptree->find(key);
        delete bptree;

        search_BPT_time += Util::getTimeElapsed(time);
        delete time;

        //cout << "HERE!" << endl;
        if (use_bloom_filters)
            no_bloom_searches++;
        if (val != -1)
        {
            return val;
        }

        if (use_bloom_filters)
            no_false_positive++;
    }

    if (no_elements == 0)
    {
        return -1;
    }

    for (int i = 0; i < no_elements; i++)
    {
        if (keys[i] >= key)
        {
            InternalSkeletonNode* child = pointers[i];
            child->setLock();
            VALUE val = child->find(key);
            child->unsetLock();
            return val;
        }
    }
    InternalSkeletonNode* child = pointers[no_elements];
    child->setLock();
    VALUE val = child->find(key);
    child->unsetLock();
    return val;
}

VALUE InternalSkeletonNode::findFromRoot(KEY key)
{
    if (no_elements == 0)
    {
        if (pointers[0] == NULL)
            return -1;

        InternalSkeletonNode* child = pointers[0];
        child->setLock();
        VALUE val = child->find(key);
        child->unsetLock();
        return val;
    }

    for (int i = 0; i < no_elements; i++)
    {
        if (keys[i] >= key)
        {
            InternalSkeletonNode* child = pointers[i];
            child->setLock();
            VALUE val = child->find(key);
            child->unsetLock();
            return val;
        }
    }
    InternalSkeletonNode* child = pointers[no_elements];
    child->setLock();
    VALUE val = child->find(key);
    child->unsetLock();
    return val;
}

void InternalSkeletonNode::find(KEY low, KEY high, vector<KEY>* result)
{
    Time* time = new Time;
    Util::startTime(time);

    if (has_tree)
    {
        BPTree* bptree = new BPTree(dataRootPointer, rnode_size);
        bptree->find(low, high, result);
        delete bptree;
    }

    search_BPT_time += Util::getTimeElapsed(time);
    delete time;

    if (getNoElements() == 0)
        return;

    for (int i = 0; i <= getNoElements(); i++)
    {
        if ((i == getNoElements() || getKeyAt(i) > low) && (i==0 || getKeyAt(i-1) <= high))
        {
            InternalSkeletonNode* child = (InternalSkeletonNode*)this->getPointerAt(i);
            child->setLock();
            child->find(low, high, result);
            child->unsetLock();
        }
    }

}

void InternalSkeletonNode::findFromRoot(KEY low, KEY high, vector<KEY>* result)
{
    if (getNoElements() == 0)
    {
        if (this->getPointerAt(0) == NULL)
            return;

        InternalSkeletonNode* child = (InternalSkeletonNode*)this->getPointerAt(0);
        child->setLock();
        child->find(low, high, result);
        child->unsetLock();
        return;
    }

    for (int i = 0; i <= getNoElements(); i++)
    {
        if ((i == getNoElements() || getKeyAt(i) > low) && (i==0 || getKeyAt(i-1) <= high))
        {
            InternalSkeletonNode* child = (InternalSkeletonNode*)this->getPointerAt(i);
            child->setLock();
            child->find(low, high, result);
            child->unsetLock();
        }
    }
}

InternalSkeletonNode::~InternalSkeletonNode()
{
    delete dataBeginPointer;
    delete dataRootPointer;
    delete bloomFilter;
    delete bufferBloomFilter;
    delete[] keys;
    delete[] pointers;
    delete[] element_counts;
    delete[] sizes;
}


    	
void InternalSkeletonNode::deleteAncestors()
{
    if (getNoElements()==0)
        return;

    for (int i = 0; i <= getNoElements(); i++)
    {
        ((InternalSkeletonNode*)getPointerAt(i))->deleteAncestors();
        delete ((InternalSkeletonNode*)getPointerAt(i));
    }
}
