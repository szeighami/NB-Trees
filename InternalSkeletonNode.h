/*
 * InnerNode.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef INTERNALSKELETONNODE_H_
#define INTERNALSKELETONNODE_H_
#include "NBTree.h"
#include "BloomFilter.h"
#include "KeyValueType.h"
#include "BPTree.h"
#include <mutex>



class Record;
class Pointer;
class BPTree;
class NBTree;
class BloomFilter;

extern double search_BPT_time;
extern int no_false_positive;
extern int no_bloom_searches;
extern Cache* cache;
extern uint32_t MAX_VAL_SIZE;

class InternalSkeletonNode 
{
public:
	InternalSkeletonNode(unsigned long no_elements_snode, bool use_bloom_filters, NBTree* nbtree, bool is_leaf, int rnode_size, int bloom_bpe);

	InternalSkeletonNode(InternalSkeletonNode* other);

    void resetBloomFilter();
    void addToBloomFilter(KEY key);

    int getNoElements(){return no_elements;}
	KEY getKeyAt(int i)
	{
		if (i <= no_elements && i >=0)
			return keys[i];
		return 0;
	}
	InternalSkeletonNode* getPointerAt(int i)
	{
		if (i <= no_elements && i >=0)
			return pointers[i];
		return NULL;
	}
    void updatePointer(int i, InternalSkeletonNode* p)
    {
        pointers[i] = p;
    }

    BloomFilter* getBloomFilter();
    void insert(KEY key, InternalSkeletonNode* nodePointer);
    void printStructur(int level)
    {
        for (int j = 0; j < level; j++)
             cout << "\t";
        cout << this->getNoElements() + 1 << " children, " << this->no_rkeys_in_snode << "no rkeys, " << this->no_pages << "pages" << endl;
        for (int i = 0; getNoElements()!= 0 && i <= getNoElements(); i++)
            this->getPointerAt(i)->printStructur(level+1);
    }
    void printNode(int level)
    {
        for (int j = 0; j < level; j++)
             cout << "\t";
        cout << this->getNoElements() << " elements" << endl;

        if (getNoElements() == 0)
            return ;
        for (int i = 0; i < getNoElements(); i++)
        {
             ((InternalSkeletonNode*)this->getPointerAt(i))->printNode(level + 1);
             for (int j = 0; j < level; j++)
                 cout << "\t";
             cout <<this->getKeyAt(i) << endl;
        }
        ((InternalSkeletonNode*)this->getPointerAt(getNoElements()))->printNode(level + 1);
    }

    VALUE find(KEY key);
    VALUE findFromRoot(KEY key);
    void findFromRoot(KEY low, KEY high, vector<KEY>* result);
    void find(KEY low, KEY high, vector<KEY>* result);
    void checkDataPointer(int level = 0)
    {
        if (getNoElements() == 0)
        {
            for (int j = 0; j < level; j++)
                 cout << "\t";
             cout << "data"<< this->dataRootPointer->getDisk() << "/NBTree" << this->dataRootPointer->getFileNo() << ".txt:" << this->dataRootPointer->getLocation() <<  endl;
             return;
        }
        for (int i = 0; i < getNoElements(); i++)
        {
             ((InternalSkeletonNode*)this->getPointerAt(i))->checkDataPointer(level + 1);
             for (int j = 0; j < level; j++)
                 cout << "\t";

             cout << "data"<< this->dataRootPointer->getDisk() << "/NBTree" << this->dataRootPointer->getFileNo() << ".txt:" << this->dataRootPointer->getLocation() <<  endl;
        }
        ((InternalSkeletonNode*)this->getPointerAt(getNoElements()))->checkDataPointer(level + 1);
    }

    void checkBloomFilter(int level = 0)
    {
        if (getNoElements() == 0)
        {
            for (int j = 0; j < level; j++)
                 cout << "\t";
//            bloomFilter->printInfo();
             return;
        }
        for (int i = 0; i < getNoElements(); i++)
        {
             ((InternalSkeletonNode*)this->getPointerAt(i))->checkBloomFilter(level + 1);
             for (int j = 0; j < level; j++)
                 cout << "\t";

//            bloomFilter->printInfo();
        }
        ((InternalSkeletonNode*)this->getPointerAt(getNoElements()))->checkBloomFilter(level + 1);
    }

    void writeBloomFilter();
    bool useBloomFilters(){return use_bloom_filters;}

    ~InternalSkeletonNode();
    
    bool isLeaf(){return getNoElements() == 0;}
    bool hasTree(){return has_tree;}

    void updateDiskInfo(Pointer* dataBeginPointer, Pointer* dataRootPointer, unsigned long no_rkeys, unsigned long no_pages, bool has_tree, uint64_t begin_index, bool tree_starts_from_begin)
    {
        if (has_tree)
        {
            this->tree_starts_from_begin = tree_starts_from_begin;
            this->begin_index = begin_index;
            if (dataBeginPointer)
                this->dataBeginPointer->copyPointer(dataBeginPointer);
            if (dataRootPointer)
                this->dataRootPointer->copyPointer(dataRootPointer);
            this->no_rkeys_in_snode = no_rkeys;
            this->no_pages = no_pages; 
        }
        this->has_tree = has_tree;
    }
    void setNoRKeys(unsigned long no_rkeys){ this->no_rkeys_in_snode = no_rkeys;}
    void setNoPages(unsigned long no_pages){ this->no_pages = no_pages;}

    unsigned long getNoRKeys(){return no_rkeys_in_snode;}
    //This is not entirley accurate
    unsigned long getNoPages(){return no_pages;}

    Pointer* getBeginPointer(){return dataBeginPointer;}
    Pointer* getRootPointer(){return dataRootPointer;}
    uint64_t getBeginIndex(){return begin_index;}
    bool treeStartsFromBegin(){return tree_starts_from_begin;}
    void turnOffBloomFilters(){use_bloom_filters = false;}
    void print(int no_tab);
    void deleteAncestors();
    
    void setLock(){mlock.lock();}
    void unsetLock(){ mlock.unlock(); }

    uint64_t getSizesAt(int i)
    {
        if (sizes == NULL)
            return 0;
        return sizes[i];
    }

    uint64_t getElementCountsAt(int i)
    {
        if (element_counts == NULL)
            return 0;
        return element_counts[i];
    }

    uint64_t* getSizes()
    {
        delete[] sizes;
        sizes = new uint64_t[no_elements+1];
        return sizes;
    }

    uint64_t* getElementCounts()
    {
        delete[] element_counts;
        element_counts = new uint64_t[no_elements+1];
        return element_counts;
    }
    void getSNodeSize(std::vector<long>* result);
    uint64_t getMergeSize();
    int getHeight()
    {
        if (isLeaf())
            return 1;
        return 1+pointers[0]->getHeight();
    }

private: 
    bool has_tree;
    Pointer* dataBeginPointer;
    uint64_t begin_index;
    Pointer* dataRootPointer;
    BloomFilter* bloomFilter;
    BloomFilter* bufferBloomFilter;
    double bloom_filter_error_rate;
    unsigned long no_rkeys_in_snode;
    unsigned long max_no_rkeys_snode;
    unsigned long no_pages;
    bool use_bloom_filters;
    bool tree_starts_from_begin;
    int rnode_size;
    NBTree* tree;
    int bloom_bpe;

    KEY* keys;
    InternalSkeletonNode** pointers;
    uint64_t* sizes;
    uint64_t* element_counts;
    int no_elements;

    std::mutex mlock;
};






#endif /* INTERNALINNERNODE_H_ */
