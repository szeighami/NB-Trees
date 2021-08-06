/*
 * BPTree.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef BPTREE_H_
#define BPTREE_H_
#include "DiskIterator.h"

extern double get_page_time;
extern int cache_misses;
extern int pages_read;

extern Cache* cache;
extern uint32_t PAGE_SIZE;
extern uint32_t MAX_VALUE_SIZE;

class BPTree
{
public:
	BPTree(Pointer* p, int nodeSize);
	~BPTree();


	void print(int level);
    void find(KEY low, KEY high, vector<KEY>* result);
	VALUE find(KEY key);
	void printNode(Pointer* node_pointer, int level);
	VALUE findValue(Pointer* node_pointer, KEY key, bool only_snode);
    void findValue(Pointer* node_pointer, KEY low, KEY high, vector<KEY>* result, bool only_snode);
    void setRoot(Pointer* root){this->root_pointer->copyPointer(root);}

protected:
	Pointer* root_pointer;
	File* file;
};




#endif /* BPTREE_H_ */
