/*
 * MemMerger.h
 *
 *  Created on: 6 Feb 2018
 *      Author: sepanta
 */

#ifndef MEMMERGER_H_
#define MEMMERGER_H_
#include <vector>
#include <stdexcept>

#include <map>
#include <random>
#include <climits>

#include "NBTree.h"
#include "InternalSkeletonNode.h"
#include "MemoryWriter.h"
#include "MemoryManager.h"
#include "MemIterator.h"
#include "DiskIterator.h"
//#include "File.h"

class Record;
//class File;
class InternalBPTree;
class InternalSkeletonNode;
class IBPTreeIterator;
class BPTree;
class NodeStatus;
class MoveUpData;
class MemoryWriter;
class KeyValue;
class MemoryManager;
class MapNode;
class Iterator;

class NBTree;

class MemIterator;

class Merger
{
public:
	Merger(NBTree* tree, count_t rnode_size, count_t snode_size, count_t buffer_size){this->rnode_size = rnode_size; this->snode_size = snode_size; this->tree = tree; this->buffer_size = buffer_size;}


    void merge(InternalSkeletonNode* parent_sn, MapNode* rkeys_to_merge, bool only_move_memory);

private:
	count_t rnode_size;
	count_t snode_size;
	count_t buffer_size;
    NBTree* tree;


	void getMoveDownIndices(count_t*& indices, count_t& count, NodeStatus* node_status, bool only_largest);
    //returns true if the child was split by size and false otherwise
    bool moveParentToChild(Iterator* child_iter, Iterator* parent_iter, InternalSkeletonNode* child_sn, InternalSkeletonNode* parent_sn, MemoryWriter* mem_writer, int curr_child_index);
    bool splitChildBySKeys(InternalSkeletonNode* root_sn,InternalSkeletonNode*& child_sn, int child_index);
    void splitOversizedLeaf(InternalSkeletonNode* parent_sn, InternalSkeletonNode* child_sn, int child_index, KEY child_key);
    void finishWritingChild(MemoryWriter* mem_writer, NodeStatus* children_status, int curr_child_index, InternalSkeletonNode* child_sn, uint64_t total_child_size);
    void updateParentInfo(InternalSkeletonNode* parent_sn, Iterator* parent_iter);
	void mergeDownNode(NodeStatus* children_status, InternalSkeletonNode* parent_sn, Iterator* parent_iter, Iterator* child_iter);
	void moveDownDiskData(NodeStatus* oversize_nodes, InternalSkeletonNode* skeletonNode);
	uint8_t getDiskNoToWrite();
    int getMoveDownCount(NodeStatus* node_status, bool largest_only);
};





#endif /* MEMMERGER_H_ */
