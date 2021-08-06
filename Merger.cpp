/*
 * Merger.cpp
 *
 *  Created on: 6 Feb 2018
 *      Author: sepanta
 */
#include "Merger.h"

uint8_t Merger::getDiskNoToWrite()
{
    return 1;
}

void Merger::splitOversizedLeaf(InternalSkeletonNode* parent_sn, InternalSkeletonNode* child_sn, int child_index, KEY child_key)
{
    InternalSkeletonNode* child_sn2 = new InternalSkeletonNode(child_sn);
    parent_sn->setLock();
    parent_sn->insert(child_key, child_sn);
    parent_sn->updatePointer(child_index+1, child_sn2);
    parent_sn->unsetLock();

}

bool Merger::moveParentToChild(Iterator* child_iter, Iterator* parent_iter, InternalSkeletonNode* child_sn, InternalSkeletonNode* parent_sn, MemoryWriter* mem_writer, int curr_child_index)
{
    int child_sn_no_keys = child_sn->getNoElements();
    int parent_sn_no_keys = parent_sn->getNoElements();
    KEY parent_sn_key = -1;
    if (curr_child_index!=parent_sn_no_keys)
        parent_sn_key = parent_sn->getKeyAt(curr_child_index);
    unsigned long child_sn_no_pages = child_sn->getNoPages();
    unsigned long child_sn_no_rkeys = child_sn->getNoRKeys();

    bool child_is_leaf = child_sn_no_keys == 0;
    KEY parent_key; VALUE parent_val; bool parent_end;
    KEY child_key; VALUE child_val; bool child_end;
    parent_iter->getInfo(parent_key, parent_val, parent_end, false);
    child_iter->getInfo(child_key, child_val, child_end, false);
    BloomFilter* bloom = child_sn->getBloomFilter();
    Buffers* buffer = mem_writer->getBuffer();
    bool only_largest = tree->mergeLargestOnly();
    int size_index = 0;
    uint64_t prev_size = 0;
    uint64_t prev_element_counts = 0;
    uint64_t* sizes = child_sn->getSizes();
    uint64_t* element_counts = child_sn->getElementCounts(); 
    KEY key;
    if (!child_is_leaf)
       key = child_sn->getKeyAt(0);
    while (true)
    {
        bool parent_remaining =  (!parent_end && (curr_child_index==parent_sn_no_keys|| parent_key <= parent_sn_key));
        bool child_remaining = !child_end && (curr_child_index==parent_sn_no_keys|| child_key <= parent_sn_key);
        if (!child_remaining && !parent_remaining)
        {
            if (!child_is_leaf)
            {
                sizes[size_index] = mem_writer->getNoPagesWritten() - prev_size;
                element_counts[size_index] = mem_writer->getNoElementsWritten() - prev_element_counts;
            }
            return false;
        }

        if ((!child_remaining) || (child_remaining && parent_remaining && child_key > parent_key))
        {
            //if (child_key != parent_key)
           // {
            mem_writer->increaseNoElements();
            Pointer* root = buffer->addToBuffer(parent_key, parent_val, NULL);
            if (root)
                mem_writer->setRoot(root);
            if (bloom)
                bloom->addElement(parent_key);
            //}
            //else
            //{
                //TODO: deletions
            //}
            parent_iter->getInfo(parent_key, parent_val, parent_end, true);
        }
        else
        {
            mem_writer->increaseNoElements();
            if (!child_is_leaf && key < child_key)
            {
                sizes[size_index] = mem_writer->getNoPagesWritten() - prev_size;
                element_counts[size_index] = mem_writer->getNoElementsWritten() - prev_element_counts;
                prev_size = sizes[size_index];
                prev_element_counts = element_counts[size_index];
                size_index++;
                if (size_index < child_sn_no_keys)
                    key = child_sn->getKeyAt(size_index);
                else
                    key = LONG_MAX;
            }
            if (child_key != parent_key)
            {
                Pointer* root = buffer->addToBuffer(child_key, child_val, NULL);
                if (root)
                    mem_writer->setRoot(root);
                if (bloom)
                    bloom->addElement(child_key);
            }
            uint64_t split_size;
            if (only_largest)
                split_size = snode_size / 2;
            else
                split_size = snode_size;
            bool should_split = child_is_leaf && child_sn_no_pages > split_size && child_iter->noRKeysRead() == child_sn_no_rkeys/2;
            //bool should_split = child_is_leaf && (child_sn_no_pages+parent_sn->getSizesAt(curr_child_index) > snode_size) && child_iter->noRKeysRead() == (child_sn_no_rkeys+parent_sn->getElementCountsAt(curr_child_index))/2;
            if (should_split)
            {
                splitOversizedLeaf(parent_sn, child_sn, curr_child_index, child_key);
                child_iter->moveNext();
                return true;
            }
            child_iter->getInfo(child_key, child_val, child_end, true);
        }
    }
}

bool Merger::splitChildBySKeys(InternalSkeletonNode* parent_sn,InternalSkeletonNode*& child_sn, int child_index)
{
    if (child_sn->getNoElements() + 1 <= tree->getFanout())
        return false;


    InternalSkeletonNode* sn_1 = new InternalSkeletonNode(child_sn);
    //THIS ALSO POINTS TO THE BEGINING OF THE PREV CHILD
    InternalSkeletonNode* sn_2 = new InternalSkeletonNode(child_sn);
    InternalSkeletonNode* curr_sn = sn_1;

    int no_skeys = child_sn->getNoElements();
    int split_index = no_skeys/2;
    for (int i = 0; i < no_skeys; i++)
    {
        if (i == split_index)
        {
            curr_sn->updatePointer(i, (InternalSkeletonNode*)child_sn->getPointerAt(i));
            curr_sn = sn_2; 
            continue;
        }

        curr_sn->insert(child_sn->getKeyAt(i), (InternalSkeletonNode*)child_sn->getPointerAt(i));
    }
    curr_sn->updatePointer(curr_sn->getNoElements(), (InternalSkeletonNode*)child_sn->getPointerAt(no_skeys));

    parent_sn->setLock();
    parent_sn->insert(child_sn->getKeyAt(split_index), sn_1);
    parent_sn->updatePointer(child_index+1, sn_2);
    parent_sn->unsetLock();
    child_sn->setLock();
    delete child_sn;

    child_sn = sn_1;


    return true;
}

void Merger::finishWritingChild(MemoryWriter* mem_writer, NodeStatus* children_status, int curr_child_index, InternalSkeletonNode* child_sn, uint64_t total_child_size)
{
    Pointer* root = new Pointer(0, 0, 1);
    Pointer* child_first_page = new Pointer(0, 0, 1);
    count_t no_pages_written, no_rkeys_written;
    mem_writer->moveToNextChild(root, no_pages_written, child_first_page, no_rkeys_written);
    child_sn->writeBloomFilter();
    child_sn->setLock();
    child_sn->updateDiskInfo(child_first_page, root, no_rkeys_written, no_pages_written, true, 0, true);
    child_sn->unsetLock();

    if (no_pages_written > snode_size)
        children_status->addOversizedNode(curr_child_index, no_pages_written);
    delete root;
    delete child_first_page;
}

void Merger::updateParentInfo(InternalSkeletonNode* parent_sn, Iterator* parent_iter)
{
    if (parent_iter->readAll())
        parent_sn->updateDiskInfo(NULL, NULL, 0, 0, false, 0, false); 
    else
    {
        uint64_t index; bool has_pointer; Pointer* p = new Pointer(0, 0, 1);
        parent_iter->getCurrLocation(index, p, has_pointer);
        if (has_pointer)
            parent_sn->updateDiskInfo(p, NULL, parent_sn->getNoRKeys() - parent_iter->noRKeysRead(), parent_sn->getNoPages() - parent_iter->noPagesRead(), true, index, false);
        delete p;
    }
}

//set child_iter = NULL only when child is on disk
void Merger::mergeDownNode(NodeStatus* children_status, InternalSkeletonNode* parent_sn, Iterator* parent_iter, Iterator* child_iter)
{
    int init_no_siblings = parent_sn->getNoElements() + 1;
    InternalSkeletonNode* child_sn = (InternalSkeletonNode*)parent_sn->getPointerAt(0);
	MemoryWriter* mem_writer = new MemoryWriter(tree, new Pointer(0, 0, getDiskNoToWrite()), false);
    Pointer* begin_pointer = new Pointer(child_sn->getBeginPointer());

    bool child_on_disk = child_iter == NULL;
    File* file = NULL;
    
    if (child_iter != NULL)
    {
        if (parent_sn->getNoElements() != 0)
            throw std::invalid_argument("Not Implemented");
    }
    else
    {
        file = new File(begin_pointer, false, rnode_size, false);
        child_iter = new DiskIterator(begin_pointer, child_sn->getBeginIndex(), child_sn->treeStartsFromBegin(), file, rnode_size, -1, child_sn->hasTree(), true, buffer_size);
    }

    uint64_t total_child_size = parent_sn->getNoPages();
    if (total_child_size > snode_size)
        total_child_size = snode_size;
    for (int curr_child_index = 0; curr_child_index <= parent_sn->getNoElements(); total_child_size += parent_sn->getPointerAt(curr_child_index++)->getNoPages());

    for (int curr_child_index = 0; curr_child_index <= parent_sn->getNoElements(); curr_child_index++)
    {
        child_sn->resetBloomFilter();
        bool split_by_skeys = splitChildBySKeys(parent_sn, child_sn, curr_child_index);

        if (buffer_size != 0)
            mem_writer->getFile()->setDelayedWrite(buffer_size);
        bool split_by_size = moveParentToChild(child_iter, parent_iter, child_sn, parent_sn, mem_writer, curr_child_index);
        if (buffer_size != 0)
            mem_writer->getFile()->writeDelayed();

        finishWritingChild(mem_writer, children_status, curr_child_index, child_sn, total_child_size);

        if ((curr_child_index == parent_sn->getNoElements()))
        {
            parent_sn->setLock();
            updateParentInfo(parent_sn, parent_iter);
            parent_sn->unsetLock();
            break;
        }

        child_sn = (InternalSkeletonNode*)parent_sn->getPointerAt(curr_child_index+1);
        bool child_was_split = split_by_skeys || split_by_size;
        if (child_was_split)
            child_iter->resetCounters();
        else
            child_iter->moveTo(child_sn->getBeginPointer(), child_sn->getBeginIndex(), child_sn->treeStartsFromBegin(), child_sn->hasTree());
    }
    if (child_on_disk)
        tree->getFileManager()->moveFile(begin_pointer, init_no_siblings);
	tree->getFileManager()->addFile(child_sn->getRootPointer(), parent_sn->getNoElements()+1);

    delete child_iter;
    delete file;
    delete begin_pointer;
    delete mem_writer;
    child_iter = NULL;
    cache->getFileOrAdd(child_sn->getRootPointer(), file, rnode_size);
}



void Merger::merge(InternalSkeletonNode* parent_sn, MapNode* rkeys_to_merge, bool only_move_memory)
{
    struct rusage myTime_start;
    getrusage(RUSAGE_THREAD,&myTime_start);
    Time* t_merge = new Time;
    Util::startTime(t_merge);

    NodeStatus* children_status = new NodeStatus;
    if (only_move_memory)
    {
        Iterator* parent_iter = new MemIterator(NULL);
        Iterator* child_iter = new MemIterator(rkeys_to_merge);
        mergeDownNode(children_status, parent_sn, parent_iter, child_iter);
        delete parent_iter;
        delete children_status;
        tree->increaseMergeTime(Util::getTimeElapsed(t_merge));
        delete t_merge;
        return;
    }

    Iterator* parent_iter = new MemIterator(rkeys_to_merge);
    mergeDownNode(children_status, parent_sn, parent_iter, NULL);
    delete parent_iter;

    rkeys_to_merge->setLock();
    auto rkeys_iter = rkeys_to_merge->getMapBegin();
    for (; rkeys_iter != rkeys_to_merge->getMapEnd();rkeys_iter++)
    {
        delete[] rkeys_iter->second;
        rkeys_iter->second = NULL;
    }
    rkeys_to_merge->clear();
    rkeys_to_merge->unsetLock();


    if (children_status->hasOverSizedNodes())
		moveDownDiskData(children_status, parent_sn);
    delete children_status;
    tree->increaseMergeTime(Util::getTimeElapsed(t_merge));
    delete t_merge;
    struct rusage myTime_end;
    float  userTime, sysTime;
    getrusage(RUSAGE_THREAD,&myTime_end);
    Util::calculateExecutionTime(&myTime_start, &myTime_end, &userTime, &sysTime);
    tree->increaseCPUMergeTime(userTime);
    tree->increaseSysMergeTime(sysTime);
}

int Merger::getMoveDownCount(NodeStatus* node_status, bool largest_only)
{
	if (largest_only)
    {
        if (node_status->hasOverSizedNodes())
            return 1;
        return 0;
    }
	else
		return node_status->getOversizedCount();
}

void Merger::moveDownDiskData(NodeStatus* siblings_status, InternalSkeletonNode* parent_sn)
{
    //Oversized leaf nodes are corrected when merging down at the next iteration
    InternalSkeletonNode* os_node_sn = (InternalSkeletonNode*)parent_sn->getPointerAt(0);
    if (os_node_sn->isLeaf())
        return;


	bool only_largest = tree->mergeLargestOnly() && !tree->isMergeProbablistic();

	int move_down_count = getMoveDownCount(siblings_status, only_largest);

	for (int i = 0; i < move_down_count; i++)
	{
        int move_down_index;
        if (only_largest)
            move_down_index = siblings_status->getLargestIndex();
        else
            move_down_index = siblings_status->getOSNodeIndexAt(i);


        if (tree->isMergeProbablistic() && i > 0)
        {
            float p = 0.5;
            std::default_random_engine generator;
            std::uniform_real_distribution<double> distribution(0.0,1.0);
            double number = distribution(generator);
            if (number < p)
                continue;
        }

        InternalSkeletonNode* os_node_sn = (InternalSkeletonNode*)parent_sn->getPointerAt(move_down_index);
		NodeStatus* children_status = new NodeStatus;

        Pointer* begin_pointer = os_node_sn->getBeginPointer();
        //TODO: move file to inside iterator
        File* file = new File(begin_pointer, false, rnode_size, false);
        //uint64_t buffer_size = snode_size/(tree->getFanout()*2);
        //uint64_t buffer_size = (1L << 20)/rnode_size ;
        //if (buffer_size > snode_size/2)
        //    buffer_size = snode_size/2;
        Iterator* os_node_iter = new DiskIterator(begin_pointer, os_node_sn->getBeginIndex(), os_node_sn->treeStartsFromBegin(), file, rnode_size, 2*snode_size, os_node_sn->hasTree(), false, buffer_size);
        //Iterator* os_node_iter = new DiskIterator(begin_pointer, os_node_sn->getBeginIndex(), os_node_sn->treeStartsFromBegin(), file, rnode_size, snode_size, os_node_sn->hasTree(), false, buffer_size);

        mergeDownNode(children_status, os_node_sn, os_node_iter, NULL);
        delete os_node_iter;
        delete file;

		if (only_largest || children_status->hasOverSizedNodes())
			moveDownDiskData(children_status, os_node_sn);
        delete children_status;


	}
}



