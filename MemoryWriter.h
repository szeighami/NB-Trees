/*
 * BPTree.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef MEMORYWRITER_H_
#define MEMORYWRITER_H_
//#include "Util.h"
//#include "Pointer.h"
#include "Buffers.h"
#include "BPTree.h"
#include "NBTree.h"
#include "Merger.h"
#include <vector>
//#include "File.h"

class Record;
//class File;
class InternalBPTree;
class BPTree;
class NBTree;
class MemIterator;
class MemoryWriter;

extern uint32_t PAGE_SIZE;

class Buffers;

class MemoryWriter
{
public:
	MemoryWriter(NBTree* tree, Pointer* write_at_disk, bool is_root);
	void moveToNextChild(Pointer* root, count_t& node_size_written, Pointer* first_node, count_t& no_elements_written);
	void finishChildren();
	~MemoryWriter();
	void write(KEY key, VALUE value);
	count_t getNoPagesWritten(){return no_pages_written;}
	count_t getNoElementsWritten(){return no_elements_written;}
	void increasePagesWrittenBy(count_t value);

	File* getFile() {return file;}
	count_t diskWriting(){return write_at_disk->getDisk();}


    void increaseNoElements(){no_elements_written++;}
    Buffers* getBuffer(){return buffers;}
    void setRoot(Pointer* root){this->root->copyPointer(root);}
    void getRoot(Pointer* root){root->copyPointer(this->root);}



private:
	Pointer* first_node;
	Pointer* root;
	Pointer* write_at_disk;
	Buffers* buffers;
	int buffers_size;
	File* file;
	bool is_outer_leaf;
	count_t no_pages_written;
	count_t no_elements_written;
	NBTree* tree;


};






#endif /* BPTREE_H_ */
