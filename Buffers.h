/*
 * BPTree.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef BUFFERS_H_
#define BUFFERS_H_
#include "BPTree.h"
#include "NBTree.h"
#include "Merger.h"

class Record;
class BPTree;
class NBTree;
class MemIterator;
class MemoryWriter;

extern uint32_t MAX_VAL_SIZE;
extern uint32_t PAGE_SIZE;
extern Cache* cache;

class Buffers
{
public:
	Buffers(bool is_leaf, bool is_first_page, MemoryWriter* mem_writer, Pointer* file_loc);

	~Buffers()
	{
        delete upper_buffer;
		delete[] data;
        delete file_loc;
	}

	bool isEmpty()
	{
        return offset == sizeof(int);
	}

	bool getPointerToSearch(KEY key, VALUE& val, Pointer* p);
	Pointer* writeRemainingBuffers(Pointer* pointer);
	Pointer* addToBuffer(KEY key, VALUE value, Pointer* pointer);
    Buffers* getUpperBuffer(){return upper_buffer;}

private:
    Pointer* writeBufferToDiksAndEmpty(Pointer* pointer);

	bool is_leaf;
	unsigned char* data;
	int offset;
	int no_elements;
	int val_size;
	int pointer_size;
    Pointer* file_loc;

    Buffers* upper_buffer;

	MemoryWriter* mem_writer;
};






#endif 
