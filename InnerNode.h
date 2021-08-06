/*
 * InnerNode.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef INNERNODE_H_
#define INNERNODE_H_
#include "KeyValueType.h"
#include "Util.h"

class Record;
class File;
class InternalRecord;
class Pointer;
class Tree;

extern uint32_t PAGE_SIZE;
extern uint32_t MAX_VAL_SIZE;


class InnerNode
{
public:
	InnerNode();
	InnerNode(Pointer* p, File* file, bool read_at_curr);
	InnerNode(Pointer* p, unsigned char* buffer);
	InnerNode(unsigned char* buffer);
	~InnerNode();


	int getKeyIndex(KEY key, int begin, int end);

	bool isLeaf(){return is_leaf;}
	int getNoElements(){return no_elements;}
	KEY getKeyAt(int i)
	{
		if (i <= no_elements && i >=0)
        {
            if (is_leaf)
                return *(KEY*)&raw_data[4 + i*(sizeof(KEY)+val_size)];
			return *(KEY*)&raw_data[4 + i*(sizeof(KEY)+pointer_size)];
        }
		return 0;
	}
	VALUE getValueAt(int i)
	{
		if (!is_leaf)
			return 0;
		if (i <= no_elements && i >=0)
			return *(VALUE*)&raw_data[4 + i*(sizeof(KEY)+val_size) + sizeof(KEY)];
		return 0;
	}
	Pointer* getPointerAt(int i);
	Pointer* getDiscLocation() {return disk_location;}

	bool hasValue() {return is_leaf;}


private:
    unsigned char* raw_data;
	int no_elements;
	int pointer_size;
	int val_size;
	bool is_leaf;
	Pointer* disk_location;
	Pointer* return_pointer;

    void loadNode(unsigned char* buffer, Pointer* p);
};




#endif /* INNERNODE_H_ */
