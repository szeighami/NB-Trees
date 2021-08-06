#include "InnerNode.h"

InnerNode::InnerNode()
{
	//this->keys = NULL;
	//this->values = NULL;
	this->no_elements = 0;
	this->is_leaf = true;
	//this->pointers = NULL;
	this->disk_location = NULL;
}


int InnerNode::getKeyIndex(KEY key, int begin, int end)
{
	if (begin == end)
		return begin;

	int mid_index = begin + (end - begin)/2;
    int mid_offset;
    if (is_leaf)
        mid_offset = 4 + mid_index*((int)sizeof(KEY)+val_size);
    else
        mid_offset = 4 + mid_index*((int)sizeof(KEY)+pointer_size);

    //cout << "KEY: " << key << endl;
    //cout << *(KEY*)&raw_data[mid_offset] << endl;

	if (*(KEY*)&raw_data[mid_offset] == key)
		return mid_index;

	if (*(KEY*)&raw_data[mid_offset] > key)
		return getKeyIndex(key, begin, mid_index);

	return getKeyIndex(key, mid_index + 1, end);
}

Pointer* InnerNode::getPointerAt(int i)
{
    if (is_leaf)
        return NULL;
    if (i <= no_elements && i >=0)
    {
        delete return_pointer;
        if (i < no_elements)
            return_pointer = new Pointer(&raw_data[4 + i*(sizeof(KEY)+pointer_size) + sizeof(KEY)]);
        else
            return_pointer = new Pointer(&raw_data[4 + i*(sizeof(KEY)+pointer_size)]);
        return return_pointer;
    }
    return 0;
}

void InnerNode::loadNode(unsigned char* buffer, Pointer* p)
{
	this->disk_location = new Pointer(p);
    this->pointer_size = Pointer::getSize();
    this->val_size = MAX_VAL_SIZE;
    return_pointer = NULL;

    bool no_node = true;
    for (int i = 0; i < 16; i++)  
    {
        if (buffer[i] != 255)
        {
            no_node = false;
            break;
        }
    }
    if (no_node)
    {
        this->is_leaf = true;
        this->no_elements = 0;
        this->raw_data = NULL;
        return;
    }
        

	int offset = 0;
    this->no_elements = Util::readT<int>(buffer);
	offset += 4;
    if (no_elements > 0)
        this->is_leaf = true;
    else
    {
        this->is_leaf = false;
        this->no_elements = -1*this->no_elements;
    }


	//this->keys = new KEY[no_elements];
	//if (this->is_leaf)
	//	this->values = new VALUE[no_elements];
	//else
	//	this->values = NULL;

	//if (!this->is_leaf)
	//	this->pointers = new Pointer*[no_elements+1];
	//else
	//	this->pointers = NULL;

	//offset += fillNode(&buffer[offset], key_to_find, index);
}


InnerNode::InnerNode(Pointer* p, File* file, bool read_at_curr)
{
    unsigned char* buffer = new unsigned char[PAGE_SIZE+4];
    if (read_at_curr)
        file->readNodeAtCurr(buffer, false);
    else
        file->readNodeFromFile(p, buffer, false);

    loadNode(buffer, p);
    this->raw_data = buffer;

}

InnerNode::InnerNode(Pointer* p, unsigned char* buffer)
{
    loadNode(buffer, p);
    this->raw_data = new unsigned char[PAGE_SIZE];
    memcpy(this->raw_data, buffer, PAGE_SIZE);
}


InnerNode::InnerNode(unsigned char* buffer)
{
    loadNode(buffer, new Pointer(0, 0, 0));
    this->raw_data = new unsigned char[PAGE_SIZE];
    memcpy(this->raw_data, buffer, PAGE_SIZE);
}

/*bool InnerNode::nodeIsLeaf(unsigned char* buffer)
{
	int no_elements = Util::readT<int>(buffer);
    if (no_elements > 0)
        return true;
    return false;
}*/

InnerNode::~InnerNode()
{
    delete[] raw_data;
	delete disk_location;
    delete return_pointer;
}

