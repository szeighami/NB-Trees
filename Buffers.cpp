#include "Buffers.h"

Buffers::Buffers(bool is_leaf, bool is_first_page, MemoryWriter* mem_writer, Pointer* file_loc)
{
    this->file_loc = new Pointer(file_loc);
    this->data = new unsigned char[PAGE_SIZE +4];
    offset = 0;
    no_elements = 0;
    this->val_size = MAX_VAL_SIZE;
    this->pointer_size = Pointer::getSize();
    this->is_leaf = is_leaf;
    this->mem_writer = mem_writer;
    this->upper_buffer = NULL;

    offset += Util::writeT<int>(&data[offset],-1);
}

Pointer* Buffers::writeBufferToDiksAndEmpty(Pointer* pointer)
{
    if (pointer != NULL)
    {
        offset += pointer->writePointerAt(&data[offset]);
    }
    else
    {
        //Remove the last key if no extra pointer is added to the end of the leaf
        if (!is_leaf)
        {
            int key_offset = offset - (int)sizeof(KEY) - Pointer::getSize();
            for (int i = 0; i < sizeof(KEY); i++)
                data[key_offset+ i] = data[key_offset + sizeof(KEY) + i];
            no_elements--;
        }
    }

	int begin_offset = 0;

	begin_offset += Util::writeNoElementsHasValue(&data[begin_offset], no_elements, is_leaf);

	mem_writer->getFile()->writePageAtCurr(file_loc, data, file_loc);
	mem_writer->increasePagesWrittenBy(1);

    if (!is_leaf)
    {
        InnerNode* node = new InnerNode(file_loc, data);
        cache->addPage(file_loc, node);
    }

    //delete[] data;
    //data = new unsigned char[PAGE_SIZE +4];
	offset = Util::writeT<int>(data,-1);
	no_elements = 0;
    return file_loc;
}


Pointer* Buffers::addToBuffer(KEY key, VALUE value, Pointer* pointer)
{
    Pointer* loc_written;
    if (is_leaf)
    {
        no_elements++;
        memcpy(&data[offset], &key, sizeof(KEY));
		offset += (int)sizeof(KEY);
        memcpy(&data[offset], &value, sizeof(VALUE));
        offset += val_size;
        if (offset + sizeof(KEY) + val_size < PAGE_SIZE)
            return NULL;
        loc_written = writeBufferToDiksAndEmpty(NULL);
    }
    else
    {
        bool is_last = offset + sizeof(KEY) + 2*pointer_size >= PAGE_SIZE;
        if (!is_last)
        {
            no_elements++;
            memcpy(&data[offset], &key, sizeof(KEY));
            offset += (int)sizeof(KEY);
            offset += pointer->writePointerAt(&data[offset]);
            return NULL;
        }
        loc_written =  writeBufferToDiksAndEmpty(pointer);
    }
    Pointer* root = NULL;
    if (upper_buffer == NULL)
    {
        root = loc_written;
        upper_buffer = new Buffers(false, false, mem_writer, file_loc);
    }
    Pointer* temp = upper_buffer->addToBuffer(key, -1, loc_written);
    if (root == NULL)
        root = temp;
        
    return root;
}


bool Buffers::getPointerToSearch(KEY key, VALUE& val, Pointer* p)
{
    bool search_this = true;
    if (upper_buffer != NULL)
        search_this = !upper_buffer->getPointerToSearch(key, val, p);

    if (search_this)
    {
        for (int i = 0; i < no_elements; i++)
        {
            int offset;
            if (is_leaf)
                offset = 4 + i*((int)sizeof(KEY)+val_size);
            else
                offset = 4 + i*((int)sizeof(KEY)+pointer_size);

            if (*(KEY*)&data[offset] >= key)
            {
                if (is_leaf)
                {
                    if (*(KEY*)&data[offset] == key)
                        val = *(VALUE*)&data[offset + sizeof(KEY)];
                    else
                        val = -1;
                    return val != -1;
                }
                Pointer* return_pointer = new Pointer(&data[offset + sizeof(KEY)]);
                p->copyPointer(return_pointer);
                delete return_pointer;
                val = -1;
                return true;
            }
        }
        val = -1;
        return false;
    }
    else
    {
        return true;
    }
}
Pointer* Buffers::writeRemainingBuffers(Pointer* pointer)
{
    if (isEmpty() && pointer == NULL)
        return NULL;
    return writeBufferToDiksAndEmpty(pointer);
}

