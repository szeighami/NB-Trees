#include "DiskIterator.h"


void DiskIterator::preLoad(Pointer* from_pointer)
{
    unsigned char* buffer = new unsigned char[rnode_size];
    bool endOfFile = file->readNodeFromFile(from_pointer, buffer, remove_from_cache) == 0;
    this->no_pages_read++;
    long loc = from_pointer->getLocation();

    preread_all = true;
    read_buffer_size = 0;
    read_buffer_curr = 0;
    if (endOfFile)
        return;

    Time* t = new Time;
    Util::startTime(t);
    //cout << "Reading" << endl;

    delete[] read_buffer;
    delete[] read_buffer_loc;
    read_buffer = new unsigned char*[read_buffer_cap];
    read_buffer_loc = new long[read_buffer_cap];
    int f = file->getFile();
    Pointer* p = new Pointer;
    file->getCurrLocation(p);
    uint64_t bytes_read = 0;
	while ((*(long*)buffer != -1 || *(long*)&buffer[8] != -1) && (max_pages_to_read == -1 || no_pages_read <= max_pages_to_read+2))
	{
        //Node is leaf
		if (*(int*)buffer > 0)
        {
            read_buffer_loc[read_buffer_size] = loc;
            read_buffer[read_buffer_size++] = buffer;
            buffer = new unsigned char[rnode_size];
            if (read_buffer_size == read_buffer_cap)
            {
                preread_all = false;
                break;
            }
            if (max_key_to_read != 0 &&  *(KEY*)&buffer[4] > max_key_to_read)
                break;
        }
        else if (remove_from_cache)
        {
            p->setLocation(loc);
            cache->removePage(p);
        }

        //auto fut = std::async(std::launch::async, read, f, buffer, rnode_size);
        //while(fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready);
        //fut.wait();
        //bytes_read += fut.get();
        bytes_read += read(f, buffer, rnode_size);
        loc += rnode_size;
        this->no_pages_read++;
    }
    //cout << "TOOK" << Util::getTimeElapsed(t) << " READ " << bytes_read << endl;
    delete t;
    delete[] buffer;
    delete p;

}
//set max_pages_to_read to -1 not to impose a max
DiskIterator::DiskIterator(Pointer* begin_pointer, uint64_t begin_index, bool pointer_is_first_page, File* file, uint64_t rnode_size, long max_pages_to_read, bool has_tree, bool remove_from_cache, uint64_t read_ahead_size)
{
    this->has_tree = has_tree;
    this->rnode_size = rnode_size;
    this->max_pages_to_read = max_pages_to_read;
    this->remove_from_cache = remove_from_cache;

    this->file = file;
    this->curr_index = (int)begin_index;
	curr_page = NULL;
	page_raw = NULL;
    endOfTree = !has_tree;
    curr_pointer = new Pointer;
    file->getCurrLocation(curr_pointer);
    this->read_buffer_cap = read_ahead_size;
    if (this->read_buffer_cap == 0)
    {
        this->read_buffer_size = 0;
        this->read_buffer_curr = 0;
    }
    this->read_buffer = NULL;
    this->read_buffer_loc = NULL;
    this->max_key_to_read = 0;
	getNextLeaf(pointer_is_first_page, begin_pointer);
}

DiskIterator::DiskIterator(Pointer* begin_pointer, uint64_t begin_index, InnerNode* curr_node, File* file, uint64_t rnode_size, long max_pages_to_read, bool has_tree, bool remove_from_cache, uint64_t read_ahead_size, KEY max_key_to_read)
{
    this->has_tree = has_tree;
    this->rnode_size = rnode_size;
    this->max_pages_to_read = max_pages_to_read;

    this->file = file;
    this->curr_index = (int)begin_index;
	curr_page = curr_node;
    endOfTree = !has_tree;
    curr_pointer = new Pointer(begin_pointer);
    this->remove_from_cache = remove_from_cache;
    this->read_buffer_cap = read_ahead_size;
    this->max_key_to_read = max_key_to_read;
    this->read_buffer_size = 0;
    this->read_buffer_curr = 0;
    this->read_buffer = NULL;
    this->read_buffer_loc = NULL;
	this->page_raw = NULL;

    if (curr_index == curr_page->getNoElements())
    {
        getNextLeaf(false, NULL);
        this->curr_index = 0;
    }
}
DiskIterator::~DiskIterator()
{
    for (uint64_t i = read_buffer_curr; i < read_buffer_size; i++)
        delete[] read_buffer[i];
    delete[] page_raw;
    delete[] read_buffer;
    delete[] read_buffer_loc;
    delete curr_pointer;
}

void DiskIterator::getInfo(KEY& key, VALUE& val, bool& end, bool should_move)
{
    if (should_move)
        moveNext();
    end = reachedEnd();
    if (end)
    {
        key = -1;
        val = -1;
        return;
    }
    if (curr_page == NULL)
    {
        key = *(KEY*)&page_raw[key_offset];
        val = *(VALUE*)&page_raw[key_offset + sizeof(KEY)];
        return;
    }
    key = curr_page->getKeyAt(curr_index);
    val = curr_page->getValueAt(curr_index);
}


VALUE DiskIterator::getValue()
{
    if (reachedEnd())
        return -1;
    if (curr_page == NULL)
        return *(VALUE*)&page_raw[key_offset + sizeof(KEY)];
    return curr_page->getValueAt(curr_index);
}

KEY DiskIterator::getKey()
{
    if (reachedEnd())
        return -1;

    if (curr_page == NULL)
        return *(KEY*)&page_raw[key_offset];
    return curr_page->getKeyAt(curr_index);
}

void DiskIterator::moveNext()
{
    if (reachedEnd())
        return;
    ++no_rkeys_read;
    if (curr_page == NULL)
    {
        if (curr_index < no_elements - 1)
        {
            key_offset += (int)sizeof(KEY) + MAX_VAL_SIZE;
            curr_index++;
            return;
        }
    }
    else
    {
        if (curr_index < curr_page->getNoElements()-1)
        {
            curr_index++;
            return;
        }
    }

    getNextLeaf(false, NULL);
    curr_index = 0;

} 
bool DiskIterator::readAll()
{
    return endOfTree;
}
bool DiskIterator::reachedEnd()
{
    if (endOfTree)
        return true;

    if (max_pages_to_read != -1 && no_pages_read > max_pages_to_read)
        return true;

    return false;
}
void DiskIterator::getCurrLocation(uint64_t& curr_index, Pointer* curr_pointer, bool& has_pointer)
{
    has_pointer = true;
    curr_pointer->copyPointer(this->curr_pointer);
    curr_pointer->setLocation(this->curr_pointer->getLocation() - rnode_size);
    curr_index = this->curr_index;
}
void DiskIterator::moveTo(Pointer* begin_pointer, uint64_t begin_index, bool pointer_is_first_page, bool has_tree)
{
    endOfTree = !has_tree;
    resetCounters();
    this->curr_index = (int)begin_index;
    if (curr_pointer->getLocation() == begin_pointer->getLocation())
        getNextLeaf(pointer_is_first_page, NULL);
    else
        getNextLeaf(pointer_is_first_page, begin_pointer);

}


bool DiskIterator::getNextLeaf(bool is_first, Pointer* from_pointer)
{
    if (reachedEnd())
        return false;
    if (from_pointer != NULL)
        curr_pointer->copyPointer(from_pointer);
    if (read_buffer_cap != 0)
    {
        if (from_pointer != NULL)
            preLoad(from_pointer);
        if (read_buffer_curr == read_buffer_size && ! preread_all)
            preLoad(curr_pointer);
        if (read_buffer_curr != read_buffer_size)
        {
			if (curr_page != NULL)
                curr_page = NULL;
            delete[] page_raw;
            curr_pointer->setLocation(read_buffer_loc[read_buffer_curr] + rnode_size);
            page_raw = read_buffer[read_buffer_curr];
            read_buffer_curr++;
            no_elements = *(int*)page_raw;
            key_offset = sizeof(int);
            return true;
        }
        endOfTree = true;
        return false;
    }

	unsigned char* buffer = new unsigned char[rnode_size+4];
	uint64_t offset = 0;
	uint64_t pages_read = 1;
    this->no_pages_read++;


    bool endOfFile = false;
    if (from_pointer != NULL)
    {
        endOfFile = file->readNodeFromFile(curr_pointer, buffer, remove_from_cache) == 0;
    }
    else
        endOfFile = file->readNodeAtCurr(buffer, remove_from_cache) == 0;

    if (endOfFile)
    {
        delete[] buffer;
        curr_pointer->setLocation(curr_pointer->getLocation() + pages_read*rnode_size);
        endOfTree = true;
        return false;
    }


	while (pageInTree(&buffer[offset]))
	{
		if (isLeaf(&buffer[offset]))
		{
			if (curr_page != NULL)
                curr_page = NULL;
            delete[] page_raw;
            page_raw = buffer;
            no_elements = *(int*)page_raw;
            key_offset = sizeof(int);
			curr_pointer->setLocation(curr_pointer->getLocation() + (pages_read)*rnode_size);
			return true;
		}

        pages_read++;
        this->no_pages_read++;
		if (file->readNodeAtCurr(buffer, remove_from_cache) == 0)
		{
			delete[] buffer;
			curr_pointer->setLocation(curr_pointer->getLocation() + pages_read*rnode_size);
            endOfTree = true;
			return false;
		}
		offset=0;
	}

	delete[] buffer;
	curr_pointer->setLocation(curr_pointer->getLocation() + pages_read*rnode_size);
    endOfTree = true;
	return false;
}

bool DiskIterator::pageInTree(unsigned char* page_data)
{
    return *(long*)page_data != -1 || *(long*)&page_data[8] != -1;
}


bool DiskIterator::isLeaf(unsigned char* page_data)
{
        return *(int*)page_data > 0;
}
