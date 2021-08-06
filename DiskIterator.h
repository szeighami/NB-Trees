/*
 * MemMerger.h
 *
 *  Created on: 6 Feb 2018
 *      Author: sepanta
 */

#ifndef DISKITERATOR_H_
#define DISKITERATOR_H_
#include "Iterator.h"
#include  <future>
#include "NBTree.h"

class NBTree;

extern uint32_t MAX_VAL_SIZE;
extern Cache* cache;

class DiskIterator: public Iterator
{
public:
    //TODO: shouldn't be passing the entrie NBTree
    //set read_ahead_size to 0 to turn off
	DiskIterator(Pointer* begin_pointer, uint64_t begin_index, bool pointer_is_first_page, File* file, uint64_t rnode_size, long max_pages_to_read, bool has_tree, bool remove_from_cache, uint64_t read_ahead_size);
    DiskIterator(Pointer* begin_pointer, uint64_t begin_index, InnerNode* curr_node, File* file, uint64_t rnode_size, long max_pages_to_read, bool has_tree, bool remove_from_cache, uint64_t read_ahead_size, KEY max_key_to_read);
    ~DiskIterator();

	VALUE getValue();
	KEY getKey();

	void getInfo(KEY& key, VALUE& val, bool& end, bool should_move);

    //Reached end of the current snode or max read
	bool reachedEnd();
	bool readAll();
	void preLoad(Pointer* from_pointer);
	void moveNext();
	void moveTo(Pointer* begin_pointer, uint64_t begin_index, bool pointer_is_first_page, bool has_tree);
    void getCurrLocation(uint64_t& curr_index, Pointer* curr_pointer, bool& has_pointer);


private:
    bool getNextLeaf(bool is_first, Pointer* from_pointer);
    inline bool pageInTree(unsigned char* page_data);
    inline bool isLeaf(unsigned char* page_data);
	
    unsigned char** read_buffer;
    long* read_buffer_loc;
    uint64_t read_buffer_size;
    uint64_t read_buffer_curr;
    uint64_t read_buffer_cap;

    unsigned char* page_raw;
    int key_offset;
    int no_elements;

    File* file;
    InnerNode* curr_page;
    Pointer* curr_pointer;
    int curr_index;
    bool endOfTree;
    bool preread_all;

    long max_pages_to_read;
    KEY max_key_to_read;
    uint64_t rnode_size;


    bool has_tree;
    bool dont_del_page;
    bool remove_from_cache;
};




#endif /*  DISKITERATOR_H_ */
