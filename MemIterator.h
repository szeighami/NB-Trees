/*
 * MemMerger.h
 *
 *  Created on: 6 Feb 2018
 *      Author: sepanta
 */

#ifndef MEMITERATOR_H_
#define MEMITERATOR_H_
#include <stdexcept>
#include "btree/btree_map.h"
#include "MemoryManager.h"
#include "Iterator.h"

class MemoryManager;
class MapNode;
class Iterator;

class MemIterator: public Iterator
{
public:
	MemIterator(MapNode* rkey_node);
	~MemIterator(){}

	VALUE getValue();
	KEY getKey();

	void getInfo(KEY& key, VALUE& val, bool& end, bool should_move)
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
        key = rkeys_iter->first;
        char* val_c = rkeys_iter->second;
        if (val_c == NULL)
        {
            rkeys_iter++;
        }
        memcpy(&val, val_c, sizeof(VALUE));
    }
	bool reachedEnd();
	bool readAll();
	void moveNext();
	void moveTo(Pointer* begin_pointer, uint64_t begin_index, bool pointer_is_first_page, bool has_tree){throw std::invalid_argument("NOT IMPLEMENTED IN MEM MOVE TO");}
    void getCurrLocation(uint64_t& curr_index, Pointer* curr_pointer, bool& has_pointer);

    


private:
	
    btree_type::iterator rkeys_iter;
    MapNode* rkey_node;
};




#endif /*  MEMMERGER_H_ */
