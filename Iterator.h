#ifndef ITERATOR_H_
#define ITERATOR_H_

#include "Util.h"

class Iterator
{
public:
	Iterator(){no_pages_read = 0; no_rkeys_read = 0;}
	virtual ~Iterator(){}

	virtual VALUE getValue() = 0;
	virtual KEY getKey() = 0;
	virtual void getInfo(KEY& key, VALUE& val, bool& end, bool should_move) = 0;

	virtual bool reachedEnd() = 0;
	virtual bool readAll() = 0;
	virtual void moveNext() = 0;
	virtual void moveTo(Pointer* begin_pointer, uint64_t begin_index, bool pointer_is_first_page, bool has_tree) = 0;
    virtual void getCurrLocation(uint64_t& curr_index, Pointer* curr_pointer, bool& has_pointer) = 0;

    unsigned long noPagesRead(){return no_pages_read;}
    unsigned long noRKeysRead(){return no_rkeys_read;}
    void resetCounters(){no_pages_read = 0; no_rkeys_read = 0;}

protected:
    unsigned long no_pages_read; 
    unsigned long no_rkeys_read; 
};


#endif /*  ITERATOR_H_ */
