#include "MemIterator.h"

MemIterator::MemIterator(MapNode* rkey_node)
{
    if (rkey_node != NULL)
        rkeys_iter = rkey_node->getMapBegin();
    this->rkey_node = rkey_node;
}

KEY MemIterator::getKey()
{
    if (rkey_node == NULL)
        return -1;
    return rkeys_iter->first;
}
VALUE MemIterator::getValue()
{
    if (rkey_node == NULL)
        return -1;
    char* val_c = rkeys_iter->second;
    VALUE val;
    memcpy(&val, val_c, sizeof(VALUE));
    return val;
}

void MemIterator::moveNext()
{
    if (reachedEnd())
        return;
    ++rkeys_iter;
    no_rkeys_read++;
}

void MemIterator::getCurrLocation(uint64_t& curr_index, Pointer* curr_pointer, bool& has_pointer)
{
    has_pointer = false;
    curr_index = no_rkeys_read;
    
}

bool MemIterator::readAll()
{
    return reachedEnd();
}

bool MemIterator::reachedEnd()
{
    if (rkey_node == NULL)
        return true;
    if (rkeys_iter == rkey_node->getMapEnd())
        return true;
    if (rkeys_iter->second == NULL)
        return true;
    return false;
}

