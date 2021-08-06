/*
 *
 * Taken from https://github.com/jvirkki/libthis/blob/master/this.c
 */

#ifndef CACHE_H_
#define CACHE_H_
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <cstring>
#include <map>
#include <vector>
#include "Pointer.h"
#include "File.h"
#include "InnerNode.h"

class Pointer;
class File;
class InnerNode;

extern uint32_t PAGE_SIZE;
struct pages;
struct cmp_ptr
{
   bool operator()(Pointer* a, Pointer* b);
};

class Cache{
public:
    Cache(){head = NULL; page_size = 0; file_size=0;}//unsigned long PAGE_SIZE){this->PAGE_SIZE = 4096;}
    ~Cache();    

    void addPage(Pointer* pointer, InnerNode* page);
    void removePage(Pointer* pointer);
    bool getPageIfExists(Pointer* pointer, InnerNode*& page);
    uint64_t getCachedPagesSize(){return page_size;}
    uint64_t getCachedFilesSize(){return file_size;}
       
    void addFile(Pointer* pointer, File* page);
    void getFileOrAdd(Pointer* pointer, File*& page, int node_size);
    void removeFile(Pointer* file_pointer);
    void removeAllFiles();
    void deleteRedundantPages();
private:
    std::map<Pointer*, InnerNode*, cmp_ptr> directory;	
    std::map<Pointer*, File*, cmp_ptr> file_directory;	

    pages* head;	
    //unsigned long PAGE_SIZE;
    uint64_t page_size;
    uint64_t file_size;

};






#endif /* Cache_H_ */
