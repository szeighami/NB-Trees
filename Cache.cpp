#include "Cache.h"

struct pages
{
	Pointer* loc;
    pages* next;
    ~pages(){delete loc;}
};

bool cmp_ptr::operator()(Pointer* a, Pointer* b)
{
   if (a->getDisk() != b->getDisk())
       return a->getDisk() < b->getDisk();

   if (a->getFileNo() != b->getFileNo())
       return a->getFileNo() < b->getFileNo();

   return a->getLocation() < b->getLocation();
}

void Cache::addPage(Pointer* p, InnerNode* page)
{
    page_size += sizeof(Pointer) + sizeof(InnerNode) + PAGE_SIZE; 
    Pointer* copy_pointer = new Pointer(p);
    directory.insert(std::pair<Pointer*, InnerNode*>(copy_pointer, page));
}

void Cache::deleteRedundantPages()
{
    vector<Pointer*> p_temp;
    vector<InnerNode*> n_temp;

	for(pages* iter = head; iter != NULL;)
	{
        std::map<Pointer*, InnerNode*>::iterator result = directory.find(iter->loc);
        
        if (result != directory.end())
        {
            Pointer* p = result->first;
            InnerNode* n = result->second;
            directory.erase(result);
            delete p;
            delete n;
            page_size -= sizeof(Pointer) + sizeof(InnerNode) + PAGE_SIZE; 
        }

        page_size -= sizeof(pages)+ sizeof(Pointer); 
		pages* temp  = iter->next;
		delete iter;
		iter = temp;
        usleep(100);
	}
	head = NULL;
}

void Cache::removePage(Pointer* p)
{
    page_size += sizeof(pages)+ sizeof(Pointer); 
	pages* new_page = new pages;
	new_page->loc = new Pointer(p);
	new_page->next = head;
	head = new_page;
}

Cache::~Cache()
{
    deleteRedundantPages();
    std::map<Pointer*, InnerNode*>::iterator it = directory.begin();

    for (;it != directory.end(); it++)
    {
        delete it->first;
        delete it->second;
        page_size -= sizeof(Pointer) + sizeof(InnerNode) + PAGE_SIZE; 
    }

    removeAllFiles();
}

  
bool Cache::getPageIfExists(Pointer* pointer, InnerNode*& page)
{
    Pointer* copy_pointer = new Pointer(pointer);

    std::map<Pointer*, InnerNode*>::iterator result = directory.find(copy_pointer);

    delete copy_pointer;

    if (result == directory.end())
        return false;
    page = result->second;
    return true;


} 

void Cache::addFile(Pointer* pointer, File* file)
{
    static int count = 0;
    //cout << "added: " << count++ << endl;
    Pointer* copy_pointer = new Pointer(0, pointer->getFileNo(), pointer->getDisk());
    file_directory.insert(std::pair<Pointer*, File*>(copy_pointer, file));
}

void Cache::getFileOrAdd(Pointer* pointer, File*& file, int node_size)
{
    Pointer* copy_pointer = new Pointer(0, pointer->getFileNo(), pointer->getDisk());

    std::map<Pointer*, File*>::iterator result = file_directory.find(copy_pointer);

    delete copy_pointer;

    if (result != file_directory.end())
    {
        file = result->second;
        return;
    }
    
    if (file_directory.size() > 1000)
    {
        std::map<Pointer*, File*>::iterator it = file_directory.begin();
        Pointer* res_pointer = it->first;
        File* res_file  = it->second;
        file_directory.erase(res_pointer);
        file_size -= sizeof(File) + sizeof(Pointer); 
        delete res_pointer;
        delete res_file;
    }

    file_size += sizeof(File) + sizeof(Pointer); 
    file = new File(pointer, false, node_size, false);
    addFile(pointer, file);
    return;
    
}    
void Cache::removeAllFiles()
{
    std::map<Pointer*, File*>::iterator it = file_directory.begin();

    for (;it != file_directory.end(); it = file_directory.begin())
    {
        file_size -= sizeof(File) + sizeof(Pointer); 
        Pointer* res_pointer = it->first;
        File* res_file  = it->second;
        file_directory.erase(res_pointer);
        delete res_pointer;
        delete res_file;
    }
}

void Cache::removeFile(Pointer* file_pointer)
{

    auto it = file_directory.find(file_pointer);
    
    if (it != file_directory.end())
    {
        file_size -= sizeof(File) + sizeof(Pointer); 
        Pointer* res_pointer = it->first;
        File* res_file  = it->second;
        file_directory.erase(res_pointer);
        delete res_pointer;
        delete res_file;
    }
}






