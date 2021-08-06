#include "BloomFilter.h"


void BloomFilter::writeToFile(bool keep_in_memory)
{
    bool dont_write = false;
    if (dont_write)
        return;

    if (file_pointer != NULL)
    {
        char* file_name = File::getFileNameFromPointer(file_pointer);
        unlink(file_name);
        delete[] file_name;
    }
    else
        file_pointer = new Pointer(0, 0, 1);

    File* file = new File(file_pointer, true, this->bytes, false);
    file->getCurrLocation(file_pointer);
    file_pointer->setLocation(0);

    file->writePageAtLoc(file_pointer, this->bf);

    //cache->addFile(file_pointer, file);
    delete file;

    if (!keep_in_memory)
    {

        //delete[] this->bf;
        free(this->bf);
        this->bf = NULL;
        bloom_size -= this->bytes;
    }
    
}

BloomFilter::~BloomFilter()
{
    if (this->bf != NULL)
        bloom_size -= this->bytes;
        free(this->bf);

    if (file_pointer)
    {
        char* file_name = File::getFileNameFromPointer(file_pointer);
        unlink(file_name);
        delete[] file_name;
        delete file_pointer;
    }
}

bool BloomFilter::exists(KEY key)
{
    int no_hits = 0;
    //register unsigned int a = murmurhash2(&key, sizeof(key), 0x9747b28c);
    //register unsigned int b = murmurhash2(&key, sizeof(key), 0x2396f38b);
    register unsigned long a;// = key;
    register unsigned long b;// = key*2;
    MurmurHash3_x64_128(&key, sizeof(key), 0x9747b28c, &a);
    MurmurHash3_x64_128(&key, sizeof(key), 0x248c893e, &b);
    register unsigned long x;
    register unsigned long i;

    File* file = NULL;
    if (this->bf == NULL)
    {
        file = new File(file_pointer, false, 1, false);
    }
    //    cache->getFileOrAdd(file_pointer, file, 1);//new File(file_pointer, false, 1, false, NULL);
        

    for (i = 0; i < (unsigned int)this->hashes; i++) 
    {
        x = (unsigned long)(a + i*b)%this->bits;
        //x = (a + i*b) % this->bits;
        if (getBit(x, file))
            no_hits++;
    }
    //delete file;
    return no_hits == this->hashes;
}


bool BloomFilter::getBit(unsigned long x, File* file)
{
    unsigned long byte = x >> 3;
    unsigned char curr = 0;
    if (!file)
    {
        curr =this->bf[byte];
    }
    else
    {
        Pointer* loc = new Pointer(byte, file_pointer->getFileNo(), file_pointer->getDisk());
        unsigned char* buffer = new unsigned char[10];
        file->readNodeFromFile(loc,buffer, false);
        curr = buffer[0];
        delete[] buffer;
        delete loc;
    }
    unsigned char mask = (unsigned char)(1 << (x % 8));

    
    return curr & mask;
}   
