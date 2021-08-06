/*
 *
 * Taken from https://github.com/jvirkki/libthis/blob/master/this.c
 */

#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_
#include "KeyValueType.h"
#include "Murmurhash3.h"
#include "murmurhash2.h"
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include "File.h"
#include "Cache.h"

extern long bloom_size;
class Cache;

class BloomFilter{
public:
    //It's not using the error rate
    //get bpe as a parameter
    BloomFilter(uint64_t entries, double error, Cache* cache, int bpe)
    {
        this->entries = entries;
        this->error = error;

        //double num = log(this->error);
        //double denom = 0.480453013918201; // ln(2)^2
        this->bpe =bpe;//-(num / denom);

        double dentries = (double)entries;
        this->bits = (uint64_t)(dentries * this->bpe);

        if (this->bits % 8)
        {
            this->bytes = (this->bits / 8) + 1;
        } 
        else
        {
            this->bytes = this->bits / 8;
        }

        this->hashes = (int)ceil(0.693147180559945 * this->bpe);  // ln(2)
        this->cache = cache;
        this->bf = NULL;
        this->bf = (unsigned char *)calloc(this->bytes, sizeof(unsigned char));
        bloom_size += this->bytes;  
        file_pointer = NULL;

    }	


    void addElement(KEY key)
    {
        //register unsigned int a = murmurhash2(&key, sizeof(key), 0x9747b28c);
        //register unsigned int b = murmurhash2(&key, sizeof(key), 0x2396f38b);
        register unsigned long a;// = key;
        register unsigned long b;// = key*2;
        MurmurHash3_x64_128(&key, sizeof(key), 0x9747b28c, &a);
        MurmurHash3_x64_128(&key, sizeof(key), 0x248c893e, &b);
        register unsigned long x;
        register unsigned long i;
        unsigned long byte;
        unsigned char curr;
        unsigned char mask;

        for (i = 0; i < (unsigned int)this->hashes; i++) 
        {
            x = (unsigned long)(a + i*b)%this->bits;
            byte = x >> 3;
            curr = this->bf[byte]; 
            mask = (unsigned char)(1 << (x % 8));
            this->bf[byte] = curr | mask;
        }

    }
    bool exists(KEY key);
    

    ~BloomFilter();

    bool getBit(unsigned long x, File* file);

    void printInfo()
    {
        //printf("this at %p, ", (void *)this);
        //printf("entries = %d, ", this->entries);
        //printf(" ->error = %f, ", this->error);
        //printf("bits = %d", this->bits);
        //printf("bits per elem = %f\n", this->bpe);
        //printf(" ->bytes = %d, ", this->bytes);
        //printf(" ->hash functions = %d\n", this->hashes);
    }
    void writeToFile(bool keep_in_memory);
    
        

private:

  uint64_t entries;
  double error;
  uint64_t bits;
  uint64_t bytes;
  uint32_t hashes;
 
  Pointer* file_pointer;
  Cache* cache;

  double bpe;
  unsigned char * bf;
  int ready;
	
};






#endif /* INTERNALINNERNODE_H_ */
