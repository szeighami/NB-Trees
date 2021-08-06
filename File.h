/*
 * File.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef FILE_H_
#define FILE_H_
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <deque>
#include  <future>
#include <mutex>

#include "Util.h"
#include "Cache.h"
class Pointer;
class Cache;

extern Cache* cache;

class File
{
public:
	//File(int nodeSize);
	//File(char* filename1,char* filename2, char* filename3, int nodeSize);
	File(Pointer* p, bool to_write, uint64_t nodeSize, bool is_root);
	void writeNodeToEndFile(Pointer* p, unsigned char* buffer, Pointer* new_location);
	void writePageAtCurr(Pointer* p, unsigned char* buffer, Pointer* new_location);
	uint64_t writePageAtLoc(Pointer* p, unsigned char* buffer);
	uint64_t writeRootLocation(Pointer* p);
	void seekToEnd(Pointer* p, Pointer* new_location);
	void seekToPointer(Pointer* p);
	void seekToBeginning(Pointer* p, Pointer* new_location);
	void getCurrLocation(Pointer* curr_location);
	uint64_t readNodeAtCurr(unsigned char* buffer, bool remove_from_cache);
	uint64_t readNodeFromFile(Pointer* p, unsigned char* buffer, bool remove_from_cache);

	bool exists(){return file != -1;}

	void readRootLocation(Pointer* p);
	~File();

	void sync(){fsync(file);}//fsync(file3);fsync(file2);}

	static double getWSeekNo(){return write_no_seek;}
	static double getWSeekT(){return write_seek_time;}
	static double getRSeekNo(){return read_no_seek;}
	static double getRSeekT(){return read_seek_time;}
	static double getWSeqT(){return write_sequential_time;}
	static double getRSeqT(){return read_sequential_time;}
	static void initWSeekT(){write_no_seek = 0; write_seek_time = 0;}
	static void initRSeekT(){read_no_seek = 0; read_seek_time = 0;}
	static void initWSeqT(){write_sequential_time = 0;}
	static void initRSeqT(){read_sequential_time = 0;}


	static void setFileNo(int disk)
	{
		disk_curr_file_no[disk - 1] = 0;

		struct dirent **namelist;
		int n;

		char disk_name[20];
		sprintf(disk_name, "%s%d", disk_prefix, disk);
		n = scandir(disk_name, &namelist, 0, versionsort);
		if (n < 3)
		{
			for(int i =0 ; i < n; ++i)
				free(namelist[i]);
			free(namelist);
		   return;
		}
		else
		{
			int last_index= n - 1;
			if (strcmp(namelist[n-1]->d_name, "NBTreeR.txt")==0)
				last_index = n - 2;

            if (last_index < 0)
                disk_curr_file_no[disk - 1] = 0;
            else
            {
                char number[10]; number[0] = 0;
                for(int i = 0;namelist[last_index]->d_name[6+i] != '.';i++)
                    sprintf(number, "%s%c", number, namelist[last_index]->d_name[6+i]);

                disk_curr_file_no[disk - 1] = atoi(number) + 1 ;

            }

			for(int i =0 ; i < n; ++i)
				free(namelist[i]);
			free(namelist);
			return;
		}
	}

	void deleteFile(bool read_all);
	void writeDelayed();
	void setDelayedWrite(uint64_t delay_capacity)
    {
        delay_write = true;
        write_buffer_cap = delay_capacity;
        write_buffer = new unsigned char*[write_buffer_cap];
        write_buffer_size = 0;
    }
	static void setDiskPrefix(const char* prefix)
    {
        strcpy(File::disk_prefix, prefix);
		char disk_name[20];
		sprintf(disk_name, "%s%d", disk_prefix, 1);
        struct stat buffer;
        if (stat (disk_name, &buffer) != 0)
        {
            char cmd[25];
            sprintf(cmd, "mkdir %s", disk_name);
            system(cmd);
        }
    }
	static char* getFileNameFromPointer(Pointer* p);
	static int openFile(char* file_name, bool read_only, bool create);

    int getFile(){return file;}


private:
	bool safe_to_delete;
	bool reached_eof;
	int file;
	bool delay_write;
    unsigned char** write_buffer;
    uint64_t  write_buffer_size;
    uint64_t write_buffer_cap;
	char file_name[100];
	static char disk_prefix[100];
	static int disk_curr_file_no[3];

	//static int disk2_file_no;
	//static int disk3_file_no;
	static double write_seek_time;
	static double write_sequential_time;
	static double read_seek_time;
	static double read_sequential_time;
    static int read_no_seek;
    static int write_no_seek;

    uint64_t nodeSize;
	int getFileFromPointer(Pointer* p, bool use_file_name, bool read_only);
	int getNewFileFromPointerDisk(Pointer* p, bool use_file_name);
	Pointer* curr_location;

    static std::mutex mlock;

};




#endif /* FILE_H_ */

