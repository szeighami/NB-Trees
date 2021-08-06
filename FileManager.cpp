/*
 * FileManager.cpp
 *
 *  Created on: 9 Apr 2018
 *      Author: sepanta
 */

#include "FileManager.h"

struct FileStatus
{
	int no_children_written;
	int no_children_moved;
	Pointer* file_loc;
	FileStatus* next;

	~FileStatus(){delete file_loc;}
};


FileManager::~FileManager()
{
	for(FileStatus* iter = head; iter != NULL;)
	{
		FileStatus* temp  = iter->next;
		delete iter;
		iter = temp;
	}
	head = NULL;
}

void FileManager::addFile(Pointer* file_p, count_t no_childrend)
{
	FileStatus* new_file = new FileStatus;
	new_file->file_loc = new Pointer(0, file_p->getFileNo(), file_p->getDisk());
	new_file->no_children_moved = 0;
	new_file->no_children_written = (int)no_childrend;
	new_file->next = head;
	head = new_file;
}

bool FileManager::moveFile(Pointer* file_p, count_t no_childrend_moved)
{
	FileStatus* result;
	FileStatus* result_previous;
	if (!find(file_p, result, result_previous))
	{
		printf("WHY Not FOUND? \n");
		return false;
	}
	result->no_children_moved += (int)no_childrend_moved;
	if (result->no_children_moved >= result->no_children_written)
	{
		deleteNode(result, result_previous);
		return true;
	}

	return false;
}

bool FileManager::find(Pointer* file_p, FileStatus*& result, FileStatus*& result_previous)
{
	FileStatus* prev_iter = NULL;
	for(FileStatus* iter = head; iter != NULL; prev_iter = iter, iter = iter->next)
	{
		if (iter->file_loc->getDisk() == file_p->getDisk() && iter->file_loc->getFileNo() == file_p->getFileNo())
		{
			result = iter;
			result_previous= prev_iter;
			return true;
		}
	}
	return false;

}
void FileManager::deleteNode(FileStatus*& result, FileStatus*& result_previous)
{
	if (result_previous!= NULL)
		result_previous->next = result->next;
	else
		head = result->next;

	//delete result;

	result->next = files_to_delete_head;
	files_to_delete_head = result;

}

void FileManager::actuallyDeleteFile()
{
	for(FileStatus* iter = files_to_delete_head; iter != NULL;)
	{
        cache->removeFile(iter->file_loc);
		char* file_name = File::getFileNameFromPointer(iter->file_loc);
        char command[50];

        //sprintf(command, " :>%s &", file_name);
        //system(command);
        //sprintf(command, "rm -f %s &", file_name);
        //system(command);
        remove(file_name);

		delete[] file_name;

		FileStatus* temp  = iter->next;
		delete iter;
		iter = temp;
        //usleep(2000);
        sleep(10);
	}
	files_to_delete_head = NULL;
}





