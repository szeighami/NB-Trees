/*
 * FileManager.h
 *
 *  Created on: 9 Apr 2018
 *      Author: sepanta
 */

#ifndef FILEMANAGER_H_
#define FILEMANAGER_H_

#include <fstream>      // std::fstream

#include "Pointer.h"
#include "File.h"

struct FileStatus;

extern Cache* cache;
class FileManager
{
public:
	FileManager(){head = NULL; files_to_delete_head = NULL;}

    ~FileManager();

	void addFile(Pointer* file_p, count_t no_childrend);

	bool moveFile(Pointer* file_p, count_t no_childrend_moved);
	void actuallyDeleteFile();

private:
	bool find(Pointer* file_p, FileStatus*& result, FileStatus*& result_previous);

	void deleteNode(FileStatus*& result, FileStatus*& result_previous);



	FileStatus* head;
	FileStatus* files_to_delete_head;
};


#endif /* FILEMANAGER_H_ */
