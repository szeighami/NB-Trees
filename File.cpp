#include "File.h"

int File::disk_curr_file_no[3];

std::mutex File::mlock;
double File::write_seek_time;
double File::write_sequential_time;
double File::read_seek_time;
double File::read_sequential_time;

int File::read_no_seek;
int File::write_no_seek;

char File::disk_prefix[100];

int File::openFile(char* file_name, bool read_only, bool create)
{
    mlock.lock();
    int file = -1;
    if (read_only)
        file = open(file_name, O_RDONLY);
    else if (create)
        file = open(file_name, O_RDWR | O_CREAT, S_IRWXU);
    else
       file = open(file_name, O_RDWR, S_IRWXU);
    mlock.unlock();
    return file;

}

int File::getFileFromPointer(Pointer* p, bool use_file_name, bool read_only)
{
	if (!use_file_name)
		sprintf(file_name, "%s%lu/NBTree%lu.txt", disk_prefix, p->getDisk(), p->getFileNo());

    int file = openFile(file_name, read_only, false);

	if (file != -1)
		return file;

	safe_to_delete = true;


	return file;
}



int File::getNewFileFromPointerDisk(Pointer* p, bool use_file_name)
{
	if (!use_file_name)
	{
		sprintf(file_name, "%s%lu/NBTree%d.txt", disk_prefix, p->getDisk(), File::disk_curr_file_no[p->getDisk()-1]++);
	}
	int file = openFile(file_name, false, true);
	return file;
}

File::File(Pointer* p, bool to_write, uint64_t nodeSize, bool is_root)
{
    delay_write = false;
	safe_to_delete = false;
	if (to_write)
	{
		if (is_root)
		{
			sprintf(file_name, "%s1/NBTreeR.txt", disk_prefix);
			curr_location = new Pointer(0, 0, 1);
			this->file = getNewFileFromPointerDisk(p, true);
		}
		else
		{
			curr_location = new Pointer(0, File::disk_curr_file_no[p->getDisk()-1], p->getDisk());
			this->file = getNewFileFromPointerDisk(p, false);
		}
	}
	else
	{
		if (is_root)
		{
			sprintf(file_name, "%s1/NBTreeR.txt", disk_prefix);
			curr_location = new Pointer(0, 0, 1);
			this->file = getFileFromPointer(p, true, true);
		}
		else
		{
			curr_location = new Pointer(0, p->getFileNo(), p->getDisk());
			this->file = getFileFromPointer(p, false, true);
		}
	}



	this->nodeSize = nodeSize;

	//write_seek_time = 0;
	//write_sequential_time = 0;
	//read_seek_time = 0;
	//read_sequential_time = 0;
	reached_eof = false;
}

uint64_t  File::readNodeFromFile(Pointer* p, unsigned char* buffer, bool remove_from_cache)
{
    if (remove_from_cache)
        cache->removePage(p);

	uint64_t bytesRead = 0;

	Time* t = new Time;
	Util::startTime(t);
	lseek(file, p->getLocation(), 0);
	read_seek_time += Util::getTimeElapsed(t);
	delete t;
    read_no_seek++;

	t = new Time;
	Util::startTime(t);
	bytesRead = read(file, buffer, nodeSize);
	read_sequential_time += Util::getTimeElapsed(t);
	delete t;

	curr_location->setLocation(p->getLocation()+bytesRead);

	if (bytesRead < nodeSize)
		reached_eof = true;

	return bytesRead;
}

uint64_t  File::readNodeAtCurr(unsigned char* buffer, bool remove_from_cache)
{
    if (remove_from_cache)
        cache->removePage(curr_location);

	//int file = getFileFromPointer(p);
    uint64_t bytesRead = 0;

	Time* t = new Time;
	Util::startTime(t);
	bytesRead = read(file, buffer, nodeSize);
	read_sequential_time += Util::getTimeElapsed(t);
	delete t;

	curr_location->setLocation(curr_location->getLocation()+bytesRead);

	if (bytesRead < nodeSize)
		reached_eof = true;

	return bytesRead;
}


void File::seekToEnd(Pointer* p, Pointer* new_location)
{
	//int file = getFileFromPointer(p);
	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, 0, 2);
	write_seek_time += Util::getTimeElapsed(t);
    write_no_seek++;
	delete t;

	curr_location->setLocation(loc);

	new_location->setDisk(p->getDisk());
	new_location->setFileNo(p->getFileNo());
	new_location->setLocation(loc);
}

void File::seekToPointer(Pointer* p)
{
	//int file = getFileFromPointer(p);
	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, p->getLocation(), 0);
	write_seek_time += Util::getTimeElapsed(t);
	delete t;
    write_no_seek++;

	curr_location->setLocation(loc);
}


void File::seekToBeginning(Pointer* p, Pointer* new_location)
{
	//int file = getFileFromPointer(p);
	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, 0, 0);
	write_seek_time += Util::getTimeElapsed(t);
	delete t;
    write_no_seek++;

	curr_location->setLocation(loc);

	new_location->setDisk(p->getDisk());
	new_location->setFileNo(p->getFileNo());
	new_location->setLocation(loc);
}

File::~File()
{
	delete curr_location;
	close(file);
}

char* File::getFileNameFromPointer(Pointer* p)
{
	char* file_name = new char[30];
	sprintf(file_name, "%s%lu/NBTree%lu.txt", disk_prefix, p->getDisk(), p->getFileNo());
	return file_name;

}


void File::deleteFile(bool read_all)
{
	if (read_all)// || safe_to_delete)
	{
		delete curr_location;
		curr_location = NULL;
//		unlink(file_name);
	}
	/*else
	{
		char command[100];
		char new_file_name[100];
		sprintf(new_file_name, "%sD", file_name);
		sprintf(command, "mv %s %s", file_name, new_file_name);
		system(command);

	}*/
}

void File::getCurrLocation(Pointer* curr_location)
{
	curr_location->setLocation(this->curr_location->getLocation());
	curr_location->setDisk(this->curr_location->getDisk());
	curr_location->setFileNo(this->curr_location->getFileNo());
}


void File::writeNodeToEndFile(Pointer* p, unsigned char* buffer, Pointer* new_location)
{
    if (delay_write)
        throw "NOT IMPLEMENTED";
	//int file = getFileFromPointer(p);
	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, 0, 2);
	write_seek_time += Util::getTimeElapsed(t);
	delete t;
    write_no_seek++;

	t = new Time;
	Util::startTime(t);
	uint64_t bytesWrote = 0;
	bytesWrote = write(file, buffer, nodeSize);
	write_sequential_time += Util::getTimeElapsed(t);
	delete t;

	curr_location->setLocation(loc + bytesWrote);


	new_location->setDisk(p->getDisk());
	new_location->setFileNo(p->getFileNo());
	new_location->setLocation(loc);

}


void File::writeDelayed()
{
    Time* t = new Time;
    Util::startTime(t);
    uint64_t total_bytes_wrote = 0;
    //cout << "Writing" << endl;

    for (int i = 0; i < write_buffer_size; i++)
    {
        long bytesWrote = write(file, write_buffer[i], nodeSize);
        if (bytesWrote != nodeSize)
            std::cout << "DIDNT WRITE, errno=" << errno << std::endl;
        total_bytes_wrote += bytesWrote;
        delete[] write_buffer[i];
    }
    delete[] write_buffer;
    delay_write = false;
    //cout << "TOOK" << Util::getTimeElapsed(t) << " Wrote " << total_bytes_wrote  << endl;
    delete t;
}

void File::writePageAtCurr(Pointer* p, unsigned char* buffer, Pointer* new_location)
{
	uint64_t bytesWrote = 0;
    if (delay_write)
    {
        unsigned char* tmp = new unsigned char[nodeSize];
        memcpy(tmp, buffer, nodeSize);
        write_buffer[write_buffer_size++] = tmp;
        bytesWrote = nodeSize;
        if (write_buffer_size >= write_buffer_cap)
        {
            writeDelayed();
            setDelayedWrite(write_buffer_cap);
        }
    }
    else
    {
        Time* t = new Time;
        Util::startTime(t);
        bytesWrote = write(file, buffer, nodeSize);
        if (bytesWrote != nodeSize)
            std::cout << "DIDNT WRITE, errno=" << errno << std::endl;

        write_sequential_time += Util::getTimeElapsed(t);
        delete t;
    }

	unsigned long loc = curr_location->getLocation();
	curr_location->setLocation(loc + bytesWrote);

	new_location->setDisk(p->getDisk());
	new_location->setFileNo(p->getFileNo());
	new_location->setLocation(loc);

}



uint64_t File::writePageAtLoc(Pointer* p, unsigned char* buffer)
{
    if (delay_write)
        throw "NOT IMPLEMENTED";
	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, p->getLocation(), 0);
	write_seek_time += Util::getTimeElapsed(t);
	delete t;
    write_no_seek++;

	t = new Time;
	Util::startTime(t);
	uint64_t bytesWrote = 0;
	bytesWrote  = write(file, buffer, nodeSize);
	write_sequential_time += Util::getTimeElapsed(t);
	delete t;

	curr_location->setLocation(loc + bytesWrote);

	return bytesWrote;
}

uint64_t File::writeRootLocation(Pointer* p)
{
	unsigned char* buffer = new unsigned char[Pointer::getSize()+4];
	p->writePointerAt(buffer);

	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, 0, 0);
	write_seek_time += Util::getTimeElapsed(t);
	delete t;
    write_no_seek++;

	t = new Time;
	Util::startTime(t);
	uint64_t bytesWrote = 0;
	bytesWrote  = write(file, buffer, Pointer::getSize());
	write_sequential_time += Util::getTimeElapsed(t);
	delete t;

	delete[] buffer;

	curr_location->setLocation(loc + bytesWrote);

	return bytesWrote;
}

void File::readRootLocation(Pointer* p)
{
    throw "NOT IMPLEMENTED";
    /*
	unsigned char* buffer = new unsigned char[Pointer::getSize()+4];

	long loc = 0;

	Time* t = new Time;
	Util::startTime(t);
	loc = lseek(file, 0, 0);
	read_seek_time += Util::getTimeElapsed(t);
	delete t;
    read_no_seek++;

	t = new Time;
	Util::startTime(t);
	uint64_t bytesRead = 0;
	bytesRead = read(file, buffer, Pointer::getSize());
	read_sequential_time += Util::getTimeElapsed(t);
	delete t;

	p->setDisk(Util::readT<unsigned char>(&buffer[0]));
	p->setFileNo(Util::readT<unsigned int>(&buffer[1]));
	p->setLocation(Util::readT<unsigned int>(&buffer[5]));

	curr_location->setLocation(loc + bytesRead);


	delete[] buffer;
    */
}

