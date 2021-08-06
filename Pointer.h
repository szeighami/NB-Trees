/*
 * Pointer.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef POINTER_H_
#define POINTER_H_
#include"Util.h"
class Util;


class Pointer
{
public:
	Pointer(){	this->location= 0;	this->file_no= 0;	this->disk = 0;	}
	Pointer(unsigned char* buffer);
	Pointer(Pointer* other){this->location= other->location;	this->disk = other->disk; this->file_no = other->file_no;}
	Pointer(uint64_t location, uint32_t file_no, uint8_t disk){	this->location= location;	this->file_no = file_no;	this->disk = disk;	}
	~Pointer()	{	}

	int writePointerAt(unsigned char* buffer);
	uint64_t getLocation()
	{
		if (this == NULL)
		{
			printf("WHWHWHWHWHWHWHWYLL\n");
//			return -1;
		}
		return location;
	}

    void copyPointer(Pointer* other)
    {
        this->location= other->location;	this->disk = other->disk; this->file_no  = other->file_no;
    }

	uint32_t getFileNo()
	{
		if (this == NULL)
		{
			printf("WHWHWHWHWHWHWHWYFF\n");
			//return -1;
		}
		return file_no;
	}

	uint8_t getDisk()
	{
		if (this == NULL)
		{
			printf("WHWHWHWHWHWHWHWYDD\n");
			//return -1;
		}
		return disk;
	}

	void setLocation(unsigned long location)
	{
		if (this == NULL)
		{
			printf("WHWHWHWHWHWHWHWYSLL\n");
			//return;
		}
		this->location = location;
	}
	void setFileNo(uint32_t file_no)
	{
		if (this == NULL)
		{
			printf("WHWHWHWHWHWHWHWYSFF\n");
			//return;
		}
		this->file_no = file_no;
	}
	void setDisk(uint8_t disk)
	{
		if (this == NULL)
		{
			printf("WHWHWHWHWHWHWHWYSDD\n");
			//return;
		}
		this->disk = disk;
	}

	static int getSize(){return location_write_bytes+file_write_bytes+disk_write_bytes;	}

private:
	Pointer(const Pointer& other){this->location= other.location;	this->disk = other.disk; this->file_no = other.file_no;}
	static int location_write_bytes;
	static int file_write_bytes;
	static int disk_write_bytes;
	uint64_t location;
	uint32_t file_no;
	uint8_t disk;
};




#endif /* POINTER_H_ */
