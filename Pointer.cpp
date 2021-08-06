#include "Pointer.h"

int Pointer::location_write_bytes = 5;
int Pointer::file_write_bytes = 3;
int Pointer::disk_write_bytes = 1;


Pointer::Pointer(unsigned char* buffer)
{
	this->disk = Util::readT<unsigned char>(&buffer[0]);

	unsigned char* temp_char = new unsigned char[8];
	for (int i = 0; i < location_write_bytes; i++)temp_char[i] = buffer[disk_write_bytes+i];
	for (int i = location_write_bytes; i < 8; temp_char[i++] = 0);
	this->location = Util::readT<unsigned long>(temp_char);
	delete[] temp_char;

	temp_char = new unsigned char[8];
	for (int i = 0; i < file_write_bytes; i++)temp_char[i] = buffer[disk_write_bytes+location_write_bytes+i];
	for (int i = file_write_bytes; i < 8; temp_char[i++] = 0);
	this->file_no = Util::readT<uint32_t>(temp_char);
	delete[] temp_char;
}

int Pointer::writePointerAt(unsigned char* buffer){
	int offset = 0;
	offset += Util::writeT<unsigned char>(&buffer[0], this->disk);

	unsigned char* temp_char = new unsigned char[8];
	Util::writeT<unsigned long>(temp_char, this->location);
	for (int i = 0; i < location_write_bytes; i++)buffer[disk_write_bytes+i] = temp_char[i];
	delete[] temp_char;
	offset += location_write_bytes;

	temp_char = new unsigned char[8];
	Util::writeT<unsigned long>(temp_char, this->file_no);
	for (int i = 0; i < file_write_bytes; i++) buffer[disk_write_bytes+location_write_bytes+i] = temp_char[i];
	delete[] temp_char;
	offset += file_write_bytes;

	return offset;
}

