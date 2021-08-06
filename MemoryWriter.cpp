#include "MemoryWriter.h"


MemoryWriter::MemoryWriter(NBTree* tree, Pointer* write_at_disk, bool is_root)
{
	file = new File(write_at_disk, true, PAGE_SIZE, is_root);
	root = new Pointer(0, 0, 0);
	file->getCurrLocation(root);
    //TODO: Change this
	buffers = new Buffers(true, true, this, root);

    this->first_node = new Pointer(0, 0, 0);
    file->getCurrLocation(this->first_node);
	this->tree = tree;
	this->is_outer_leaf = true;
	no_pages_written = 0;
	no_elements_written = 0;
	this->write_at_disk = write_at_disk;
}


void MemoryWriter::moveToNextChild(Pointer* root, count_t& node_size_written, Pointer* first_node, count_t& no_elements_written)
{
    Pointer* loc_written = NULL;
    for (Buffers* curr = buffers; curr != NULL; curr = curr->getUpperBuffer())
    {
        loc_written = curr->writeRemainingBuffers(loc_written);
        if (loc_written != NULL && curr->getUpperBuffer() == NULL)
            this->root->copyPointer(loc_written);
    }
	root->copyPointer(this->root);
	finishChildren();


	node_size_written = no_pages_written;
	no_elements_written = this->no_elements_written;


	first_node->copyPointer(this->first_node);


    //for (int i = 0; i < buffers_size; i++)
    delete buffers;
    buffers_size = 0;
	delete this->root;
	delete this->first_node;
	this->root = new Pointer(0, 0, 0);
	file->getCurrLocation(this->root);
	no_pages_written = 0;
	this->no_elements_written = 0;
	buffers = new Buffers(true, true, this, root);
    this->first_node = new Pointer(0, 0, 0);
    file->getCurrLocation(this->first_node);
}

void MemoryWriter::finishChildren()
{
	unsigned char* end_buffer = new unsigned char[PAGE_SIZE +4];
    for (int i = 0; i < 16; end_buffer[i++] = 255);
	Pointer* endLoc = new Pointer(0, 0, write_at_disk->getDisk());
	file->writeNodeToEndFile(endLoc, end_buffer, endLoc);
	increasePagesWrittenBy(1);
	delete endLoc;
	delete[] end_buffer;

}

MemoryWriter::~MemoryWriter()
{
	delete first_node;
	delete root;
    //for (int i = 0; i < buffers_size; i++)
    //    delete buffers[i];
    delete buffers;
	delete write_at_disk;
	delete file;
}

void MemoryWriter::increasePagesWrittenBy(count_t value)
{
	no_pages_written += value;
}

void MemoryWriter::write(KEY key, VALUE value)
{

	no_elements_written++;

    Pointer* root = buffers->addToBuffer(key, value, NULL);
    if (root)
        this->root->copyPointer(root);
}


