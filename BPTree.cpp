#include "BPTree.h"


BPTree::~BPTree()
{
	delete root_pointer;
	root_pointer = NULL;
}


BPTree::BPTree(Pointer* p, int nodeSize)
{
    cache->getFileOrAdd(p, file, nodeSize);
	root_pointer = new Pointer(p);
}

void BPTree::print(int level)
{
	printNode(root_pointer, level);
}

void BPTree::printNode(Pointer* node_pointer, int level)
{
	InnerNode* node = new InnerNode(node_pointer, file, false);

	if (node->isLeaf())
	{
        for (int j = 0; j < level; j++) printf("\t");
		for (int i = 0; i < node->getNoElements(); i++)
		{
			//for (int j = 0; j < level; j++) printf("\t");
			cout << "( " << node->getKeyAt(i) << ", " << node->getValueAt(i) << ") " ;
		}
		printf("\n");
		bool is_first;

	}
	else
	{
		for (int i = 0; i < node->getNoElements(); i++)
		{
			printNode(node->getPointerAt(i), level+1);
			for (int j = 0; j < level; j++) printf("\t");
			cout << "( " << node->getKeyAt(i) << ", " << node->getValueAt(i) << ")" << endl;
		}
		printNode(node->getPointerAt(node->getNoElements()), level+1);

	}
	delete node;
}
VALUE BPTree::find(KEY key)
{
    if (!file->exists())
    {
        return -1;
    }
	return findValue(this->root_pointer, key, true);
}


void BPTree::find(KEY low, KEY high, vector<KEY>* result)
{
    if (!file->exists())
    {
        return;
    }
	findValue(this->root_pointer, low, high, result, true);
}

void BPTree::findValue(Pointer* node_pointer, KEY low, KEY high, vector<KEY>* result, bool only_snode)
{
    Time* time = new Time;
    Util::startTime(time);

    int i = 0;
	InnerNode* node;
    bool read_cache;
    Pointer* curr_pointer = new Pointer(node_pointer);
    DiskIterator* iter = NULL;
    while(true)
    {
        pages_read++;
        read_cache = false;
        if(cache->getPageIfExists(curr_pointer, node))
            read_cache = true;
        else
        {
            node = new InnerNode(curr_pointer, file, false);
            cache_misses++;
        }
        i = node->getKeyIndex(low, 0, node->getNoElements());

        if (node->isLeaf())
        {
            iter = new DiskIterator(curr_pointer, i, node, file, PAGE_SIZE, -1, true, false, 2560, high);
            break;
        }

        delete curr_pointer;
        curr_pointer = new Pointer(node->getPointerAt(i));
        if (!read_cache)
            delete node;
    }
    while (!iter->reachedEnd())
    {
        KEY curr_key = iter->getKey();
        if (curr_key <= high)
            result->push_back(curr_key);
        else
            break;

        iter->moveNext();
    }

    get_page_time += Util::getTimeElapsed(time);
    if (!read_cache)
        delete node;
    delete time;
    delete curr_pointer;
    delete iter;
    return; 
}

VALUE BPTree::findValue(Pointer* node_pointer, KEY key, bool only_snode)
{
    Time* time = new Time;
    Util::startTime(time);

    int i = 0;
	InnerNode* node;
    bool read_cache;
    Pointer* curr_pointer = new Pointer(node_pointer);
    while(true)
    {
        pages_read++;
        read_cache = false;
        if(cache->getPageIfExists(curr_pointer, node))
        {
            read_cache = true;
        }
        else
        {
            node = new InnerNode(curr_pointer, file, false);
            cache_misses++;
        }
        i = node->getKeyIndex(key, 0, node->getNoElements());


        if (node->isLeaf())
        {
            if (i == node->getNoElements() || node->getKeyAt(i) != key)
            {
                if (!read_cache)
                    delete node;
                get_page_time += Util::getTimeElapsed(time);
                delete time;
                delete curr_pointer;
                return -1;
            }
            VALUE val = node->getValueAt(i);
            if (!read_cache)
                delete node;
            get_page_time += Util::getTimeElapsed(time);
            delete time;
            delete curr_pointer;
            return val;
        }

        delete curr_pointer;
        curr_pointer = new Pointer(node->getPointerAt(i));
        if (!read_cache)
            delete node;
    }
}





