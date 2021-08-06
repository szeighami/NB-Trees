/*
 * Util.h
 *
 *  Created on: 14 Jan 2018
 *      Author: sepanta
 */

#ifndef UTIL_H_
#define UTIL_H_
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/times.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <chrono>


#include "Pointer.h"
#include "KeyValueType.h"
#include "Cache.h"
#include "File.h"


typedef long count_t;

class Pointer;

using namespace std;

struct Time
{
	struct timeval start_tv;
	struct timeval tv;
    std::chrono::steady_clock::time_point start;
};


class Util
{
public:
	template<class T>
	static T readT(unsigned char* buffer)
	{
        return *(T*)buffer;
	}

	static void calculateExecutionTime(struct rusage *myTimeStart, struct rusage *myTimeEnd, float *userTime, float *sysTime);

	template<typename T>
	static int writeT(unsigned char* outBuffer, T value)
	{
        memcpy(outBuffer, &value, sizeof(T));
		return sizeof(value);
	}

	static int writeNoElementsHasValue(unsigned char* outBuffer, int no_elements, bool has_value)
	{
		if (has_value)
			return writeT<int>(outBuffer, no_elements);
		else
			return writeT<int>(outBuffer, -1*no_elements);
	}



	static void startTime(Time* t)
	{
		gettimeofday(&t->start_tv, NULL);
        t->start = chrono::steady_clock::now();

	}

	static double getTimeElapsed(Time* t)
	{
		double elapsed = 0.0;
		gettimeofday(&t->tv, NULL);
		elapsed = static_cast<double>((t->tv.tv_sec - t->start_tv.tv_sec) +
		  (t->tv.tv_usec - t->start_tv.tv_usec)) / 1000000.0;

        std::chrono::steady_clock::time_point end = chrono::steady_clock::now();
        elapsed = static_cast<double>(chrono::duration_cast<chrono::nanoseconds>(end - t->start).count())/1000000000.0;

		return elapsed;
	}

};




#endif /* UTIL_H_ */
