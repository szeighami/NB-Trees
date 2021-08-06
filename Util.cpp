#include "Util.h"


void Util::calculateExecutionTime(struct rusage *myTimeStart, struct rusage *myTimeEnd, float *userTime, float *sysTime)
{
		(*userTime) =
				((float) (myTimeEnd->ru_utime.tv_sec  - myTimeStart->ru_utime.tv_sec)) +
				((float) (myTimeEnd->ru_utime.tv_usec - myTimeStart->ru_utime.tv_usec)) * 1e-6;
		(*sysTime) =
				((float) (myTimeEnd->ru_stime.tv_sec  - myTimeStart->ru_stime.tv_sec)) +
				((float) (myTimeEnd->ru_stime.tv_usec - myTimeStart->ru_stime.tv_usec)) * 1e-6;

}








