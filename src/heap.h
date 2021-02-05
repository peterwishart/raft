/* Internal heap APIs. */

#ifndef HEAP_H_
#define HEAP_H_

#include <stddef.h>

void *heapMalloc(size_t size);

void *heapCalloc(size_t nmemb, size_t size);

void *heapRealloc(void *ptr, size_t size);

void heapFree(void *ptr);

#endif /* HEAP_H_ */
