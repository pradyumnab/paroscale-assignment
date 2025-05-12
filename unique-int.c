// Assumptions
// + big-file is a text file with 'int' on each line.
// + that is max value considered is 'int' and not long
//
// + Use mmap to load the file into memory. Enough memory is available
// 
// + 


#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FILENAME "big-int.txt"

typedef enum StatusE {
	S_ERROR = 0,
	S_INVALID_FILE,
	S_SUCCESS,
	S_MERGE, // merge array
	S_ADD, // array
} StatusE;

#define MAX_THREAD_COUNT 4

StatusE Init(const char* filename);
StatusE DoWork(const char *filename);
StatusE MarkDuplicate(int sorted_ints[], size_t length);
StatusE MergeIntoGlobal(int unique_ints[], size_t length);
StatusE Cleanup();
StatusE PrintUniqueInts();
void* ThreadWorker(void* args);

typedef struct DetailsT {
	unsigned long long _size;
	size_t _thread_count;
	size_t _max_digit_count;  // MAX_INT
	int _fd; // the fd of opened file
	char* _big_int_buffer;
} *DetailsP;
typedef struct DetailsT DetailsT;

typedef struct ThreadDetailsT {
	size_t _start_offset;
	size_t _end_offset;
	size_t _thread_number;
	pthread_t _pthread_id;
} *ThreadDetailsP;
typedef struct ThreadDetailsT ThreadDetailsT;

typedef struct NodeIntT NodeIntT;
typedef struct NodeIntT {
	NodeIntT* _node_next;
	NodeIntT* _node_prev;
	int _i;
} *NodeIntP;

typedef struct UniqueIntHelperT {
	NodeIntP _node_head;
	NodeIntP _node_tail;
	NodeIntP _node_middle;
	size_t _count;
} *UniqueIntHelperP;
typedef struct UniqueIntHelperT UniqueIntHelperT;

static DetailsP gpDetails = NULL;
UniqueIntHelperT* gpUniqueList = NULL;
pthread_mutex_t gMutexUniqueInt = PTHREAD_MUTEX_INITIALIZER;

StatusE Init(const char* filename) {
	// validte
	gpDetails = (DetailsP) malloc(sizeof(DetailsP));
	memset(gpDetails, 0, sizeof(DetailsT));
	gpDetails->_size = 0;
	gpDetails->_thread_count = 0;
	// get size of file
	int fd = open(filename, O_RDONLY);
	struct stat file_details;
	if (-1 == fd) {
		perror("invalid file");
		return S_INVALID_FILE;
		
	} else if (-1 == fstat(fd, &file_details)) {
		perror("cant stat file");
		return S_INVALID_FILE;			
	} else {
		gpDetails->_size = file_details.st_size;
		gpDetails->_thread_count = MAX_THREAD_COUNT;
		gpDetails->_fd = fd;
		// Assumption: Using signed int as integers, hence max number of digits on
		// single line is 10
		gpDetails->_max_digit_count = 10;

		void* ints_as_string = mmap(NULL, file_details.st_size,
		                            PROT_READ, MAP_PRIVATE, fd, 0);
		if (MAP_FAILED == ints_as_string) {
			perror("error mmaping file");
			close (fd);
			return S_ERROR; // todo: mmap errro
		} else {
			gpDetails->_big_int_buffer = (char*)ints_as_string;
		}
	}
	gpUniqueList = (UniqueIntHelperP) malloc(sizeof(UniqueIntHelperT));
	gpUniqueList->_node_head = NULL;
	gpUniqueList->_node_tail = NULL;
	gpUniqueList->_node_middle = NULL;
	gpUniqueList->_count = 0;
	return S_SUCCESS;
}

// Assumption: The big file will fit in memory. Size of big-file will be < 2GB

int IntCompartor(const void *left, const void *right)
{
	return (*(int*)left - *(int*)right);
}

StatusE MarkDuplicate(int sorted_ints[], size_t length) {
	for (size_t i = 0; i < length; ++i) {
		size_t j = i + 1;
		for (j = i + 1; j < length; ++j) {
			if (sorted_ints[i] == sorted_ints[j]) {
				sorted_ints[j] = 0;				
			} else {
				break;
			}
		} // inner for
		i = j;
	}
	// All duplicates are zero
	return S_SUCCESS;
}

NodeIntP CreateIntNode(int value) {
		NodeIntP node = (NodeIntP) malloc(sizeof(NodeIntT));
		node->_node_next = NULL;
		node->_node_prev = NULL;
		node->_i = value;
		return node;
}

StatusE CheckAndAdd(int value) {
	if (0 == gpUniqueList->_count) {
		NodeIntP node = CreateIntNode(value);
		gpUniqueList->_node_head = node;
		gpUniqueList->_node_tail = node;
		++gpUniqueList->_count;
	} else {
		NodeIntP itrNode = gpUniqueList->_node_head;
		// check with tail
		if (value > gpUniqueList->_node_tail->_i) {
			NodeIntP node = CreateIntNode(value);
			node->_node_next = NULL;
			node->_node_prev = gpUniqueList->_node_tail;
			gpUniqueList->_node_tail->_node_next = node;
			//gpUniqueList->_node_tail->_node_prev; // prev willnot change
			gpUniqueList->_node_tail = node;
		} else {
			for (itrNode = gpUniqueList->_node_head;
			     NULL != itrNode;
			     itrNode = itrNode->_node_next) {
				if (value == itrNode->_i) {
					// as it exists dont do anything
					break;
				} else if (value < itrNode->_i) {
					// we know that this does not exist in the list, so it has to go
					// before the itrNode
					NodeIntP new_node = CreateIntNode(value);
					new_node->_node_next = itrNode;
					new_node->_node_prev = itrNode->_node_prev;
					// set itrNode->prev
					new_node->_node_prev->_node_next = new_node;
					// set itrNode->next
					itrNode->_node_prev = new_node;
					break;
				} else if  (value > itrNode->_i) {
					continue;
				}
			} // for
			assert("should not be null" && (NULL != itrNode));
		}
	}
	return S_SUCCESS;
}

StatusE MergeIntoGlobal(int unique_ints[], size_t length) {
	// lock
	for (size_t i = 0; i < length; ++i) {
		if (0 == unique_ints[i]) {
			continue;
		} else {
			// lock and insert
			{
				pthread_mutex_lock(&gMutexUniqueInt);
				CheckAndAdd(unique_ints[i]);
				pthread_mutex_unlock(&gMutexUniqueInt);
			}
		}
	}
	return S_SUCCESS;
}
void* ThreadWorker(void* args) {
	// Read the buffer
	ThreadDetailsP pThreadDetails = (ThreadDetailsP) args;
	if (NULL != pThreadDetails) {
		char* thread_buffer = gpDetails->_big_int_buffer + pThreadDetails->_start_offset;
		int bytes_read = 0;
		int value = 0;
		UniqueIntHelperP pThreadList = (UniqueIntHelperP) malloc(sizeof(UniqueIntHelperT));
		pThreadList->_node_head = NULL;
		pThreadList->_node_tail = NULL;
                
		//pThreadList->_node_head = (NodeIntP) malloc(sizeof(NodeIntT));
		//pThreadList->_node_tail = (NodeIntP) malloc(sizeof(NodeIntT));
#define MAX_CHUNKS 1024

		int chunks[MAX_CHUNKS] = {0};
		memset(chunks, 0, sizeof(chunks));
		size_t i = 0;
		// as this buffer is in memory, we dont have to worry about locks for the
		// buffer. Each thread will read independent aread of memory
		for (size_t start_offset = pThreadDetails->_start_offset;
		     start_offset < pThreadDetails->_end_offset; ) {
			sscanf(thread_buffer + start_offset, "%d%n", &value, &bytes_read);
			if (i < MAX_CHUNKS) {
				chunks[i] = value;
				++i;
				qsort(chunks, MAX_CHUNKS, sizeof(int), IntCompartor);
				// mark all duplicates as 0
				MarkDuplicate(chunks, MAX_CHUNKS);
				MergeIntoGlobal(chunks, MAX_CHUNKS);
				
			} else {
				memset(chunks, 0, sizeof(chunks));
				i = 0;
				chunks[i] = value;
				++i;
			}
			
			
			start_offset += bytes_read;
		} // for
		
	} else {
		perror("args null");
		
	}
	return NULL;
}

StatusE DoWork(const char *filename) {
	StatusE eStatus = S_SUCCESS;
	// gpDetails is set
	size_t bytes_to_read = gpDetails->_size / gpDetails->_thread_count;
	size_t next_offset = 0;
	ThreadDetailsT aThreadDetails[gpDetails->_thread_count];
	for (size_t i = 0; i < gpDetails->_thread_count; ++i) {
		ThreadDetailsP pThreadDetails = (ThreadDetailsP) &aThreadDetails[i];
		memset(pThreadDetails, 0, sizeof(ThreadDetailsT));
		pThreadDetails->_start_offset = next_offset;
		pThreadDetails->_pthread_id = 0;
		// Assume int. As max_int is aournd 10 digit (with newline), we read 10
		// extra digits
		pThreadDetails->_end_offset
			= bytes_to_read
			+ next_offset
			+ gpDetails->_max_digit_count; // assume very long int as string.
		if (pThreadDetails->_end_offset > gpDetails->_size) {
			// handle the last part to be read, end of file. This will ensure that
			// end of file is not breached.
			pThreadDetails->_end_offset = gpDetails->_size;
		}
		pThreadDetails->_thread_number = i;

		int retCode = pthread_create(&pThreadDetails->_pthread_id, NULL,
		                             ThreadWorker,
		                             (void*) pThreadDetails);
		if (0 != retCode) {
			perror("Error creating thread");
			return S_ERROR;
		} else {
			
		}

		next_offset += bytes_to_read + 1;
	}
	for (size_t i = 0; i < gpDetails->_thread_count; ++i) {
		pthread_join(aThreadDetails[i]._pthread_id, NULL);
	}
	return eStatus;
}

StatusE PrintUniqueInts() {
	NodeIntP node = gpUniqueList->_node_head;
	for (node = gpUniqueList->_node_head; NULL != node; node = node->_node_next) {
		printf("%d ", node->_i);
	}
	printf(" \n");
	//for ()
	return S_SUCCESS;
}
StatusE Cleanup() {
	munmap(gpDetails->_big_int_buffer, gpDetails->_size);
	close(gpDetails->_fd);
	return S_SUCCESS;
}

int main(int argc, char *argv[]) {
	if (2 != argc) {
		printf("Usage %s <file-name>", argv[0]);
		return EXIT_FAILURE;
	}
	StatusE eStatus = S_ERROR;
	const char* filename = argv[1];
	if (S_ERROR == (eStatus = Init(filename))) {
		
	} else if (S_ERROR == (eStatus = DoWork(filename))) {
		
	}  else if (S_ERROR == (eStatus = PrintUniqueInts())) {
		
	} else if (S_ERROR == (eStatus = Cleanup())) {
		
	}
	//eStatus = S_SUCCESS;
	return (S_SUCCESS == eStatus) ? 0 : EXIT_FAILURE;
}

// gcc -Wall ./unique-int.c
// g++ -Wall ./unique-int.c
