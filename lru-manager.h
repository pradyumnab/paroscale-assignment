#ifndef __LRU_MANAGER_H__
#define __LRU_MANAGER_H__

#include <assert.h>
#include <time.h>

// Approach:
// + Hash-Map with key and file-path
// + Doubly Linked list


#define HASH_BUCKETS 128
#define MAX_LRU_ENTRIES 256
#define MAX_FILE_NAME_WITH_PATH 256

typedef enum StatusE {
	S_ERROR = 0,
	S_SUCCESS,
	S_ADD,
	S_UPDATE,
	S_PRESENT,
	S_REMOVE,
	S_REMOVE_TIME,
} StatusE;


typedef struct InfoT {
	char*  _file_name; // MAX_FILE_NAME_WITH_PATH;
	time_t _time;
} *InfoP;

typedef struct NodeT {
	// Details of whats cached
	InfoP _info;
	
	// doubly link list for TimeOrderedListT
	NodeT* _node_prev;
	NodeT* _node_next;
	// doubly link list for HashT
	NodeT* _node_hash_prev;
	NodeT* _node_hash_next;
	// hash index is to help remove quickly
	size_t _hash_index;
} *NodeP;
typedef struct NodeT NodeT;

typedef struct TimeOrderedListT {
	//NodeP
	int _list_entries;
	NodeP _node_order_head;
	NodeP _node_order_tail;
} *TimeOrderedListP;
typedef struct TimeOrderedListT TimeOrderedListT;

typedef struct HashT {
	int   _buckets;
	NodeT** _node_buckets;
} *HashP;
typedef struct HashT HashT;

typedef struct LRUManagerT {
	// Hash map to get quick access
	HashP _lru_hash;
	// first element will always be recent, the last will be lesst used.
	TimeOrderedListP _time_ordered_list;
	int  _entries;
  
} *LRUManagerP;
typedef struct LRUManagerT LRUManagerT;

InfoP   CreateInfo(const char* file_name);
StatusE Init(LRUManagerP *ppLRUManager);
StatusE AddItem(LRUManagerP pLRUManager, const char *file_name);
StatusE FindAndBringAhead(LRUManagerP pLRUManager, NodeP recent_node);
StatusE RemoveItem(LRUManagerP pLRUManager);
size_t  GetBucketCount(const char* key_name, int buckets);

#endif // __LRU_MANAGER_H__

// gcc -Wall ./lru-manager.c
// g++ -Wall ./lru-manager.c -o ./test-lru.bin
