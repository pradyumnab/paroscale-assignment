#include "lru-manager.h"

#include <stdlib.h>
#include <string.h>

InfoP   CreateInfo(const char* file_name) {
	InfoP info = (InfoP) malloc(sizeof(InfoP));
	info->_file_name = (char*) malloc((strlen(file_name) + 1));
	memset(info->_file_name, 0, (strlen(file_name) + 1));
	strcpy(info->_file_name, file_name);
	info->_time = time(NULL);
	return info;	
}
StatusE Init(LRUManagerP *ppLRUManager) {
	StatusE eStatus = S_ERROR;
	if ((NULL != ppLRUManager) && (NULL != *ppLRUManager)) {
		LRUManagerP lru_manager = (LRUManagerP) malloc(sizeof(LRUManagerP));
		lru_manager->_entries = 0;
		// init hash 
		lru_manager->_lru_hash = (HashP)malloc(sizeof(HashP));
		lru_manager->_lru_hash->_buckets = HASH_BUCKETS;
		// create elements for HashT
		lru_manager->_lru_hash->_node_buckets = 
			(NodeP*)malloc((sizeof(NodeP) * HASH_BUCKETS));
		memset(lru_manager->_lru_hash->_node_buckets, 0,
		       (sizeof(NodeP) * HASH_BUCKETS));
		// no entry hence null
		lru_manager->_time_ordered_list
			= (TimeOrderedListP)malloc(sizeof(TimeOrderedListP));
		lru_manager->_time_ordered_list->_node_order_head = NULL;
		lru_manager->_time_ordered_list->_node_order_tail = NULL;
		
		*ppLRUManager = lru_manager;
		eStatus = S_SUCCESS;
	}
	return eStatus;
}

size_t GetBucketCount(const char* key_name, int buckets) {
	size_t bucket_no = 0;
	for (size_t i = 0; i < strlen(key_name); ++i) {
		bucket_no += ((int)key_name[i] * 33);
	}
	return (bucket_no % buckets);
}

// Assumption: Init() called
StatusE AddItem(LRUManagerP pLRUManager, const char* file_name) {
	StatusE eStatus = S_ERROR;

	if (NULL != pLRUManager) {
		size_t index = GetBucketCount(file_name, HASH_BUCKETS);
		HashP hash = pLRUManager->_lru_hash;
		NodeP working_node = hash->_node_buckets[index];
		if (NULL == working_node) {
			// Create new node and add to HashT
			// Update TimeOrderedListT
			NodeP new_node = (NodeP) malloc(sizeof(NodeP));
			new_node->_info = CreateInfo(file_name);
			new_node->_node_prev = NULL;
			new_node->_node_next = NULL;
			new_node->_node_hash_prev = NULL;
			new_node->_node_hash_next = NULL;
			new_node->_hash_index = index;
			hash->_node_buckets[index] = new_node;
			working_node = new_node;
			
			++pLRUManager->_entries;
			eStatus = S_SUCCESS;			
		} else { // collision
			// Checking if entry exists
			NodeP node = working_node;
			for (; NULL != node; node = node->_node_hash_next) {
				if (0 == (strcpy(node->_info->_file_name, file_name))) {
					// node exists. We have to get the node to front of TimeOrderedListP
					working_node = node;
					assert("hash idx dont match" && (index == working_node->_hash_index));
					eStatus = S_PRESENT;
					break;
				}
			} // for hash node
			if (NULL == node) {
				// entry does not exist
				NodeP new_node = (NodeP) malloc(sizeof(NodeP));
				new_node->_info = CreateInfo(file_name);
				new_node->_node_prev = NULL;
				new_node->_node_next = NULL;
				new_node->_node_hash_prev = NULL;
				new_node->_node_hash_next = NULL;
				new_node->_hash_index = index;
				// As this is lastest we will set the new node at head of bucket
				NodeP bucket_node = hash->_node_buckets[index];
				assert("head bucket prev is null"
				       && (NULL == bucket_node->_node_hash_prev));
				// new-node
				new_node->_node_hash_next = bucket_node;
				new_node->_node_hash_prev = NULL;
				// bucket's new node
				bucket_node->_node_hash_prev = new_node;
				// set the bucket
				hash->_node_buckets[index] = new_node;
				
				bucket_node->_node_next = new_node;

				++pLRUManager->_entries;

				working_node = new_node;
				eStatus = S_SUCCESS;			
			} // node does not exist
		} // collision

		FindAndBringAhead(pLRUManager, working_node);
		// Will only delete last element if max element reached
		RemoveItem(pLRUManager);
	}	
	return eStatus;
}

StatusE FindAndBringAhead(LRUManagerP pLRUManager, NodeP recent_node) {
	StatusE eStatus = S_ERROR;
	TimeOrderedListP order_list = pLRUManager->_time_ordered_list;
	if (1 == pLRUManager->_entries) {
		order_list->_node_order_head = recent_node;
		order_list->_node_order_tail = recent_node;
		// as this is the first node
		recent_node->_node_next = NULL;
		recent_node->_node_prev = NULL;
		eStatus = S_UPDATE;
	} else {
		// Update the TimeOrderedListP
		//
		// The working_node is the latest node. Its set for 3 cases
		// 1. Newly added node in Hash
		// 2. Collision -> Node exists
		// 3. Collision -> Newly added node
		
		//order_list
		if (order_list->_node_order_head == recent_node) {
			// its already in ahead
			return S_UPDATE;
		} else {
			// its in middle or end
			// recent_node -> prev; check: 
			if (order_list->_node_order_tail == recent_node) {
				// set new tail 
				order_list->_node_order_tail = order_list->_node_order_tail->_node_prev;
			}
			recent_node->_node_prev->_node_next = recent_node->_node_next;
			// recent_node -> next; check: 
			recent_node->_node_next->_node_prev = recent_node->_node_prev;
			// chcek: already assigned above
			recent_node->_node_prev = NULL;
			recent_node->_node_next = order_list->_node_order_head;
			order_list->_node_order_head->_node_prev = recent_node;
			// check: next need not be set
			// order_list->_node_order_head->_node_next; //
			order_list->_node_order_head = recent_node;
		}		
		eStatus = S_SUCCESS;
	}

	return eStatus;
}

StatusE RemoveItem(LRUManagerP pLRUManager) {
	StatusE eStatus = S_ERROR;
	if (pLRUManager->_entries == MAX_LRU_ENTRIES) {
		// remove the last
		NodeP to_remvoe = pLRUManager->_time_ordered_list->_node_order_tail;

		to_remvoe->_node_prev->_node_next = NULL;
		NodeP node_in_hash
			= pLRUManager->_lru_hash->_node_buckets[to_remvoe->_hash_index];
		if (node_in_hash == to_remvoe ) {
			assert("prev should be null" && (NULL == node_in_hash->_node_hash_prev));
			pLRUManager->_lru_hash->_node_buckets[to_remvoe->_hash_index]
				= node_in_hash->_node_hash_next;
			node_in_hash->_node_hash_next->_node_hash_prev = NULL;
			// check: use "_node_hosh_" ptr
		} else {
			// check: use "_node_hosh_" ptr
			for (; NULL != node_in_hash; node_in_hash = node_in_hash->_node_hash_next) {
				if (0 == (strcmp(node_in_hash->_info->_file_name,
				                 to_remvoe->_info->_file_name))) {
					// found
					if ((NULL != node_in_hash) && (NULL != node_in_hash->_node_hash_prev)) {
						pLRUManager->_lru_hash->_node_buckets[to_remvoe->_hash_index]
							= node_in_hash->_node_hash_next;						
						node_in_hash->_node_hash_prev->_node_hash_next
							= node_in_hash->_node_hash_next;
					}
					if ((NULL != node_in_hash) && (NULL != node_in_hash->_node_hash_next)) {
						pLRUManager->_lru_hash->_node_buckets[to_remvoe->_hash_index]
							= node_in_hash->_node_hash_next;						
						node_in_hash->_node_hash_next->_node_hash_prev
							= node_in_hash->_node_hash_prev;
					}
				}
			} // for middle or end			
		}
		--pLRUManager->_entries;
		free (to_remvoe);
	}
	return eStatus;
};

// As this is a lib, using the main just to test compilation with g++
int main() {
	return 0;
}
