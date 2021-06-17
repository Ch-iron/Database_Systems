#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#define HASH_NUM 5

typedef struct lock_t lock_t;
typedef struct hash_t hash_t;

struct lock_t {
	lock_t* prev_lobj;
	lock_t* next_lobj;
	hash_t* sentinel;
	pthread_cond_t cond;
};

struct hash_t{
	int table_id;
	int record_id;
	lock_t* tail;
	lock_t* head;
	hash_t* next;
};

typedef struct index_t{
	int index;
	hash_t* hash_entry;
}index_t;

/* APIs for lock table */
int hash_function(int table_id, int64_t key);
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

pthread_mutex_t lock_table_latch;
index_t** hash_table;

#endif /* __LOCK_TABLE_H__ */
