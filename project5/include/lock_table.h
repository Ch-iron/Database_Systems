#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define HASH_NUM 1000
#define TRX_NUM 1000

typedef struct lock_t lock_t;
typedef struct hash_t hash_t;

struct lock_t {
	lock_t* prev_lobj;
	lock_t* next_lobj;
	hash_t* sentinel;
	pthread_cond_t cond;
	int lock_mode;
	lock_t* trx_next_lobj;
	int owner_trx_id;
	int is_wait;
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

typedef struct trx_t{
	int trx_id;
	lock_t* next_lobj;
	int xlock_count;
	char** undo_log;
}trx_t;

/* APIs for lock table */
int hash_function(int table_id, int64_t key);
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
int deadlock_situation(lock_t* wait_lobj, trx_t* trx);
int deadlock_detect(lock_t* possible_deadlock);
int lock_release(lock_t* lock_obj);
int abort_lock_release(lock_t* lock_obj);

extern pthread_mutex_t lock_table_latch;
extern pthread_mutex_t trx_table_latch;
extern pthread_mutex_t buffer_latch;
extern index_t** hash_table;
extern int global_trx_id;
extern trx_t** trx_table;

#endif /* __LOCK_TABLE_H__ */
