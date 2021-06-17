#ifndef __INTERFACE_H__
#define __INTERFACE_H__

#include "disk_bpt.h"
#include "buffer.h"
#include "lock_table.h"

#define UPDATE_NUM 1000

extern int fd;
extern char* pathname;
extern char* value;
extern int table_num;
extern int num_buf;
extern int id_table;
extern buffer_st** buffer;
extern buffer_st* LRU_head;
extern buffer_st* LRU_tail;
extern table** table_list;
extern pthread_mutex_t lock_table_latch;
extern pthread_mutex_t trx_table_latch;
extern pthread_mutex_t buffer_latch;
//extern trx_t** trx_table;

int init_db(int bufnum);
int shutdown_db();
int open_table(char* pathname);
int close_table(int table_id);
int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);
int db_find(int table_id, int64_t key, char* ret_value, int trx_id);
int db_update(int table_id, int64_t key, char* values, int trx_id);
int db_insert(int table_id, uint64_t key, char* value);
int db_delete(int table_id, uint64_t key);

#endif
