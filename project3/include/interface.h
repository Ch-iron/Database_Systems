#ifndef __INTERFACE_H__
#define __INTERFACE_H__

#include "disk_bpt.h"
#include "buffer.h"

typedef struct table{
	char* pathname;
	int table_id;
	int fd;
}table;

extern int fd;
extern char* pathname;
extern char* value;
extern int table_num;
extern int num_buf;
extern int table_id;
extern buffer_st** buffer;
extern buffer_st* LRU_head;
extern buffer_st* LRU_tail;
extern table** table_list;

int init_db(int num_buf);
int shutdown_db();
int open_table(char* pathname);
int close_table(int table_id);
int db_find(int table_id, int64_t key, char* ret_value);
int db_insert(int table_id, uint64_t key, char* value);
int db_delete(int table_id, uint64_t key);

#endif
