#ifndef __INTERFACE_H__
#define __INTERFACE_H__

#include "disk_bpt.h"

extern int fd;
extern char* pathname;

int open_table(char* pathname);
int db_find(int64_t key, char* ret_value);
int db_insert(uint64_t key, char* value);
int db_delete(uint64_t key);

#endif
