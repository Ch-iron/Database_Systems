#ifndef __FILE_H__
#define __FILE_H__

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
//#include <pthread.h>

#define PAGE_SIZE 4096
#define PAGE_HEADER 128
#define INTERNAL_ORDER (PAGE_SIZE - PAGE_HEADER)/16
#define LEAF_ORDER (PAGE_SIZE - PAGE_HEADER)/128


typedef uint64_t pagenum_t;

typedef struct header{
    pagenum_t free;
    pagenum_t root;
    uint64_t numpage;
    char Reserved[PAGE_SIZE - (sizeof(pagenum_t) * 3)];
}header;

typedef struct record{
    uint64_t key;
    char value[120];
}record;

typedef struct internal_record{
    uint64_t key;
    pagenum_t page_num;
}internal_record;

typedef struct internal_page{
	pagenum_t NoP;
	int isleaf;
	int num_keys;
    uint64_t page_LSN;
    char Reserved[PAGE_HEADER - (sizeof(pagenum_t) * 3) - (sizeof(int) * 2)];
    pagenum_t otherp;
	internal_record data[INTERNAL_ORDER];
}internal_page;

typedef struct leaf_page{
	pagenum_t NoP;
	int isleaf;
	int num_keys;
    uint64_t page_LSN;
    char Reserved[PAGE_HEADER - (sizeof(pagenum_t) * 3) - (sizeof(int) * 2)];
    pagenum_t otherp;
	record data[LEAF_ORDER];
}leaf_page;

typedef struct page_t{
    pagenum_t NoP;
    int isleaf;
    int num_keys;
    uint64_t page_LSN;
    char Reserved[PAGE_HEADER - (sizeof(pagenum_t) * 3) - (sizeof(int) * 2)];
    pagenum_t otherp;
    char KeyArea[PAGE_SIZE - PAGE_HEADER];
}page_t;

#pragma pack(push, 1)
typedef struct BCRlog{
    int Logsize;
    uint64_t LSN;
    uint64_t prev_LSN;
    int trx_id;
    int type;
}BCRlog;

typedef struct Ulog{
    int Logsize;
    uint64_t LSN;
    uint64_t prev_LSN;
    int trx_id;
    int type;
    int table_id;
    pagenum_t page_num;
    int offset;
    int data_length;
    char old_image[120];
    char new_image[120];
}Ulog;

typedef struct Comlog{
    int Logsize;
    uint64_t LSN;
    uint64_t prev_LSN;
    int trx_id;
    int type;
    int table_id;
    pagenum_t page_num;
    int offset;
    int data_length;
    char old_image[120];
    char new_image[120];
    uint64_t next_undo_LSN;
}Comlog;
#pragma pack(pop)

extern int fd;
extern char* pathname;
extern int num_buf;
extern int log_fd;
extern int commit_fd;
extern Comlog** log_buffer;
extern pthread_mutex_t log_buffer_latch;

//FILE I/O
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);
void file_create(char* pathname);
void file_open(char* pathname);
header* get_headerpage();
void write_headerpage(header* head);
void make_headerpage(pagenum_t fnum, pagenum_t rnum, uint64_t num);
void commit_create(char* commit_path);
void commit_open(char* commit_path);
void commit_read(int* commit_table, int* final_trx_id);
void commit_write(int commit_trx, int final_trx_id);
void log_create(char* log_path);
void log_open(char* log_path);
void logmsg_open(char* logmsg_path);
int logfile_offset();
Comlog* log_read(int begin, int up, int com);
Comlog* log_read_offset(int off_set);
void BCRlog_wirte(BCRlog* log);
void Ulog_write(Ulog* log);
void Comlog_write(Comlog* log);
void log_write();

#endif
