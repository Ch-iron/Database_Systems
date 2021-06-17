#ifndef __FILE_H__
#define __FILE_H__

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

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
    char Reserved[PAGE_HEADER - (sizeof(pagenum_t) * 2) - (sizeof(int) * 2)];
    pagenum_t otherp;
	internal_record data[INTERNAL_ORDER];
}internal_page;

typedef struct leaf_page{
	pagenum_t NoP;
	int isleaf;
	int num_keys;
    char Reserved[PAGE_HEADER - (sizeof(pagenum_t) * 2) - (sizeof(int) * 2)];
    pagenum_t otherp;
	record data[LEAF_ORDER];
}leaf_page;

typedef struct page_t{
    pagenum_t NoP;
    int isleaf;
    int num_keys;
    char Reserved[PAGE_HEADER - (sizeof(pagenum_t) * 2) - (sizeof(int) * 2)];
    pagenum_t otherp;
    char KeyArea[PAGE_SIZE - PAGE_HEADER];
}page_t;

extern int fd;
extern char* pathname;
extern int num_buf;

//FILE I/O
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);
void file_create(char* pathname);
void file_open(char* pathname);
header* get_headerpage();
void write_headerpage(header* head);
void make_headerpage(pagenum_t fnum, pagenum_t rnum, uint64_t num);

#endif
