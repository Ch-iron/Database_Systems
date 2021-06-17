#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "file.h"

typedef struct buffer_st{
	page_t* frame;
	int table_id;
	pagenum_t page_num;
	int is_dirty;
	int is_pinned;
	struct buffer_st* prev_LRU;
	struct buffer_st* next_LRU;
}buffer_st;

typedef struct buffer_header{
	header* header;
	int table_id;
	pagenum_t page_num;
	int is_dirty;
	int is_pinned;
	struct buffer_st* prev_LRU;
	struct buffer_st* next_LRU;
}buffer_header;

extern int fd;
extern char* pathname;
extern int num_buf;
extern int table_id;
extern buffer_st* LRU_head;
extern buffer_st* LRU_tail;
extern buffer_st** buffer;

//create new something
void create_new_file();
void make_new_freepage();
int start_table(char* pathname);

//Buffer function
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void pinning(buffer_st* pin_frame);
void unpinning(buffer_st* unpin_frame);
void all_unpinning(int table_id);
void modified_frame(buffer_st* modify_frame);
void LRU_update(buffer_st* using_frame);
buffer_st* LRU_choice();
header* read_buffer_headerpage();
void unpinning_headerpage();
void write_buffer_headerpage();
page_t* read_buffer_page(pagenum_t page_num);
void unpinning_page(pagenum_t page_num);
void write_buffer_page(pagenum_t page_num);
void frame_flush(pagenum_t page_num, buffer_st* drop);
void drop_frame(pagenum_t page_num, buffer_st* drop);
#endif
