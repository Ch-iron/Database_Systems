#ifndef __DISK_BPT_H__
#define __DISK_BPT_H__

#include "file.h"

extern int fd;
extern char* pathname;

//Utility
pagenum_t get_rootpage_num();
int cut(int length);

//Open
void make_headerpage(pagenum_t fnum, pagenum_t rnum, uint64_t num);
void create_new_file();

//Find
pagenum_t find_leafpage(uint64_t key);
record* find(uint64_t key);

//Insertion
record* make_record(uint64_t key, char* value);
void make_new_freepage();
void make_leafpage(pagenum_t makeleaf);
int get_left_index(pagenum_t parentpagenum, pagenum_t leftpagenum);
void set_other_pagenum(pagenum_t n, pagenum_t other_pagenum);
void insert_into_leafpage(pagenum_t leaf_pagenum, record* insert_record);
void insert_into_leafpage_after_splitting(pagenum_t leafpage_num, record* insert_record);
void insert_into_nodepage(pagenum_t n, int left_index, internal_record* record, pagenum_t right);
void insert_into_nodepage_after_splitting(pagenum_t old_node, int left_index, internal_record* record, pagenum_t right);
void insert_into_parent(pagenum_t leftpagenum, internal_record* key, pagenum_t rightpagenum);
void insert_into_new_rootpage(pagenum_t leftpagenum, internal_record* key, pagenum_t rightpagenum);
void start_new_tree(record* insert_record);

//Deletion
int get_neighbor_index(pagenum_t pagenum);
void remove_entry_from_node(pagenum_t page_num, uint64_t key);
void moving_isolated_leaf(pagenum_t rootpage);
void adjust_root();
void coalesce_nodes(pagenum_t page_num, pagenum_t neighbor_num, int neighbor_index, uint64_t key);
void delete_entry(pagenum_t leafpage_num, uint64_t key);

#endif
