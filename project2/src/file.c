#include "file.h"

int fd;
char* pathname;

pagenum_t file_alloc_page(){
	pagenum_t allocpage_num;
	page_t* allocpage;
    header* head;

    head = get_headerpage();
    allocpage = (page_t*)calloc(1, PAGE_SIZE);
	allocpage_num = head->free;
    file_read_page(allocpage_num, allocpage);
    head->free = allocpage->NoP;
    allocpage->NoP = 0;
    allocpage->isleaf = 0;
	allocpage->num_keys = 0;
    allocpage->otherp = 0;
    file_write_page(allocpage_num, allocpage);
    write_headerpage(head);
    free(allocpage);
    return allocpage_num;
}

void file_free_page(pagenum_t pagenum){
	header* head;
    page_t* page;
    pagenum_t last_freepage_num, next_freepage_num;

	head = get_headerpage();
    page = (page_t*)calloc(1, PAGE_SIZE);
    file_write_page(pagenum, page);
	file_read_page(pagenum, page);
	page->NoP = head->free;
	head->free = pagenum;
	file_write_page(pagenum, page);
	write_headerpage(head);
}

//fild read/write page

void file_read_page(pagenum_t pagenum, page_t* dest){
    pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

void file_write_page(pagenum_t pagenum, const page_t* src){
    pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
    fsync(fd);
}

//file read/write record

void file_read_record(pagenum_t pagenum, record* dest, int i){
    pread(fd, dest, sizeof(record), (pagenum * PAGE_SIZE) + PAGE_HEADER + (i * sizeof(record)));
}

void file_write_record(pagenum_t pagenum, const record* src, int i){
    pwrite(fd, src, sizeof(record), (pagenum * PAGE_SIZE) + PAGE_HEADER + (i * sizeof(record)));
    fsync(fd);
}

//file read/write internal_record

void file_read_internal_record(pagenum_t pagenum, internal_record* dest, int i){
    pread(fd, dest, sizeof(internal_record), (pagenum * PAGE_SIZE) + PAGE_HEADER + (i * sizeof(internal_record)));
}
 
void file_write_internal_record(pagenum_t pagenum, const internal_record* src, int i){
    pwrite(fd, src, sizeof(internal_record), (pagenum * PAGE_SIZE) + PAGE_HEADER + (i * sizeof(internal_record)));
    fsync(fd);
}

//file open

void file_create(char* pathname){
    fd = open(pathname, O_RDWR | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU | S_IRWXG | S_IRWXO);
}
void file_open(char* pathname){
    fd = open(pathname, O_RDWR | O_SYNC, S_IRWXU | S_IRWXG | S_IRWXO);
}

//read/write headerpage

header* get_headerpage(){
    header* head;
    head = (header*)calloc(1, PAGE_SIZE);
    pread(fd, head, PAGE_SIZE, 0);
    return head;
}

void write_headerpage(header* head){
    pwrite(fd, head, PAGE_SIZE, 0);
    fsync(fd);
}

