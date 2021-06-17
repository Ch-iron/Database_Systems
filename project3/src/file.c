#include "file.h"

int fd;

//fild read/write page
void file_read_page(pagenum_t pagenum, page_t* dest){
    pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

void file_write_page(pagenum_t pagenum, const page_t* src){
    pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
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

//make headerpage
void make_headerpage(pagenum_t fnum, pagenum_t rnum, uint64_t num){
    header* head;

    head = (header*)calloc(1, PAGE_SIZE);
    head->free = fnum;
    head->root = rnum;
    head->numpage = num;
    write_headerpage(head);
    free(head);
} 

