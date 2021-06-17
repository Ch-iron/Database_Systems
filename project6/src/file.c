#include "file.h"

#define LOG_NUM 10000

int fd;
int log_fd;
int commit_fd;
//int logmsg_fd;

//file read/write page
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

//commit table file read/write/open
void commit_create(char* commit_path){
    commit_fd = open(commit_path, O_RDWR | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU | S_IRWXG | S_IRWXO);
}

void commit_open(char* commit_path){
    commit_fd = open(commit_path, O_RDWR | O_SYNC, S_IRWXU | S_IRWXG | S_IRWXO);
}

void commit_read(int* commit_table, int* final_trx_id){
    int end_offset;
    int n, m;
    int i = 0;

    m = lseek(commit_fd, 0, SEEK_END);
    if(m != 0){
        end_offset = lseek(commit_fd, -sizeof(int), SEEK_END);
        read(commit_fd, final_trx_id, sizeof(int));
        n = lseek(commit_fd, 0, SEEK_SET);
        while(n < end_offset){
            read(commit_fd, &commit_table[i], sizeof(int));
            i++;
            n = lseek(commit_fd, 0, SEEK_CUR);
        }
    }
}

void commit_write(int commit_trx, int final_trx_id){
    int n;

    n = lseek(commit_fd, 0, SEEK_CUR);
    if(n == 0){
        write(commit_fd, &commit_trx, sizeof(int));
    }
    else{
        lseek(commit_fd, -(sizeof(int)), SEEK_END);
        write(commit_fd, &commit_trx, sizeof(int));
    }
    write(commit_fd, &final_trx_id, sizeof(int));
    fsync(commit_fd);
}

//log file read/write/open
void log_create(char* log_path){
    log_fd = open(log_path, O_RDWR | O_CREAT | O_TRUNC | O_SYNC | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
    //logmsg_fd = open(log_path, O_RDWR | O_CREAT | O_TRUNC | O_SYNC | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
}

void log_open(char* log_path){
    log_fd = open(log_path, O_RDWR | O_SYNC | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
}

/*void logmsg_open(char* logmsg_path){
    logmsg_fd = open(logmsg_path, O_RDWR | O_SYNC | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
}*/

int logfile_offset(){
    int n;

    n = lseek(log_fd, 0, SEEK_END);
    return n;
}

Comlog* log_read(int begin, int up, int com){
    BCRlog* bcr;
    Ulog* update;
    Comlog* compensate;
    int off_set;
    int n;

    bcr = (BCRlog*)calloc(1, sizeof(BCRlog));
    update = (Ulog*)calloc(1, sizeof(Ulog));
    compensate = (Comlog*)calloc(1, sizeof(Comlog));
    off_set = (begin * sizeof(BCRlog)) + (up * sizeof(Ulog)) + (com * sizeof(Comlog));
    n = lseek(log_fd, 0, SEEK_END);
    if(off_set == n){
        return NULL;
    }

    pread(log_fd, bcr, sizeof(BCRlog), off_set);
    if(bcr->type == 0 || bcr->type == 2 || bcr->type == 3){
        compensate->Logsize = bcr->Logsize;
        compensate->LSN = bcr->LSN;
        compensate->prev_LSN = bcr->prev_LSN;
        compensate->trx_id = bcr->trx_id;
        compensate->type = bcr->type;
        return compensate;
    }
    else if(bcr->type == 1){
        pread(log_fd, update, sizeof(Ulog), off_set);
        compensate->Logsize = update->Logsize;
        compensate->LSN = update->LSN;
        compensate->prev_LSN = update->prev_LSN;
        compensate->trx_id = update->trx_id;
        compensate->type = update->type;
        compensate->table_id = update->table_id;
        compensate->page_num = update->page_num;
        compensate->offset = update->offset;
        compensate->data_length = update->data_length;
        sprintf(compensate->old_image, "%s", update->old_image);
        sprintf(compensate->new_image, "%s", update->new_image);
        return compensate;
    }
    else if(bcr->type == 4){
        pread(log_fd, compensate, sizeof(Comlog), off_set);
        return compensate;
    }
}

Comlog* log_read_offset(int off_set){
    BCRlog* bcr;
    Ulog* update;
    Comlog* compensate;

    bcr = (BCRlog*)calloc(1, sizeof(BCRlog));
    update = (Ulog*)calloc(1, sizeof(Ulog));
    compensate = (Comlog*)calloc(1, sizeof(Comlog));

    pread(log_fd, bcr, sizeof(BCRlog), off_set);
    if(bcr->type == 0 || bcr->type == 2 || bcr->type == 3){
        compensate->Logsize = bcr->Logsize;
        compensate->LSN = bcr->LSN;
        compensate->prev_LSN = bcr->prev_LSN;
        compensate->trx_id = bcr->trx_id;
        compensate->type = bcr->type;
        return compensate;
    }
    else if(bcr->type == 1){
        pread(log_fd, update, sizeof(Ulog), off_set);
        compensate->Logsize = update->Logsize;
        compensate->LSN = update->LSN;
        compensate->prev_LSN = update->prev_LSN;
        compensate->trx_id = update->trx_id;
        compensate->type = update->type;
        compensate->table_id = update->table_id;
        compensate->page_num = update->page_num;
        compensate->offset = update->offset;
        compensate->data_length = update->data_length;
        sprintf(compensate->old_image, "%s", update->old_image);
        sprintf(compensate->new_image, "%s", update->new_image);
        return compensate;
    }
    else if(bcr->type == 4){
        pread(log_fd, compensate, sizeof(Comlog), off_set);
        return compensate;
    }
}

void BCRlog_wirte(BCRlog* log){
    write(log_fd, log, sizeof(BCRlog));
    fsync(log_fd);
}

void Ulog_write(Ulog* log){
    write(log_fd, log, sizeof(Ulog));
    fsync(log_fd);
}

void Comlog_write(Comlog* log){
    write(log_fd, log, sizeof(Comlog));
    fsync(log_fd);
}

void log_write(){
    BCRlog* bcr;
    Ulog* update;
    Comlog* compensate;
    int i;

    bcr = (BCRlog*)calloc(1, sizeof(BCRlog));
    update = (Ulog*)calloc(1, sizeof(Ulog));
    compensate = (Comlog*)calloc(1, sizeof(Comlog));

    //pthread_mutex_lock(&log_buffer_latch);
    for(i = 0; i < LOG_NUM; i++){
        if(log_buffer[i]->LSN == 1){
            break;
        }
        if(log_buffer[i]->type == 0 || log_buffer[i]->type == 2 || log_buffer[i]->type == 3){
            bcr->Logsize = log_buffer[i]->Logsize;
            bcr->LSN = log_buffer[i]->LSN;
            bcr->prev_LSN = log_buffer[i]->prev_LSN;
            bcr->trx_id = log_buffer[i]->trx_id;
            bcr->type = log_buffer[i]->type;
            BCRlog_wirte(bcr);

            //initialize
            log_buffer[i]->LSN = 1;
        }
        else if(log_buffer[i]->type == 1){
            update->Logsize = log_buffer[i]->Logsize;
            update->LSN = log_buffer[i]->LSN;
            update->prev_LSN = log_buffer[i]->prev_LSN;
            update->trx_id = log_buffer[i]->trx_id;
            update->type = log_buffer[i]->type;
            update->table_id = log_buffer[i]->table_id;
            update->page_num = log_buffer[i]->page_num;
            update->offset = log_buffer[i]->offset;
            update->data_length = log_buffer[i]->data_length;
            sprintf(update->old_image, "%s", log_buffer[i]->old_image);
            sprintf(update->new_image, "%s", log_buffer[i]->new_image);
            Ulog_write(update);

            //initialize
            log_buffer[i]->LSN = 1;
        }
        else if(log_buffer[i]->type == 4){
            compensate = log_buffer[i];
            Comlog_write(compensate);

            //initialize
            log_buffer[i]->LSN = 1;
        }
    }
    //pthread_mutex_unlock(&log_buffer_latch);
}