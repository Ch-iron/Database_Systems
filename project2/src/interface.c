#include "interface.h"

int open_table(char* pathname){
	if(access(pathname, F_OK) != 0){
		file_create(pathname);
		create_new_file();
	}
	else
		file_open(pathname);
    return fd - 2;
}

int db_find(int64_t key, char* ret_value){
    record* find_record;

    if(fd >= 3){
        find_record = find(key);
        if(find_record == NULL)
            return -1;
        sprintf(ret_value, "%s", find_record->value);
        return 0;
    }
    else return -1;
}

int db_insert(uint64_t key, char* value){
    record* insert_record;
    header* head;
    page_t* leafpage;
    pagenum_t leafpagenum;

    if(fd >= 3){
        leafpage = (page_t*)calloc(1, PAGE_SIZE);
        insert_record = make_record(key, value);
        head = get_headerpage();
        if(head->root == 0){
            start_new_tree(insert_record);
            return 0;
        }
        if(find(key) != NULL){
            printf("Input key exists.\n");
            return -1;
        }
        leafpagenum = find_leafpage(key);
        file_read_page(leafpagenum, leafpage);
        if(leafpage->num_keys < LEAF_ORDER - 1){
            insert_into_leafpage(leafpagenum, insert_record);
        }
        else
            insert_into_leafpage_after_splitting(leafpagenum, insert_record);
        return 0;
    }
    else return -1;
}

int db_delete(uint64_t key){
    record* key_record;
    pagenum_t key_leafpage_num;

    if(fd >= 3){
        key_record = find(key);
        key_leafpage_num = find_leafpage(key);
        if(key_record != NULL){
            delete_entry(key_leafpage_num, key);
            free(key_record);
            return 0;
        }
        else{
            printf("Input key not exists.\n");
            return -1;
        }
    }
    else return -1;
}

