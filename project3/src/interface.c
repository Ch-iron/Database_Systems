#include "interface.h"

table** table_list;
int table_num = 0;
buffer_st** buffer;
buffer_st* LRU_head;
buffer_st* LRU_tail;
char* pathname;
char* value;
int num_buf;
int table_id;

int init_db(int num_buf){
	int i;

	if(num_buf == 0){
		return -1;
	}
	table_list = (table**)calloc(10, sizeof(table*));
	for(i = 0; i < 10; i++){
		table_list[i] = (table*)calloc(1, sizeof(table));
		table_list[i]->pathname = (char*)calloc(1, sizeof(char));
	}
	pathname = (char*)calloc(1, sizeof(char));
	value = (char*)calloc(1, sizeof(char));

	buffer = (buffer_st**)calloc(num_buf, sizeof(buffer_st*));	
	for(i = 0; i < num_buf; i++){
		buffer[i] = (buffer_st*)calloc(1, sizeof(buffer_st));
		buffer[i]->frame = (page_t*)calloc(1, PAGE_SIZE);
	}
	for(i = 0; i < num_buf; i++){
		if(i != 0 && i != num_buf - 1){
			buffer[i]->prev_LRU = buffer[i - 1];
			buffer[i]->next_LRU = buffer[i + 1];
		}
		else if(i == 0){
			LRU_head = buffer[i];
			buffer[i]->prev_LRU = NULL;
			buffer[i]->next_LRU = buffer[i + 1];
		}
		else if(i == num_buf - 1){
			LRU_tail = buffer[i];
			buffer[i]->prev_LRU = buffer[i - 1];
			buffer[i]->next_LRU = NULL;
		}
	}
	if(buffer != NULL){
		//printf("buffer allocated!!!!!!\n");
		return 0;
	}
	else{
		//printf("buffer allocate Fail~~~~~\n");
		return -1;
	}
}

int shutdown_db(){
	int i;

	for(i = 0; i < num_buf; i++){
		if(buffer[i]->is_dirty == 1){
			table_id = buffer[i]->table_id;
			fd = table_list[table_id - 1]->fd;
			frame_flush(buffer[i]->page_num, buffer[i]);
		}
	}
	for(i = 0; i < 10; i++){
		free(table_list[i]->pathname);
		free(table_list[i]);
	}
	free(table_list);
	for(i = 0; i < num_buf; i++){
		free(buffer[i]->frame);
		free(buffer[i]);
	}
	free(buffer);
	if(buffer != NULL){
		return 0;
	}
	else
		return -1;
}

int open_table(char* pathname){
	int i;

	for(i = 0; i < 10; i++){
		if(strcmp(pathname, table_list[i]->pathname) == 0 && table_list[i]->fd == 0){
			table_list[i]->fd = start_table(pathname);
			//printf("table_id : %d\n", table_list[i]->table_id);
			return table_list[i]->table_id;
		}
		else if(strcmp(pathname, table_list[i]->pathname) == 0 && table_list[i]->fd != 0){
			//printf("file already open!!!!!!!!!!!!!\ntable_id : %d\nfile's fd : %d\n", table_list[i]->table_id, table_list[i]->fd);
			return table_list[i]->table_id;
		}
	}
	if(i == 10 && table_num == 10){
		//printf("table_list full!!!!!!!!!!!!!!\n");
		return -1;
	}
	if(i == 10 && table_num < 10){
		for(i = 0; i < 10; i++){
			if(table_list[i]->table_id == 0){
				table_num++;
				table_list[i]->table_id = table_num;
				sprintf(table_list[i]->pathname, "%s", pathname);
				table_id = table_list[i]->table_id;
				fd = start_table(pathname);
				table_list[i]->fd = fd;
				//printf("Table id allocate %d\n", table_list[i]->table_id);
				return table_list[i]->table_id;
			}
		}
	}
}

int close_table(int table_id){
	int i;

	if(table_list[table_id - 1]->fd == 0){
		//printf("file not open\n");
		return -1;
	}
	else{
		fd = table_list[table_id - 1]->fd;
		for(i = 0; i < num_buf; i++){
			if(buffer[i]->is_dirty == 1 && buffer[i]->table_id == table_id){
				frame_flush(buffer[i]->page_num, buffer[i]);
			}
		}
		close(fd);
		table_list[table_id - 1]->fd = 0;
		return 0;
	}
}

int db_find(int table_id, int64_t key, char* ret_value){
	int i;
    record* find_record;

	fd = table_list[table_id - 1]->fd;
	//printf("This table is %s\n", table_list[table_id - 1]->pathname);
    if(fd >= 3){
        find_record = find(key);
        if(find_record == NULL){
			all_unpinning(table_id);
            return -1;
		}
        sprintf(ret_value, "%s", find_record->value);
		all_unpinning(table_id);
        return 0;
    }
    else{
		all_unpinning(table_id);
		return -1;
	}
}

int db_insert(int table_id, uint64_t key, char* value){
    record* insert_record;
    header* head;
    page_t* leafpage;
    pagenum_t leafpagenum;

	fd = table_list[table_id - 1]->fd;
    if(fd >= 3){
        insert_record = make_record(key, value);
		head = read_buffer_headerpage();
        if(head->root == 0){
			unpinning_headerpage();
            start_new_tree(insert_record);
            return 0;
        }
		unpinning_headerpage();
        if(find(key) != NULL){
			all_unpinning(table_id);
            //printf("Input key exists.\n");
            return -1;
        }
        leafpagenum = find_leafpage(key);
		leafpage = read_buffer_page(leafpagenum);
        if(leafpage->num_keys < LEAF_ORDER - 1){
			unpinning_page(leafpagenum);
            insert_into_leafpage(leafpagenum, insert_record);
        }
        else{
			unpinning_page(leafpagenum);
            insert_into_leafpage_after_splitting(leafpagenum, insert_record);
		}
		all_unpinning(table_id);
		return 0;
    }
    else{
		all_unpinning(table_id);
		return -1;
    }
}

int db_delete(int table_id, uint64_t key){
    record* key_record;
    pagenum_t key_leafpage_num;

	fd = table_list[table_id - 1]->fd;
    if(fd >= 3){
        key_record = find(key);
        key_leafpage_num = find_leafpage(key);
        if(key_record != NULL){
            delete_entry(key_leafpage_num, key);
			all_unpinning(table_id);
            return 0;
        }
        else{
		    all_unpinning(table_id);
            //printf("Input key not exists.\n");
            return -1;
        }
    }
    else{
	all_unpinning(table_id);
	return -1;
    }
}

