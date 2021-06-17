#include "interface.h"

table** table_list;
int table_num = 0;
buffer_st** buffer;
buffer_st* LRU_head;
buffer_st* LRU_tail;
char* pathname;
char* value;
int num_buf;
int id_table;
int global_trx_id;
pthread_mutex_t trx_table_latch;
trx_t** trx_table;
pthread_mutex_t global_latch = PTHREAD_MUTEX_INITIALIZER;

int init_db(int bufnum){
	int i, j;

	if(bufnum == 0){
		return -1;
	}

	num_buf = bufnum;
	table_list = (table**)calloc(10, sizeof(table*));
	for(i = 0; i < 10; i++){
		table_list[i] = (table*)calloc(1, sizeof(table));
		table_list[i]->pathname = (char*)calloc(1, sizeof(char));
	}

	//init trx_table
	trx_table = (trx_t**)calloc(TRX_NUM, sizeof(trx_t*));
	for(i = 0; i < TRX_NUM; i++){
		trx_table[i] = (trx_t*)calloc(1, sizeof(trx_t));
		trx_table[i]->trx_id = 0;
		trx_table[i]->next_lobj = NULL;
		trx_table[i]->xlock_count = 0;
		trx_table[i]->undo_log = (char**)calloc(UPDATE_NUM, sizeof(char*));
		//for(j = 0; j < UPDATE_NUM; j++){
		//	trx_table[i]->undo_log[j] = (char*)calloc(120, sizeof(char));
		//}
	}

	//init lock_table
	init_lock_table();

	pathname = (char*)calloc(20, sizeof(char));
	value = (char*)calloc(120, sizeof(char));

	buffer = (buffer_st**)calloc(num_buf, sizeof(buffer_st*));	
	for(i = 0; i < num_buf; i++){
		buffer[i] = (buffer_st*)calloc(1, sizeof(buffer_st));
		buffer[i]->frame = (page_t*)calloc(1, PAGE_SIZE);
		pthread_mutex_init(&buffer[i]->page_latch, NULL);
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
			id_table = buffer[i]->table_id;
			fd = table_list[id_table - 1]->fd;
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
			fd = table_list[i]->fd;
			id_table = table_list[i]->table_id;
			//printf("table_id : %d\n", table_list[i]->table_id);
			return table_list[i]->table_id;
		}
		else if(strcmp(pathname, table_list[i]->pathname) == 0 && table_list[i]->fd != 0){
			//printf("file already open!!!!!!!!!!!!!\ntable_id : %d\nfile's fd : %d\n", table_list[i]->table_id, table_list[i]->fd);
			fd = table_list[i]->fd;
			id_table = table_list[i]->table_id;
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
				id_table = table_list[i]->table_id;
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
		id_table = table_id;
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

int trx_begin(){
	int i;

	pthread_mutex_lock(&trx_table_latch);
	global_trx_id++;
	for(i = 0; i < TRX_NUM; i++){
		if(trx_table[i]->trx_id == 0 && trx_table[i]->next_lobj == NULL){
			trx_table[i]->trx_id = global_trx_id;
			break;
		}
	}
	if(trx_table[i]->trx_id == 0){
		return 0;
	}
	//printf("trx_begin!!!!! %d\n", trx_table[i]->trx_id);
	pthread_mutex_unlock(&trx_table_latch);
	return trx_table[i]->trx_id;
}

int trx_commit(int trx_id){
	int i, j;
	lock_t* lock_obj;
	lock_t* next_lobj;

	pthread_mutex_lock(&trx_table_latch);
	//printf("commit start!!!! %d\n", trx_id);
	for(i = 0; i < TRX_NUM; i++){
		if(trx_table[i]->trx_id == trx_id){
			lock_obj = trx_table[i]->next_lobj;
			while(lock_obj != NULL){
				next_lobj = lock_obj->trx_next_lobj;
				//printf("release lock!!!! table : %d key : %d\n", lock_obj->sentinel->table_id, lock_obj->sentinel->record_id);
				pthread_mutex_unlock(&trx_table_latch);
				lock_release(lock_obj);
				pthread_mutex_lock(&trx_table_latch);
				//printf("lock release!!! %d\n", trx_id);
				lock_obj = next_lobj;
			}
			//printf("Normally final lock release!!! %d\n", trx_id);
			trx_table[i]->trx_id = 0;
			trx_table[i]->next_lobj = NULL;
			for(j = 0; j < trx_table[i]->xlock_count; j++){
				free(trx_table[i]->undo_log[j]);
				trx_table[i]->undo_log[j] = NULL;
			}
			trx_table[i]->xlock_count = 0;
			//for(j = 0; j < UPDATE_NUM; j++){
			//	trx_table[i]->undo_log[j] = 0;
			//}
			if(trx_table[i]->next_lobj != NULL){
				pthread_mutex_unlock(&trx_table_latch);
				return 0;
			}
			break;
		}
	}
	//printf("commit finish!!! %d\n", trx_id);
	pthread_mutex_unlock(&trx_table_latch);
	return trx_id;
}

int trx_abort(int trx_id){
	int i, j, k, l;
	lock_t* lock_obj;
	lock_t* next_lobj;
	hash_t* entry;
	pagenum_t pagenum;
	leaf_page* leafpage;

	pthread_mutex_lock(&trx_table_latch);
	//printf("trx_abort start!!!! %d\n", trx_id);
	k = 0;
	for(i = 0; i < TRX_NUM; i++){
		if(trx_table[i]->trx_id == trx_id){
			lock_obj = trx_table[i]->next_lobj;
			//printf("lock_obj's mode : %d\n", lock_obj->lock_mode);
			while(lock_obj != NULL){
				//printf("lock_obj's mode : %d\n", lock_obj->lock_mode);
				next_lobj = lock_obj->trx_next_lobj;
				if(lock_obj->lock_mode == 1 && k < trx_table[i]->xlock_count){
					entry = lock_obj->sentinel;
					//printf("key = %d\n", entry->record_id);
					pthread_mutex_lock(&global_latch);
					fd = table_list[entry->table_id - 1]->fd;
					id_table = entry->table_id;
					pagenum = find_leafpage(entry->record_id);
					leafpage = (leaf_page*) read_buffer_page(pagenum);
					pthread_mutex_unlock(&global_latch);
					for(l = 0; l < leafpage->num_keys; l++){
						if(leafpage->data[l].key == entry->record_id){
						break;
						}
					}
					for(j = 0; j < sizeof(leafpage->data[i].value); j++){
						leafpage->data[l].value[j] = 0;
					}
					//printf("copy start!!! %d %s\n", k, trx_table[i]->undo_log[k]);
					sprintf(leafpage->data[l].value, "%s", trx_table[i]->undo_log[k]);
					//printf("copy finish!!\n");
					k++;
				}
				pthread_mutex_unlock(&trx_table_latch);
				abort_lock_release(lock_obj);
				pthread_mutex_lock(&trx_table_latch);
				//printf("lock release OK!!!!!!!!!!!!!!!!!!!\n");
				lock_obj = next_lobj;
			}
			trx_table[i]->trx_id = 0;
			trx_table[i]->next_lobj = NULL;
			for(j = 0; j < trx_table[i]->xlock_count; j++){
				free(trx_table[i]->undo_log[j]);
				trx_table[i]->undo_log[j] = NULL;
			}
			trx_table[i]->xlock_count = 0;
			//for(j = 0; j < UPDATE_NUM; j++){
			//	trx_table[i]->undo_log[j] = 0;
			//}
			break;
		}
	}
	if(trx_table[i]->next_lobj != NULL){
		pthread_mutex_unlock(&lock_table_latch);
		pthread_mutex_unlock(&trx_table_latch);
		return 0;
	}
	//printf("trx_abort finish!!!!! %d\n", trx_id);
	pthread_mutex_unlock(&lock_table_latch);
	pthread_mutex_unlock(&trx_table_latch);
	return trx_id;

}

int db_find(int table_id, int64_t key, char* ret_value, int trx_id){
	int i;
    record* find_record;
	leaf_page* leafpage;
	pagenum_t pagenum;

	pthread_mutex_lock(&global_latch);
	fd = table_list[table_id - 1]->fd;
	id_table = table_id;
	//printf("This table is %s\n", table_list[table_id - 1]->pathname);
    if(fd >= 3){
        //find_record = find(key);
		if(find(key) == NULL){
			//all_unpinning(table_id);
			pthread_mutex_unlock(&global_latch);
			trx_abort(trx_id);
            return -1;
		}
		pthread_mutex_unlock(&global_latch);
		if(lock_acquire(table_id, key, trx_id, 0) == NULL){
			//printf("deadlock occur!! abort!! Find trx:%d table:%d key:%ld\n", trx_id, table_id, key);
			pthread_mutex_unlock(&global_latch);
			//printf("global_latch release %d\n", trx_id);
			trx_abort(trx_id);
			//pthread_mutex_unlock(&global_latch);
			return -1;
		}
		//printf("deadlock detect pass!! update\n");
		pthread_mutex_lock(&global_latch);
		fd = table_list[table_id - 1]->fd;
		id_table = table_id;
		pagenum = find_leafpage(key);
		//printf("pagenum : %ld %ld\n", pagenum, key);
		leafpage = (leaf_page*) read_buffer_page(pagenum);
		pthread_mutex_unlock(&global_latch);
		for(i = 0; i < num_buf; i++){
			if(buffer[i]->page_num == pagenum && buffer[i]->table_id == id_table){
				//printf("buffer : %d\n", i);
				break;
			}
		}
		for(i = 0; i < leafpage->num_keys; i++){
			if(leafpage->data[i].key == key){
				break;
			}
		}
        sprintf(ret_value, "%s", leafpage->data[i].value);
		pthread_mutex_unlock(&buffer[i]->page_latch);
		//printf("page latch release\n");
		//all_unpinning(table_id);
		//printf("%d %ld Find value : %s %d\n", table_id, key, ret_value, trx_id);
        return 0;
    }
    else{
		//all_unpinning(table_id);
		return -1;
	}
}

int db_update(int table_id, int64_t key, char* values, int trx_id){
	record* find_record;
	pagenum_t pagenum;
	//page_t* page;
	leaf_page* leafpage;
	int undo_seq;
	int i, j;

	pthread_mutex_lock(&global_latch);
	fd = table_list[table_id - 1]->fd;
	id_table = table_id;
	if(fd >= 3){
		//find_record = find(key);
		if(find(key) == NULL){
			pthread_mutex_unlock(&global_latch);
			trx_abort(trx_id);
			return -1;
		}
		pthread_mutex_unlock(&global_latch);
		if(lock_acquire(table_id, key, trx_id, 1) == NULL){
			//printf("deadlock occur!! abort!! Update trx:%d table:%d key:%ld\n", trx_id, table_id, key);
			pthread_mutex_unlock(&global_latch);
			//printf("global_latch release %d\n", trx_id);
			trx_abort(trx_id);
			//pthread_mutex_unlock(&global_latch);
			return -1;
		}
		//printf("deadlock detect pass!! update\n");
		pthread_mutex_lock(&global_latch);
		fd = table_list[table_id - 1]->fd;
		id_table = table_id;
		pagenum = find_leafpage(key);
		//printf("find leaf finish!!\n");
		leafpage = (leaf_page*) read_buffer_page(pagenum);
		//printf("read leafpage finish!!\n");
		pthread_mutex_unlock(&global_latch);
		for(i = 0; i < leafpage->num_keys; i++){
			if(leafpage->data[i].key == key){
				break;
			}
		}
		//pthread_mutex_lock(&trx_table_latch);
		for(j = 0; j < TRX_NUM; j++){
			if(trx_table[j]->trx_id == trx_id){
				trx_table[j]->xlock_count++;
				undo_seq = trx_table[j]->xlock_count;
				trx_table[j]->undo_log[undo_seq - 1] = (char*)calloc(120, sizeof(char));
				break;
			}
		}
		//pthread_mutex_unlock(&trx_table_latch);
		//printf("where undo_log : %d %d %d\n", undo_seq, trx_id, j);
		sprintf(trx_table[j]->undo_log[undo_seq - 1], "%s", leafpage->data[i].value);
		//printf("undo_log : %d, %d, %s\n", trx_table[j]->trx_id, trx_table[j]->xlock_count, trx_table[j]->undo_log[undo_seq - 1]);
		sprintf(leafpage->data[i].value, "%s", values);
		//printf("test!!!!!!\n");
		write_buffer_page(pagenum);
		//printf("page latch release\n");
		//all_unpinning(table_id);
		//printf("%d %ld Update value : %s %d\n", table_id, key, leafpage->data[i].value, trx_id);
		return 0;
	}
	else{
		//all_unpinning(table_id);
		return -1;
	}
	
}

int db_insert(int table_id, uint64_t key, char* value){
    record* insert_record;
    header* head;
    page_t* leafpage;
    pagenum_t leafpagenum;

	fd = table_list[table_id - 1]->fd;
	id_table = table_id;
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
			//all_unpinning(table_id);
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
		//all_unpinning(table_id);
		return 0;
    }
    else{
		//all_unpinning(table_id);
		return -1;
    }
}

int db_delete(int table_id, uint64_t key){
    record* key_record;
    pagenum_t key_leafpage_num;

	fd = table_list[table_id - 1]->fd;
	id_table = table_id;
    if(fd >= 3){
        key_record = find(key);
        key_leafpage_num = find_leafpage(key);
        if(key_record != NULL){
            delete_entry(key_leafpage_num, key);
			//all_unpinning(table_id);
            return 0;
        }
        else{
		    //all_unpinning(table_id);
            //printf("Input key not exists.\n");
            return -1;
        }
    }
    else{
		//all_unpinning(table_id);
		return -1;
    }
}

