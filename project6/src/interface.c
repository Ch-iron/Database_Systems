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
uint64_t global_LSN;
pthread_mutex_t trx_table_latch;
pthread_mutex_t log_buffer_latch;
trx_t** trx_table;
Comlog** log_buffer;
int commit_table[COMMIT_NUM];
int loser_trx_table[COMMIT_NUM];
compensate_assist** compensate_table;
pthread_mutex_t global_latch = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t commit_table_latch = PTHREAD_MUTEX_INITIALIZER;
FILE* fp;
int begin = 0, up = 0, com = 0;
Comlog* recovery_log;
int final_trx_id;
const int bcrlogsize = 28;
const int ulogsize = 288;
const int comlogsize = 296;

int init_db(int bufnum, int flag, int log_num, char* log_path, char* logmsg_path){
	int i, j, k;
	int analysis;
	int key;
	char* commit_path = "commit_table";
	pagenum_t pagenum;
	leaf_page* leafpage;
	char tmp[120];
	int recovery_count;
	char path[20] = "DATA";
	char filenum[10];

	if(bufnum == 0){
		return -1;
	}

	num_buf = bufnum;
	table_list = (table**)calloc(10, sizeof(table*));
	for(i = 0; i < 10; i++){
		table_list[i] = (table*)calloc(1, sizeof(table));
		table_list[i]->pathname = (char*)calloc(20, sizeof(char));
	}

	//init trx_table
	global_trx_id = 0;
	trx_table = (trx_t**)calloc(TRX_NUM, sizeof(trx_t*));
	for(i = 0; i < TRX_NUM; i++){
		trx_table[i] = (trx_t*)calloc(1, sizeof(trx_t));
		trx_table[i]->trx_id = 0;
		trx_table[i]->next_lobj = NULL;
		//trx_table[i]->xlock_count = 0;
		//trx_table[i]->undo_log = (char**)calloc(UPDATE_NUM, sizeof(char*));
		pthread_mutex_init(&trx_table[i]->trx_latch, NULL);
		trx_table[i]->last_LSN = 0;
		//for(j = 0; j < UPDATE_NUM; j++){
		//	trx_table[i]->undo_log[j] = (char*)calloc(120, sizeof(char));
		//}
	}

	//init lock_table
	init_lock_table();

	//init log_buffer
	//global_LSN = 0;
	log_buffer = (Comlog**)calloc(LOG_NUM, sizeof(Comlog*));
	for(i = 0; i < LOG_NUM; i++){
		log_buffer[i] = (Comlog*)calloc(1, sizeof(Comlog));
		log_buffer[i]->LSN = 1;
	}
	pthread_mutex_init(&log_buffer_latch, NULL);

	//open log, logmsg_file
	if(access(log_path, F_OK) != 0) {
		log_create(log_path);
	}
	else
		log_open(log_path);

	fp = fopen(logmsg_path, "w");

	//init commit_table
	if(access(commit_path, F_OK) != 0) {
		commit_create(commit_path);
	}
	else
		commit_open(commit_path);
	
	for(i = 0; i < COMMIT_NUM; i++){
		commit_table[i] = 0;
	}
	for(i = 0; i < COMMIT_NUM; i++){
		loser_trx_table[i] = 0;
	}
	compensate_table = (compensate_assist**)calloc(COMMIT_NUM, sizeof(compensate_assist*));
	for(i = 0; i < COMMIT_NUM; i++){
		compensate_table[i] = (compensate_assist*)calloc(1, sizeof(compensate_assist));
	}

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

	//If logfile is empty, no excute recovery
	global_LSN = logfile_offset();
	//global_LSN++;
	if(global_LSN == 0){
		return 0;
	}

	//Recovery Start
	recovery_log = (Comlog*)calloc(1, sizeof(Comlog));
	//Analysis Phase
	analysis = 0;
	commit_read(commit_table, &final_trx_id);
	//printf("%d\n", commit_table[1]);
	fputs("[ANALYSIS] Analysis pass start\n", fp);
	//printf("[ANALYSIS] Analysis pass start\n");
	//fclose(fp);
	for(analysis = 1; analysis <= final_trx_id; analysis++){
		for(i = 0; i < COMMIT_NUM; i++){
			if(analysis == commit_table[i]){
				break;
			}
		}
		if(i == COMMIT_NUM){
			for(j = 0; j < COMMIT_NUM; j++){
				if(loser_trx_table[j] == 0){
					loser_trx_table[j] = analysis;
					break;
				}
			}
		}
	}
	fputs("[ANALYSIS] Analysis success. Winner:", fp);
	for(i = 0; i < COMMIT_NUM; i++){
		if(commit_table[i] == 0){
			break;
		}
		fprintf(fp, " %d", commit_table[i]);
	}
	fputs(", Loser :", fp);
	for(i = 0; i < COMMIT_NUM; i++){
		if(loser_trx_table[i] == 0){
			break;
		}
		fprintf(fp, " %d", loser_trx_table[i]);
	}
	fputs("\n", fp);
	fclose(fp);
	//printf("[ANALYSIS] Analysis success. Winner:\n");

	//Redo Phase
	fp = fopen(logmsg_path, "a");
	fputs("[REDO] Redo pass start\n", fp);
	//printf("[REDO] Redo pass start\n");
	recovery_count = 0;
	recovery_log = log_read(begin, up, com);
	while(recovery_log != NULL){
		if(recovery_log->type == 0){
			fprintf(fp, "LSN %lu [BEGIN] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			//printf("LSN %lu [BEGIN] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			begin++;
			for(i = 0; i < COMMIT_NUM; i++){
				if(loser_trx_table[i] == 0) break;
				if(loser_trx_table[i] == recovery_log->trx_id){
					for(j = 0; j < LOG_NUM; j++){
						if(compensate_table[j]->LSN == 0){
							compensate_table[j]->LSN = recovery_log->LSN;
							compensate_table[j]->trx_id = recovery_log->trx_id;
							break;
						}
					}
					break;
				}
			}
		}
		else if(recovery_log->type == 1){
			sprintf(filenum, "%d", recovery_log->table_id);
			path[4] = filenum[0];
			sprintf(pathname, "%s", path);
			open_table(pathname);
			fd = table_list[recovery_log->table_id - 1]->fd;
			id_table = table_list[recovery_log->table_id - 1]->table_id;
			leafpage = (leaf_page*) read_buffer_page(recovery_log->page_num);
			if(recovery_log->LSN <= leafpage->page_LSN){
				fprintf(fp, "LSN %lu [CONSIDER REDO] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
				//printf("LSN %lu [CONSIDER REDO] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			}
			else{
				key = (recovery_log->offset - PAGE_HEADER) / 120;
				sprintf(leafpage->data[key].value, "%s", recovery_log->new_image);
				fprintf(fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", recovery_log->LSN, recovery_log->trx_id);
				//printf("LSN %lu [UPDATE] Transaction id %d redo apply\n", recovery_log->LSN, recovery_log->trx_id);
				leafpage->page_LSN = recovery_log->LSN;
				for(i = 0; i < COMMIT_NUM; i++){
					if(loser_trx_table[i] == 0) break;
					if(loser_trx_table[i] == recovery_log->trx_id){
						for(j = 0; j < LOG_NUM; j++){
							if(compensate_table[j]->LSN == 0){
								compensate_table[j]->LSN = recovery_log->LSN;
								compensate_table[j]->trx_id = recovery_log->trx_id;
								break;
							}
						}
						break;
					}
				}
			}
			up++;
			close(fd);
		}
		else if(recovery_log->type == 2){
			fprintf(fp, "LSN %lu [COMMIT] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			//printf("LSN %lu [COMMIT] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			begin++;
		}
		else if(recovery_log->type == 3){
			fprintf(fp, "LSN %lu [ROLLBACK] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			//printf("LSN %lu [ROLLBACK] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			begin++;
		}
		else if(recovery_log->type == 4){
			sprintf(filenum, "%d", recovery_log->table_id);
			path[4] = filenum[0];
			sprintf(pathname, "%s", path);
			open_table(pathname);
			fd = table_list[recovery_log->table_id - 1]->fd;
			id_table = table_list[recovery_log->table_id - 1]->table_id;
			leafpage = (leaf_page*) read_buffer_page(recovery_log->page_num);
			if(recovery_log->LSN <= leafpage->page_LSN){
				fprintf(fp, "LSN %lu [CONSIDER REDO] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
				//printf("LSN %lu [CONSIDER REDO] Transaction id %d\n", recovery_log->LSN, recovery_log->trx_id);
			}
			else{
				key = (recovery_log->offset - PAGE_HEADER) / 120;
				sprintf(leafpage->data[key].value, "%s", recovery_log->old_image);
				fprintf(fp, "LSN %lu [CLR] next undo lsn %lu\n", recovery_log->LSN, recovery_log->next_undo_LSN);
				//printf("LSN %lu [CLR] next undo lsn %lu\n", recovery_log->LSN, recovery_log->next_undo_LSN);
				leafpage->page_LSN = recovery_log->LSN;
			}
			com++;
			close(fd);
		}
		recovery_count++;
		if(flag == 1 && recovery_count == log_num){
			shutdown_db();
			fclose(fp);
			return 0;
		}
		recovery_log = log_read(begin, up, com);
	}
	fputs("[REDO] Redo pass end\n", fp);
	//printf("[REDO] Redo pass end\n");
	fclose(fp);

	//Compensate log create
	for(i = 0; i < LOG_NUM; i++){
		if(compensate_table[i]->LSN == 0){
			break;
		}
	}
	k = i;
	for(j = 0; j < i; j++){
		log_buffer[j] = log_read_offset(compensate_table[k - 1]->LSN);
		if(log_buffer[j]->type == 0){
			log_buffer[j]->LSN = global_LSN;
			log_buffer[j]->type = 3;
			global_LSN = global_LSN + bcrlogsize;
		}
		else if(log_buffer[j]->type == 1){
			log_buffer[j]->LSN = global_LSN;
			log_buffer[j]->type = 4;
			log_buffer[j]->next_undo_LSN = log_buffer[j]->prev_LSN;
			sprintf(tmp, "%s", log_buffer[j]->old_image);
			sprintf(log_buffer[j]->old_image, "%s",log_buffer[j]->new_image);
			sprintf(log_buffer[j]->new_image, "%s", tmp);
			global_LSN = global_LSN + comlogsize;
		}
		k--;
	}
	//Undo Phase
	fp = fopen(logmsg_path, "a");
	fputs("[UNDO] Undo pass start\n", fp);
	//printf("[UNDO] Undo pass start\n");
	recovery_count = 0;
	for(i = 0; i < LOG_NUM; i++){
		if(log_buffer[i]->LSN == 1){
			break;
		}
		if(log_buffer[i]->type == 3){
			fprintf(fp, "LSN %lu [ROLLBACK] Transaction id %d\n", log_buffer[i]->LSN, log_buffer[i]->trx_id);
			//printf("LSN %lu [ROLLBACK] Transaction id %d\n", log_buffer[i]->LSN, log_buffer[i]->trx_id);
		}
		else if(log_buffer[i]->type == 4){
			sprintf(filenum, "%d", log_buffer[i]->table_id);
			path[4] = filenum[0];
			sprintf(pathname, "%s", path);
			open_table(pathname);
			fd = table_list[log_buffer[i]->table_id - 1]->fd;
			id_table = table_list[log_buffer[i]->table_id - 1]->table_id;
			leafpage = (leaf_page*) read_buffer_page(log_buffer[i]->page_num);
			key = (log_buffer[i]->offset - PAGE_HEADER) / 120;
			sprintf(leafpage->data[key].value, "%s", log_buffer[i]->old_image);
			fprintf(fp, "LSN %lu [CLR] next undo lsn %lu\n", log_buffer[i]->LSN, log_buffer[i]->next_undo_LSN);
			//printf("LSN %lu [CLR] next undo lsn %lu\n", log_buffer[i]->LSN, log_buffer[i]->next_undo_LSN);
			leafpage->page_LSN = log_buffer[i]->LSN;
			close(fd);
		}
		recovery_count++;
		if(flag == 2 && recovery_count == log_num){
			log_write();
			shutdown_db();
			fclose(fp);
			return 0;
		}
	}
	fputs("[UNDO] Undo pass end\n", fp);
	//printf("[UNDO] Undo pass end\n");
	fclose(fp);

	//commit_table init
	for(i = 0; i < COMMIT_NUM; i++){
		if(commit_table[i] == 0) break;
		else{
			commit_table[i] = 0;
		}
	}
	for(i = 0; i < COMMIT_NUM; i++){
		if(loser_trx_table[i] == 0) break;
		else{
			loser_trx_table[i] = 0;
		}
	}
	lseek(commit_fd, 0, SEEK_SET);

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
	int table_id;

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
		if(pathname[5] == 48){
			table_id = 10;
		}
		else{
			table_id = pathname[4] - 48;
		}
		table_num++;
		table_list[table_id - 1]->table_id = table_id;
		sprintf(table_list[table_id - 1]->pathname, "%s", pathname);
		id_table = table_list[table_id - 1]->table_id;
		fd = start_table(pathname);
		table_list[table_id - 1]->fd = fd;
		return table_list[table_id - 1]->table_id;
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
	int i, j;

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
	//write log buffer
	pthread_mutex_lock(&log_buffer_latch);
	for(j = 0; j < LOG_NUM; j++){
		if(log_buffer[j]->LSN == 1){
			log_buffer[j]->Logsize = bcrlogsize;
			log_buffer[j]->LSN = global_LSN;
			log_buffer[j]->prev_LSN = trx_table[i]->last_LSN;
			log_buffer[j]->trx_id = trx_table[i]->trx_id;
			log_buffer[j]->type = 0;
			trx_table[i]->last_LSN = log_buffer[j]->LSN;
			global_LSN = global_LSN + bcrlogsize;
			break;
		}
	}
	//printf("BCRlog LSN : %lu, type : %d\n", log_buffer[j]->LSN, log_buffer[j]->type);
	pthread_mutex_unlock(&log_buffer_latch);
	//printf("trx_begin!!!!! %d\n", trx_table[i]->trx_id);
	pthread_mutex_unlock(&trx_table_latch);
	return trx_table[i]->trx_id;
}

int trx_commit(int trx_id){
	int i, j, k;
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
			
			//wtire log_bufffer
			pthread_mutex_lock(&log_buffer_latch);
			for(k = 0; k < LOG_NUM; k++){
				if(log_buffer[k]->LSN == 1){
					log_buffer[k]->Logsize = bcrlogsize;
					log_buffer[k]->LSN = global_LSN;
					log_buffer[k]->prev_LSN = trx_table[i]->last_LSN;
					log_buffer[k]->trx_id = trx_table[i]->trx_id;
					log_buffer[k]->type = 2;
					trx_table[i]->last_LSN = log_buffer[k]->LSN;
					global_LSN = global_LSN + bcrlogsize;
					break;
				}
			}
			//printf("BCRlog LSN : %lu, type : %d\n", log_buffer[k]->LSN, log_buffer[k]->type);
			log_write();
			pthread_mutex_unlock(&log_buffer_latch);
			trx_table[i]->trx_id = 0;
			trx_table[i]->next_lobj = NULL;
			/*for(j = 0; j < trx_table[i]->xlock_count; j++){
				free(trx_table[i]->undo_log[j]);
				trx_table[i]->undo_log[j] = NULL;
			}*/
			//trx_table[i]->xlock_count = 0;
			trx_table[i]->last_LSN = 0;
			//for(j = 0; j < UPDATE_NUM; j++){
			//	trx_table[i]->undo_log[j] = 0;
			//}
			break;
		}
	}
	pthread_mutex_unlock(&trx_table_latch);

	pthread_mutex_lock(&commit_table_latch);
	/*for(k = 0; i < COMMIT_NUM; k++){
		if(commit_table[k] == 0){
			commit_table[k] = trx_id;
			break;
		}
	}*/
	commit_write(trx_id, global_trx_id);
	pthread_mutex_unlock(&commit_table_latch);
	//printf("commit finish!!! %d\n", trx_id);
	if(trx_table[i]->next_lobj != NULL){
		return 0;
	}
	return trx_id;
}

int trx_abort(int trx_id){
	int i, j, k, l, m, n, o;
	lock_t* lock_obj;
	lock_t* next_lobj;
	hash_t* entry;
	pagenum_t pagenum;
	leaf_page* leafpage;
	Comlog* log;

	pthread_mutex_lock(&trx_table_latch);
	pthread_mutex_trylock(&lock_table_latch);
	//printf("trx_abort start!!!! %d\n", trx_id);
	k = 0;
	for(i = 0; i < TRX_NUM; i++){
		if(trx_table[i]->trx_id == trx_id){
			lock_obj = trx_table[i]->next_lobj;
			//printf("lock_obj's mode : %d\n", lock_obj->lock_mode);

			//find final operation
			while(lock_obj->trx_next_lobj != NULL){
				lock_obj = lock_obj->trx_next_lobj;
			}
			while(lock_obj != NULL){
				if(lock_obj->lock_mode == 1){
					entry = lock_obj->sentinel;
					pthread_mutex_lock(&global_latch);
					fd = table_list[entry->table_id - 1]->fd;
					id_table = entry->table_id;
					pagenum = find_leafpage(entry->record_id);
					leafpage = (leaf_page*)read_buffer_page(pagenum);
					pthread_mutex_unlock(&global_latch);
					for(l = 0; l < leafpage->num_keys; l++){
						if(leafpage->data[l].key == entry->record_id){
						break;
						}
					}
					for(j = 0; j < sizeof(leafpage->data[i].value); j++){
						leafpage->data[l].value[j] = 0;
					}
				}

				//undo
				if(k == 0){
					for(m = 0; m < LOG_NUM; m++){
						if(log_buffer[m]->LSN == trx_table[i]->last_LSN){
							log = log_buffer[m];
							sprintf(leafpage->data[i].value, "%s", log->old_image);
							for(n = 0; n < LOG_NUM; n++){
								if(log->prev_LSN == log_buffer[n]->LSN){
									log = log_buffer[n];
								}
							}
							k++;

							//write compensate log
							pthread_mutex_lock(&log_buffer_latch);
							for(o = 0; o < LOG_NUM; o++){
								if(log_buffer[o]->LSN == 1){
									log_buffer[0]->Logsize = comlogsize;
									log_buffer[o]->LSN = global_LSN;
									log_buffer[o]->prev_LSN = trx_table[i]->last_LSN;
									log_buffer[o]->trx_id = trx_table[i]->trx_id;
									log_buffer[o]->type = 4;
									log_buffer[o]->table_id = entry->table_id;
									log_buffer[o]->offset = PAGE_HEADER + (l * 120);
									log_buffer[o]->page_num = pagenum;
									log_buffer[o]->data_length = 120;
									if(log == NULL){
										log_buffer[o]->next_undo_LSN = 0;
									}
									else{
										log_buffer[o]->next_undo_LSN = log->LSN;
									}
									sprintf(log_buffer[o]->old_image, "%s", log->new_image);
									sprintf(log_buffer[o]->new_image, "%s", leafpage->data[i].value);
									trx_table[i]->last_LSN = log_buffer[o]->LSN;
									global_LSN = global_LSN + comlogsize;
									break;
								}
							}
							leafpage->page_LSN = log_buffer[o]->LSN;
							//printf("Comlog LSN : %lu, type : %d, table_id : %d, page : %ld\n", log_buffer[o]->LSN, log_buffer[o]->type, log_buffer[o]->table_id, log_buffer[o]->page_num);
							pthread_mutex_unlock(&log_buffer_latch);
							break;
						}
					}
				}
				else{
					sprintf(leafpage->data[i].value, "%s", log->old_image);
					for(n = 0; n < LOG_NUM; n++){
						if(log->prev_LSN == log_buffer[n]->LSN){
							log = log_buffer[n];
							k++;
							break;
						}
					}
					//write compensate log
					pthread_mutex_lock(&log_buffer_latch);
					for(o = 0; o < LOG_NUM; o++){
						if(log_buffer[o]->LSN == 1){
							log_buffer[o]->Logsize = comlogsize;
							log_buffer[o]->LSN = global_LSN;
							log_buffer[o]->prev_LSN = trx_table[i]->last_LSN;
							log_buffer[o]->trx_id = trx_table[i]->trx_id;
							log_buffer[o]->type = 4;
							log_buffer[o]->table_id = entry->table_id;
							log_buffer[o]->offset = PAGE_HEADER + (l * 120);
							log_buffer[o]->page_num = pagenum;
							log_buffer[o]->data_length = 120;
							if(log == NULL){
								log_buffer[o]->next_undo_LSN = 0;
							}
							else{
								log_buffer[o]->next_undo_LSN = log->LSN;
							}
							sprintf(log_buffer[o]->old_image, "%s", log->new_image);
							sprintf(log_buffer[o]->new_image, "%s", leafpage->data[i].value);
							trx_table[i]->last_LSN = log_buffer[o]->LSN;
							global_LSN = global_LSN + comlogsize;
							break;
						}
					}
					leafpage->page_LSN = log_buffer[o]->LSN;
					//printf("Comlog LSN : %lu, type : %d, table_id : %d, page : %ld\n", log_buffer[o]->LSN, log_buffer[o]->type, log_buffer[o]->table_id, log_buffer[o]->page_num);
					pthread_mutex_unlock(&log_buffer_latch);
				}
				pthread_mutex_unlock(&trx_table_latch);
				abort_lock_release(lock_obj);
				pthread_mutex_lock(&trx_table_latch);
				//printf("lock release OK!!!!!!!!!!!!!!!!!!!\n");
				lock_obj = lock_obj->trx_prev_lobj;
			}

			//This is winner trx - write commit_table, write rollback_log
			pthread_mutex_lock(&commit_table_latch);
			commit_write(trx_id, global_trx_id);
			pthread_mutex_unlock(&commit_table_latch);

			pthread_mutex_lock(&log_buffer_latch);
			for(j = 0; j < LOG_NUM; j++){
				if(log_buffer[j]->LSN == 1){
					log_buffer[j]->LSN = global_LSN;
					log_buffer[j]->prev_LSN = trx_table[i]->last_LSN;
					log_buffer[j]->trx_id = trx_table[i]->trx_id;
					log_buffer[j]->type = 3;
					trx_table[i]->last_LSN = log_buffer[j]->LSN;
					global_LSN = global_LSN + bcrlogsize;
					break;
				}
			}
			//printf("BCRlog LSN : %lu, type : %d\n", log_buffer[j]->LSN, log_buffer[j]->type);
			pthread_mutex_unlock(&log_buffer_latch);


			trx_table[i]->trx_id = 0;
			trx_table[i]->next_lobj = NULL;
			/*for(j = 0; j < trx_table[i]->xlock_count; j++){
				free(trx_table[i]->undo_log[j]);
				trx_table[i]->undo_log[j] = NULL;
			}*/
			//trx_table[i]->xlock_count = 0;
			//for(j = 0; j < UPDATE_NUM; j++){
			//	trx_table[i]->undo_log[j] = 0;
			//}
			break;
		}
	}
	//printf("trx_latch release!! %d\n", trx_id);
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
			return -1;
		}
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
	int i, j, k;

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
			return -1;
		}
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
				//trx_table[j]->xlock_count++;
				//undo_seq = trx_table[j]->xlock_count;
				//trx_table[j]->undo_log[undo_seq - 1] = (char*)calloc(120, sizeof(char));
				break;
			}
		}
		//pthread_mutex_unlock(&trx_table_latch);
		//printf("where undo_log : %d %d %d\n", undo_seq, trx_id, j);
		//sprintf(trx_table[j]->undo_log[undo_seq - 1], "%s", leafpage->data[i].value);
		pthread_mutex_lock(&log_buffer_latch);
		for(k = 0; k < LOG_NUM; k++){
			if(log_buffer[k]->LSN == 1){
				log_buffer[k]->Logsize = ulogsize;
				log_buffer[k]->LSN = global_LSN;
				log_buffer[k]->prev_LSN = trx_table[j]->last_LSN;
				log_buffer[k]->trx_id = trx_table[j]->trx_id;
				log_buffer[k]->type = 1;
				log_buffer[k]->table_id = table_id;
				log_buffer[k]->offset = PAGE_HEADER + (i * 120);
				log_buffer[k]->page_num = pagenum;
				log_buffer[k]->data_length = 120;
				sprintf(log_buffer[k]->old_image, "%s", leafpage->data[i].value);
				sprintf(log_buffer[k]->new_image, "%s", values);
				trx_table[j]->last_LSN = log_buffer[k]->LSN;
				global_LSN = global_LSN + ulogsize;
				break;
			}
		}
		sprintf(leafpage->data[i].value, "%s", values);
		leafpage->page_LSN = log_buffer[k]->LSN;
		//printf("Ulog LSN : %lu, type : %d, table_id : %d, page : %ld\n", log_buffer[k]->LSN, log_buffer[k]->type, log_buffer[k]->table_id, log_buffer[k]->page_num);
		pthread_mutex_unlock(&log_buffer_latch);
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

