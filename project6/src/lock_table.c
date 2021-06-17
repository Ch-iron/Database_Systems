#include "lock_table.h"

pthread_mutex_t lock_table_latch;
index_t** hash_table;


int hash_function(int table_id, int64_t key){
	int sum;

	sum = table_id + key;
	return sum % HASH_NUM;
}

int
init_lock_table(){

    //해쉬테이블 초기화
    hash_table = (index_t**)calloc(HASH_NUM, sizeof(index_t*));
    for(int i = 0; i < HASH_NUM; i++){
        hash_table[i] = (index_t*)calloc(1, sizeof(index_t));
    }

	for(int i = 0; i < HASH_NUM; i++){
		hash_table[i]->index = i;
		hash_table[i]->hash_entry = NULL;
	}

    //mutex 초기화
    pthread_mutex_init(&lock_table_latch, NULL);
    return 0;
}

lock_t*
lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode)
{
	lock_t* new_obj;
	lock_t* final_obj;
	hash_t* record;
	int index;
	int i;
	lock_t* trx_lobj;
	int exclusive_count = 0;
	int same_count = 0;

	pthread_mutex_lock(&lock_table_latch);
	//printf("lock_table_latch acquire!!!!! %d\n", trx_id);
	//락오브젝트 생성, 초기화
	new_obj = (lock_t*)malloc(sizeof(lock_t));
	new_obj->prev_lobj = NULL;
	new_obj->next_lobj = NULL;
	new_obj->sentinel = NULL;
	pthread_cond_init(&new_obj->cond, NULL);

	//new attribute init
	new_obj->lock_mode = lock_mode;
	new_obj->trx_next_lobj = NULL;
	new_obj->trx_prev_lobj = NULL;
	new_obj->owner_trx_id = trx_id;
	new_obj->is_wait = 0;

	//해당하는 해쉬엔트리 찾기
	index = hash_function(table_id, key);
	record = hash_table[index]->hash_entry;
	while(record != NULL){
		if(record->table_id == table_id && record->record_id == key){
			break;
		}
		record = record->next;
	}
	if(record == NULL){
		record = (hash_t*)calloc(1, sizeof(hash_t));
		record->table_id = table_id;
		record->record_id = key;
		record->head = NULL;
		record->tail = NULL;
		record->next = hash_table[index]->hash_entry;
		hash_table[index]->hash_entry = record;
	}

	//connect lock_obj with trx_table
	pthread_mutex_lock(&trx_table_latch);
	//printf("trx_latch acquire!! %d\n", trx_id);
	for(i = 0; i < TRX_NUM; i++){
		if(trx_table[i]->trx_id == trx_id){
			if(trx_table[i]->next_lobj == NULL){
				trx_table[i]->next_lobj = new_obj;
				new_obj->owner_trx_id = trx_id;
			}
			else{
				trx_lobj = trx_table[i]->next_lobj;
				while(trx_lobj->trx_next_lobj != NULL){
					trx_lobj = trx_lobj->trx_next_lobj;
				}
				trx_lobj->trx_next_lobj = new_obj;
				new_obj->trx_prev_lobj = trx_lobj;
				new_obj->owner_trx_id = trx_id;
			}
		}
	}
	pthread_mutex_unlock(&trx_table_latch);
	//printf("trx_latch release %d\n", trx_id);

	//오브젝트와 레코드 연결
	new_obj->sentinel = record;

	//락을 획득
	if(record->head == NULL){
		record->head = new_obj;
		record->tail = new_obj;
		pthread_mutex_unlock(&lock_table_latch);
		//printf("record_lock acquire!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
		return new_obj;
	}
	//락이 있다면 대기(뒤에 매달기)
	else{
		//마지막 락오브젝트 탐색
		final_obj = record->head;
		while(1){
			if(final_obj->lock_mode == 1){
				exclusive_count++;
			}
			if(final_obj->owner_trx_id != new_obj->owner_trx_id){
				same_count++;
			}
			if(final_obj->next_lobj != NULL){
				final_obj = final_obj->next_lobj;
			}
			else{
				break;
			}
		}
		//printf("exclusive_count : %d same_count : %d\n", exclusive_count, same_count);
		//매달기
		//one more exclusive lock in front of new_obj
		if(exclusive_count > 0){
			if(same_count > 0){
				final_obj->next_lobj = new_obj;
				new_obj->prev_lobj = final_obj;
				record->tail = new_obj;
				//deadlock_detection
				if(deadlock_detect(new_obj) == 1){
					new_obj->is_wait = 1;
					//pthread_mutex_unlock(&lock_table_latch);
					return NULL;
				}
				new_obj->is_wait = 1;
				//printf("Wait!!!!!!!!!!!!!!!!!!!!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
				pthread_cond_wait(&new_obj->cond, &lock_table_latch);
				//new_obj->is_wait = 0;
				pthread_mutex_unlock(&lock_table_latch);
				return new_obj;
			}
			else{
				final_obj->next_lobj = new_obj;
				new_obj->prev_lobj = final_obj;
				record->tail = new_obj;
				pthread_mutex_unlock(&lock_table_latch);
				//printf("record_lock acquire!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
				return new_obj;
			}

		}
		//shared lock
		else if(new_obj->lock_mode == 0 && exclusive_count == 0){
			final_obj->next_lobj = new_obj;
			new_obj->prev_lobj = final_obj;
			record->tail = new_obj;
			pthread_mutex_unlock(&lock_table_latch);
			//printf("record_lock acquire!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
			return new_obj;
		}
		//exclusive lock
		else if(new_obj->lock_mode == 1 && exclusive_count == 0){
			if(same_count > 0){
				final_obj->next_lobj = new_obj;
				new_obj->prev_lobj = final_obj;
				record->tail = new_obj;
				//deadlock_detection
				if(deadlock_detect(new_obj) == 1){
					new_obj->is_wait = 1;
					//pthread_mutex_unlock(&lock_table_latch);
					return NULL;
				}
				new_obj->is_wait = 1;
				//printf("Wait!!!!!!!!!!!!!!!!!!!!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
				pthread_cond_wait(&new_obj->cond, &lock_table_latch);
				//new_obj->is_wait = 0;
				pthread_mutex_unlock(&lock_table_latch);
				return new_obj;
			}
			else{
				final_obj->next_lobj = new_obj;
				new_obj->prev_lobj = final_obj;
				record->tail = new_obj;
				pthread_mutex_unlock(&lock_table_latch);
				//printf("record_lock acquire!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
				return new_obj;
			}
		}

		pthread_mutex_unlock(&lock_table_latch);
		//printf("record_lock acquire!!! table : %d key : %ld trx : %d mode : %d\n", table_id, key, trx_id, lock_mode);
		return new_obj;
	}

	//아무일도 일어나지 않았을때
	//pthread_mutex_unlock(&lock_table_latch);
	//return NULL;
}

int deadlock_situation(lock_t* wait_lobj, trx_t* trx){
	hash_t* entry;
	hash_t* inspect_entry;
	trx_t* inspect_trx;
	lock_t* front_lobj;
	lock_t* inspect_lobj;
	int i;

	//printf("wait_lobj - table : %d key : %d trx : %d mode : %d\n", wait_lobj->sentinel->table_id, wait_lobj->sentinel->record_id, wait_lobj->owner_trx_id, wait_lobj->lock_mode);
	entry = wait_lobj->sentinel;
	front_lobj = entry->head;
	while(front_lobj != wait_lobj){
		//printf("situ front_lobj - table : %d key : %d trx : %d mode : %d\n", front_lobj->sentinel->table_id, front_lobj->sentinel->record_id, front_lobj->owner_trx_id, front_lobj->lock_mode);
		for(i = 0; i < TRX_NUM; i++){
			if(trx_table[i]->trx_id == front_lobj->owner_trx_id){
				break;
			}
		}
		if(trx_table[i] == trx){
			return 1;
		}
		if(front_lobj->owner_trx_id != wait_lobj->owner_trx_id){
			inspect_trx = trx_table[i];
			//printf("situation trx_id : %d %d\n", inspect_trx->trx_id, inspect_trx->next_lobj->owner_trx_id);
			inspect_lobj = inspect_trx->next_lobj;
			while(inspect_lobj != NULL){
				if(inspect_lobj != front_lobj){
					if(inspect_lobj->is_wait == 1){
					//printf("inspect_lobj - table : %d key : %d trx : %d mode : %d\n", inspect_lobj->sentinel->table_id, inspect_lobj->sentinel->record_id, inspect_lobj->owner_trx_id, inspect_lobj->lock_mode);
					//printf("inspect_lobj is wait!!!!\n");
						if(deadlock_situation(inspect_lobj, trx) == 1){
							return 1;
						}
					}
				}
				inspect_lobj = inspect_lobj->trx_next_lobj;
			}
		}
		front_lobj = front_lobj->next_lobj;
	}
	return 0;
}

int deadlock_detect(lock_t* possible_deadlock){
	hash_t* entry;
	trx_t* self_trx;
	trx_t* inspect_trx;
	lock_t* front_lobj;
	lock_t* inspect_lobj;
	int i;

	pthread_mutex_lock(&trx_table_latch);
	for(i = 0; i < TRX_NUM; i++){
		if(trx_table[i]->trx_id == possible_deadlock->owner_trx_id){
			self_trx = trx_table[i];
			break;
		}
	}
	entry = possible_deadlock->sentinel;
	front_lobj = entry->head;
	while(front_lobj != possible_deadlock){
		//printf("front_lobj - table : %d key : %d trx : %d mode : %d\n", front_lobj->sentinel->table_id, front_lobj->sentinel->record_id, front_lobj->owner_trx_id, front_lobj->lock_mode);
		if(front_lobj->owner_trx_id != possible_deadlock->owner_trx_id){
			for(i = 0; i < TRX_NUM; i++){
				if(trx_table[i]->trx_id == front_lobj->owner_trx_id){
					break;
				}
			}
			inspect_trx = trx_table[i];
			//printf("deadlock trx_id : %d  \n", inspect_trx->trx_id);
			inspect_lobj = inspect_trx->next_lobj;
			while(inspect_lobj != NULL){
				if(inspect_lobj != front_lobj){
					if(inspect_lobj->is_wait == 1){
					//printf("inspect_lobj - table : %d key : %d trx : %d mode : %d\n", inspect_lobj->sentinel->table_id, inspect_lobj->sentinel->record_id, inspect_lobj->owner_trx_id, inspect_lobj->lock_mode);
					//printf("inspect_lobj is wait!!!!\n");
						if(deadlock_situation(inspect_lobj, self_trx) == 1){
							pthread_mutex_unlock(&trx_table_latch);
							return 1;
						}
					}
				}
				inspect_lobj = inspect_lobj->trx_next_lobj;
			}
		}
		front_lobj = front_lobj->next_lobj;
	}
	pthread_mutex_unlock(&trx_table_latch);
	return 0;
}

int
lock_release(lock_t* lock_obj)
{
	hash_t* record;
	lock_t* shared;
	lock_t* final_obj;
	lock_t* tmp;
	int exclusive_count = 0;

	//printf("lock_table_latch trylock\n");
	pthread_mutex_lock(&lock_table_latch);
	//printf("lock_release start!!\n");
	//락 오브젝트 제거
	record = lock_obj->sentinel;

	//only one object
	if(lock_obj->next_lobj == NULL && lock_obj->prev_lobj == NULL){
		record->tail = NULL;
		record->head = NULL;
		free(lock_obj);
		lock_obj = NULL;
	}

	//more than one object
	else{
		//When first lock
		if(record->head == lock_obj){
			if(lock_obj->lock_mode == 1 && lock_obj->owner_trx_id != lock_obj->next_lobj->owner_trx_id){
				if(lock_obj->next_lobj->lock_mode == 1){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
					record->head->is_wait = 0;
					pthread_cond_signal(&record->head->cond);
					//printf("wakeup!!!!!! table: %d key: %d\n", record->table_id, record->record_id);
				}
				else if(lock_obj->next_lobj->lock_mode == 0){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
					shared = record->head;
					while(shared->lock_mode == 0){
						shared->is_wait = 0;
						pthread_cond_signal(&shared->cond);
						//printf("wakeup!!!!!! table: %d key: %d\n", record->table_id, record->record_id);
						shared = shared->next_lobj;
						if(shared == NULL){
							break;
						}
					}
				}
			}
			else if(lock_obj->lock_mode == 0 && lock_obj->owner_trx_id != lock_obj->next_lobj->owner_trx_id){
				if(lock_obj->next_lobj->lock_mode == 1){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
					record->head->is_wait = 0;
					pthread_cond_signal(&record->head->cond);
					//printf("wakeup!!!!!! table: %d key: %d\n", record->table_id, record->record_id);
				}
				else if(lock_obj->next_lobj->lock_mode == 0){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
				}
			}
		}
		//When middle lock
		else if(record->head != lock_obj && record->tail != lock_obj){
			if(lock_obj->lock_mode == 1 && lock_obj->owner_trx_id != lock_obj->next_lobj->owner_trx_id){
				final_obj =  record->head;
				while(final_obj != lock_obj){
					if(final_obj->lock_mode == 1){
						exclusive_count++;
					}
					final_obj = final_obj->next_lobj;
				}
				if(exclusive_count == 0){
					tmp = lock_obj->next_lobj;
					lock_obj->prev_lobj->next_lobj = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = lock_obj->prev_lobj;
					free(lock_obj);
					lock_obj = NULL;
					tmp->is_wait = 0;
					pthread_cond_signal(&tmp->cond);
				}
				else{
					lock_obj->prev_lobj->next_lobj = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = lock_obj->prev_lobj;
					free(lock_obj);
					lock_obj = NULL;
				}
			}
			else{
				lock_obj->prev_lobj->next_lobj = lock_obj->next_lobj;
				lock_obj->next_lobj->prev_lobj = lock_obj->prev_lobj;
				free(lock_obj);
				lock_obj = NULL;
			}
		}
		//When final lock
		else if(record->tail == lock_obj){
			record->tail = lock_obj->prev_lobj;
			lock_obj->prev_lobj->next_lobj = NULL;
			free(lock_obj);
			lock_obj = NULL;
		}
	}

	if(lock_obj == NULL){
		pthread_mutex_unlock(&lock_table_latch);
		return 0;
	}
	else{
		pthread_mutex_unlock(&lock_table_latch);
		return -1;
	}
}

int
abort_lock_release(lock_t* lock_obj)
{
	hash_t* record;
	lock_t* shared;
	lock_t* final_obj;
	lock_t* tmp;
	int exclusive_count = 0;

	//printf("lock_release start!!\n");
	//락 오브젝트 제거
	record = lock_obj->sentinel;

	//only one object
	if(lock_obj->next_lobj == NULL && lock_obj->prev_lobj == NULL){
		record->tail = NULL;
		record->head = NULL;
		free(lock_obj);
		lock_obj = NULL;
	}

	//more than one object
	else{
		//When first lock
		if(record->head == lock_obj){
			if(lock_obj->lock_mode == 1 && lock_obj->owner_trx_id != lock_obj->next_lobj->owner_trx_id){
				if(lock_obj->next_lobj->lock_mode == 1){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
					record->head->is_wait = 0;
					pthread_cond_signal(&record->head->cond);
					//printf("wakeup!!!!!! table: %d key: %d\n", record->table_id, record->record_id);
				}
				else if(lock_obj->next_lobj->lock_mode == 0){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
					shared = record->head;
					while(shared->lock_mode == 0){
						shared->is_wait = 0;
						pthread_cond_signal(&shared->cond);
						//printf("wakeup!!!!!! table: %d key: %d\n", record->table_id, record->record_id);
						shared = shared->next_lobj;
						if(shared == NULL){
							break;
						}
					}
				}
			}
			else if(lock_obj->lock_mode == 0 && lock_obj->owner_trx_id != lock_obj->next_lobj->owner_trx_id){
				if(lock_obj->next_lobj->lock_mode == 1){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
					record->head->is_wait = 0;
					pthread_cond_signal(&record->head->cond);
					//printf("wakeup!!!!!! table: %d key: %d\n", record->table_id, record->record_id);
				}
				else if(lock_obj->next_lobj->lock_mode == 0){
					record->head = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = NULL;
					free(lock_obj);
					lock_obj = NULL;
				}
			}
		}
		//When middle lock
		else if(record->head != lock_obj && record->tail != lock_obj){
			if(lock_obj->lock_mode == 1 && lock_obj->owner_trx_id != lock_obj->next_lobj->owner_trx_id){
				final_obj =  record->head;
				while(final_obj != lock_obj){
					if(final_obj->lock_mode == 1){
						exclusive_count++;
					}
					final_obj = final_obj->next_lobj;
				}
				if(exclusive_count == 0){
					tmp = lock_obj->next_lobj;
					lock_obj->prev_lobj->next_lobj = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = lock_obj->prev_lobj;
					free(lock_obj);
					lock_obj = NULL;
					tmp->is_wait = 0;
					pthread_cond_signal(&tmp->cond);
				}
				else{
					lock_obj->prev_lobj->next_lobj = lock_obj->next_lobj;
					lock_obj->next_lobj->prev_lobj = lock_obj->prev_lobj;
					free(lock_obj);
					lock_obj = NULL;
				}
			}
			else{
				lock_obj->prev_lobj->next_lobj = lock_obj->next_lobj;
				lock_obj->next_lobj->prev_lobj = lock_obj->prev_lobj;
				free(lock_obj);
				lock_obj = NULL;
			}
		}
		//When final lock
		else if(record->tail == lock_obj){
			record->tail = lock_obj->prev_lobj;
			lock_obj->prev_lobj->next_lobj = NULL;
			free(lock_obj);
			lock_obj = NULL;
		}
	}

	if(lock_obj == NULL){
		//pthread_mutex_unlock(&lock_table_latch);
		return 0;
	}
	else{
		//pthread_mutex_unlock(&lock_table_latch);
		return -1;
	}
}
