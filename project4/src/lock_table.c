#include <lock_table.h>

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
lock_acquire(int table_id, int64_t key)
{
	lock_t* new_obj;
	lock_t* final_obj;
	hash_t* record;
	int index;

	pthread_mutex_lock(&lock_table_latch);
	//락오브젝트 생성, 초기화
	new_obj = (lock_t*)malloc(sizeof(lock_t));
	new_obj->prev_lobj = NULL;
	new_obj->next_lobj = NULL;
	new_obj->sentinel = NULL;
	pthread_cond_init(&new_obj->cond, NULL);

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

	//오브젝트와 레코드 연결
	new_obj->sentinel = record;

	//락을 획득
	if(record->head == NULL){
		record->head = new_obj;
		record->tail = new_obj;
		pthread_mutex_unlock(&lock_table_latch);
		return new_obj;
	}
	//락이 있다면 대기(뒤에 매달기)
	else{
		//마지막 락오브젝트 탐색
		final_obj = record->head;
		while(final_obj->next_lobj != NULL){
			final_obj = final_obj->next_lobj;
		}
		//매달기
		final_obj->next_lobj = new_obj;
		new_obj->prev_lobj = final_obj;
		record->tail = new_obj;
		pthread_cond_wait(&new_obj->cond, &lock_table_latch);

		pthread_mutex_unlock(&lock_table_latch);
		return new_obj;
	}

	//아무일도 일어나지 않았을때
	pthread_mutex_unlock(&lock_table_latch);
	return NULL;
}

int
lock_release(lock_t* lock_obj)
{
	hash_t* record;

	pthread_mutex_lock(&lock_table_latch);
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
		record->head = lock_obj->next_lobj;
		lock_obj->next_lobj->prev_lobj = NULL;
		free(lock_obj);
		lock_obj = NULL;
		//뒤에 달린 오브젝트 깨워주기
		pthread_cond_signal(&record->head->cond);
	}

	if(record->head == NULL){
		pthread_mutex_unlock(&lock_table_latch);
		return 0;
	}
	else{
		pthread_mutex_unlock(&lock_table_latch);
		return -1;
	}
}
