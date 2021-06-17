#include "buffer.h"

pthread_mutex_t buffer_latch;

//create new something
void create_new_file(){
    header* head;
    pagenum_t freepage_num;

    make_headerpage(0, 0, 1);
    make_new_freepage();
}

void make_new_freepage(){
    page_t* page;
    header* head;
    pagenum_t num_page;
    pagenum_t last_freepage_num;

	head = read_buffer_headerpage();
	file_free_page(head->numpage);
	head = read_buffer_headerpage();
	head->numpage++;
	write_buffer_headerpage();
}

int start_table(char* pathname){
	if(access(pathname, F_OK) != 0 ){
		file_create(pathname);
		create_new_file();
	}
	else
		file_open(pathname);
	return fd;
}

//Buffer function
pagenum_t file_alloc_page(){
	pagenum_t allocpage_num;
	page_t* allocpage;
    header* head;

    head = read_buffer_headerpage();
	allocpage_num = head->free;
	allocpage = read_buffer_page(allocpage_num);
    head->free = allocpage->NoP;
    allocpage->NoP = 0;
    allocpage->isleaf = 0;
	allocpage->num_keys = 0;
    allocpage->otherp = 0;
	write_buffer_page(allocpage_num);
	write_buffer_headerpage();
    return allocpage_num;
}

void file_free_page(pagenum_t pagenum){
	header* head;
	page_t* page;
    pagenum_t last_freepage_num, next_freepage_num;

	head = read_buffer_headerpage();
	page = read_buffer_page(pagenum);
	memset(page, 0, PAGE_SIZE);
	page->NoP = head->free;
	head->free = pagenum;
	write_buffer_page(pagenum);
	write_buffer_headerpage();
}

/*void pinning(buffer_st* pin_frame){
	pin_frame->is_pinned = 1;
}

void unpinning(buffer_st* unpin_frame){
	unpin_frame->is_pinned = 0;
}*/

/*void all_unpinning(int table_id){
	int i;
	int n;

	for(i = 0; i < num_buf; i++){
		if(buffer[i]->table_id == table_id && (n = pthread_mutex_trylock(&buffer[i]->page_latch)) != 0){
			//unpinning(buffer[i]);
			pthread_mutex_unlock(&buffer[i]->page_latch);
		}
	}
}*/

void modified_frame(buffer_st* modify_frame){
	modify_frame->is_dirty = 1;
}

void LRU_update(buffer_st* using_frame){
	if(using_frame == LRU_head){
		using_frame->prev_LRU = LRU_tail;
		LRU_tail->next_LRU = using_frame;
		LRU_tail = using_frame;
		LRU_head = using_frame->next_LRU;
		LRU_head->prev_LRU = NULL;
		LRU_tail->next_LRU = NULL;
	}
	else if(using_frame == LRU_tail){
	}
	else{
		using_frame->next_LRU->prev_LRU = using_frame->prev_LRU;
		using_frame->prev_LRU->next_LRU = using_frame->next_LRU;
		using_frame->prev_LRU = LRU_tail;
		LRU_tail->next_LRU = using_frame;
		LRU_tail = using_frame;
		using_frame->next_LRU = NULL;
	}
}

buffer_st* LRU_choice(){
	int i;
	int n;
	buffer_st* frame;

	frame = LRU_head;
	for(i = 0; i < num_buf; i++){
		if(frame->table_id == 0){
			if((n = pthread_mutex_trylock(&frame->page_latch)) == 0){
				pthread_mutex_unlock(&frame->page_latch);
				LRU_update(frame);
				return frame;
			}
		}
		/*if(frame->is_pinned == 0 && frame->table_id == 0){
			LRU_update(frame);
			return frame;
		}*/
		else if(frame->table_id != 0 && frame->is_dirty == 1){
			if((n = pthread_mutex_trylock(&frame->page_latch)) == 0){
				pthread_mutex_unlock(&frame->page_latch);
				drop_frame(frame->page_num, frame);
				return frame;
			}
		}
		/*else if(frame->is_pinned == 0 && frame->table_id != 0 && frame->is_dirty == 1){
			drop_frame(frame->page_num, frame);
			return frame;
		}*/
		else if(frame->table_id != 0 && frame->is_dirty == 0){
			if((n = pthread_mutex_trylock(&frame->page_latch)) == 0){
				pthread_mutex_unlock(&frame->page_latch);
				return frame;
			}
		}
		/*else if(frame->is_pinned == 0 && frame->table_id != 0 && frame->is_dirty == 0){
			return frame;
		}*/
		else{
			frame = frame->next_LRU;
		}
	}
}

header* read_buffer_headerpage(){
	int i;
	header* head;
	buffer_header* choice;
	buffer_st* tmp;

	pthread_mutex_lock(&buffer_latch);
	//printf("buffer latch acquire!!\n");
	for(i = 0; i < num_buf; i++){
		if(buffer[i]->page_num == 0 && buffer[i]->table_id == id_table){
			choice = (buffer_header*)buffer[i];
			head = choice->header;
			//pinning
			//pinning(buffer[i]);
			if(pthread_mutex_trylock(&buffer[i]->page_latch) == 0){
				//printf("headerpage latch acquire!!!\n");
			}
			//LRU list modify
			LRU_update(buffer[i]);
			pthread_mutex_unlock(&buffer_latch);
			//printf("buffer latch release\n");
			return head;
		}
	}
	//if not exist page in buffer
	if(i == num_buf){
		tmp = LRU_choice();
		choice = (buffer_header*)tmp;
		choice->header = get_headerpage();
		head = choice->header;
		tmp->page_num = 0;
		tmp->table_id = id_table;
		//pinning
		//pinning(tmp);
		pthread_mutex_trylock(&tmp->page_latch);
		pthread_mutex_unlock(&buffer_latch);
		//printf("buffer latch release\n");
		return head;
	}
}

void unpinning_headerpage(){
	int i;

	for(i = 0; i < num_buf; i++){
		if(buffer[i]->page_num == 0 && buffer[i]->table_id == id_table){
			//unpinning(buffer[i]);
			pthread_mutex_unlock(&buffer[i]->page_latch);
			//printf("headerlatch release\n");
		}
	}
}

void write_buffer_headerpage(){
	int i;

	pthread_mutex_lock(&buffer_latch);
	for(i = 0; i < num_buf; i++){
		if(buffer[i]->page_num == 0 && buffer[i]->table_id == id_table){
			modified_frame(buffer[i]);
			//unpinning(buffer[i]);
			pthread_mutex_unlock(&buffer[i]->page_latch);
			//("headlatch release\n");
		}
	}
	pthread_mutex_unlock(&buffer_latch);
}

page_t* read_buffer_page(pagenum_t page_num){
	page_t* page;
	int i;
	buffer_st* choice;

	pthread_mutex_lock(&buffer_latch);
	//printf("buffer latch acquire!!\n");
	for(i = 0; i < num_buf; i++){
		if(buffer[i]->page_num == page_num && buffer[i]->table_id == id_table){
			page = buffer[i]->frame;
			//pinning
			//pinning(buffer[i]);
			if(pthread_mutex_trylock(&buffer[i]->page_latch) == 0){
				//printf("page latch acquire!!\n");
			}
			//LRU list modify
			LRU_update(buffer[i]);
			pthread_mutex_unlock(&buffer_latch);
			//printf("buffer latch release\n");
			return page;
		}
	}
	//if not exist page in buffer
	if(i == num_buf){
		page = (page_t*)calloc(1, PAGE_SIZE);
		file_read_page(page_num, page);
		choice = LRU_choice();
		choice->frame = page;
		choice->table_id = id_table;
		choice->page_num = page_num;
		//pinning
		//pinning(choice);
		pthread_mutex_lock(&choice->page_latch);
		pthread_mutex_unlock(&buffer_latch);
		//printf("buffer latch release\n");
		return page;
	}
}

void unpinning_page(pagenum_t page_num){
	int i;

	for(i = 0; i < num_buf; i++){
		if(buffer[i]->page_num == page_num && buffer[i]->table_id == id_table){
			//unpinning(buffer[i]);
			pthread_mutex_unlock(&buffer[i]->page_latch);
			//printf("page latch release\n");
		}
	}
}

void write_buffer_page(pagenum_t page_num){
	int i;

	pthread_mutex_lock(&buffer_latch);
	for(i = 0; i < num_buf; i++){
		if(buffer[i]->page_num == page_num && buffer[i]->table_id == id_table){
			modified_frame(buffer[i]);
			//unpinning(buffer[i]);
			pthread_mutex_unlock(&buffer[i]->page_latch);
			//printf("page latch release\n");
		}
	}
	pthread_mutex_unlock(&buffer_latch);
}

void frame_flush(pagenum_t page_num, buffer_st* drop){
	int fd_back;

	fd_back = fd;
	fd = table_list[drop->table_id - 1]->fd;
	file_write_page(page_num, drop->frame);
	fd = fd_back;
	memset(drop->frame, 0, PAGE_SIZE);
	drop->table_id = 0;
	drop->page_num = 0;
	drop->is_dirty = 0;
	pthread_mutex_init(&drop->page_latch, NULL);
}

void drop_frame(pagenum_t page_num, buffer_st* drop){
	if(drop == LRU_head){
		drop->prev_LRU = LRU_tail;
		LRU_tail->next_LRU = drop;
		LRU_tail = drop;
		LRU_head = drop->next_LRU;
		LRU_head->prev_LRU = NULL;
		LRU_tail->next_LRU = NULL;
		frame_flush(page_num, drop);
	}
	else if(drop == LRU_tail){
		frame_flush(page_num, drop);
	}
	else{
		drop->next_LRU->prev_LRU = drop->prev_LRU;
		drop->prev_LRU->next_LRU = drop->next_LRU;
		drop->prev_LRU = LRU_tail;
		LRU_tail->next_LRU = drop;
		LRU_tail = drop;
		drop->next_LRU = NULL;
		frame_flush(page_num, drop);
	}
}


