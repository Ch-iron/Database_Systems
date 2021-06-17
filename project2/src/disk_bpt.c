#include "disk_bpt.h"

//Utility function
pagenum_t get_rootpage_num(){
    header* head;
    pagenum_t rootpage_num;

    head = get_headerpage();
    rootpage_num = head->root;
    free(head);
    return rootpage_num;
}

int cut(int length){
    if(length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

//Used by open function
void make_headerpage(pagenum_t fnum, pagenum_t rnum, uint64_t num){
    header* head;

    head = (header*)calloc(1, PAGE_SIZE);
    head->free = fnum;
    head->root = rnum;
    head->numpage = num;
    write_headerpage(head);
    free(head);
} 

void create_new_file(){
    header* head;
    pagenum_t freepage_num;

    make_headerpage(0, 0, 1);
    head = get_headerpage();
    make_new_freepage();
    free(head);
}

//Used by find function
pagenum_t find_leafpage(uint64_t key){
    pagenum_t pagenum;
    int i;
    page_t* page;
    internal_record* find_record;

    page = (page_t*)calloc(1, PAGE_SIZE);
    find_record = (internal_record*)calloc(1, sizeof(internal_record));
    pagenum = get_rootpage_num();
	if(pagenum == 0)
		return pagenum;
    file_read_page(pagenum, page);
    while(!page->isleaf){
        i = 0;
        while(i < page->num_keys){
             file_read_internal_record(pagenum, find_record, i);
            if(key > find_record->key) i++;
            else break;
        }
        if(i == 0){
            if(key < find_record->key){
                pagenum = page->otherp;
                file_read_page(pagenum, page);
            }
            else{
                pagenum = find_record->page_num;
                file_read_page(pagenum, page);
            }
        }
        else{
           if(key < find_record->key){
                i--;
                file_read_internal_record(pagenum, find_record, i);
                pagenum = find_record->page_num;
                file_read_page(pagenum, page);
            }
            else{
                pagenum = find_record->page_num;
                file_read_page(pagenum, page);
            }
        }
    }
    free(page);
    free(find_record);
    return pagenum;
}

record* find(uint64_t key){
    int i;
    page_t* page;
    record* find_record;
    pagenum_t pagenum;

    page = (page_t*)calloc(1, PAGE_SIZE);
    find_record = (record*)calloc(1, sizeof(record));
    pagenum = find_leafpage(key);
    file_read_page(pagenum, page);
    if(pagenum == 0) return NULL;
    for(i = 0; i < page->num_keys; i++){
        file_read_record(pagenum, find_record, i);
        if(find_record->key == key) break;
    }
    if(i == page->num_keys){
        free(page);
        free(find_record);
        return NULL;
    }
    else{
        free(page);
        return find_record;
    }
}

//Used by Insertion function
record* make_record(uint64_t key, char* value){
    record* new_record;

    new_record = (record*)calloc(1, sizeof(record));
    new_record->key = key;
    sprintf(new_record->value, "%s", value);
    return new_record;
} 

void make_new_freepage(){
    page_t* page;
    header* head;
    pagenum_t num_page;
    pagenum_t last_freepage_num;

    page = (page_t*)calloc(1, PAGE_SIZE);
    head = get_headerpage();
	file_free_page(head->numpage);
	head = get_headerpage();
	head->numpage++;
	write_headerpage(head);
    free(head);
    free(page);
}

void make_leafpage(pagenum_t makeleaf){
    page_t* page;

    page = (page_t*)calloc(1, PAGE_SIZE);
    file_read_page(makeleaf, page);
    page->isleaf = 1;
    file_write_page(makeleaf, page);
    free(page);
}

int get_left_index(pagenum_t parentpagenum, pagenum_t leftpagenum){
    int left_index = 1;
    page_t* parent;
    internal_record* left;

    parent = (page_t*)calloc(1, PAGE_SIZE);
    left = (internal_record*)calloc(1, sizeof(internal_record));
    file_read_page(parentpagenum, parent);
    file_read_internal_record(parentpagenum, left, 0);
    while(left_index <= parent->num_keys && left->page_num != leftpagenum){
        file_read_internal_record(parentpagenum, left, (left_index));
        left_index++;
    }
    if((left_index - 1) == parent->num_keys){
        left_index = 0;
    }
    return left_index;
}

void set_other_pagenum(pagenum_t n, pagenum_t other_pagenum){
    page_t* page;

    page = (page_t*)calloc(1, PAGE_SIZE);
    file_read_page(n, page);
    page->otherp = other_pagenum;
    file_write_page(n, page);
}

void insert_into_leafpage(pagenum_t leaf_pagenum, record* insert_record){
    int i;
    int insertion_point;
    page_t* leaf;
    record* compare_record;

    leaf = (page_t*)calloc(1, PAGE_SIZE);
    compare_record = (record*)calloc(1, sizeof(record));
    insertion_point = 0;
    file_read_page(leaf_pagenum, leaf);
    file_read_record(leaf_pagenum, compare_record, insertion_point);
    while(insertion_point < leaf->num_keys && compare_record->key < insert_record->key){
        insertion_point++;
        file_read_record(leaf_pagenum, compare_record, insertion_point);
    }
    for(i = leaf->num_keys; i > insertion_point; i--){
        file_read_record(leaf_pagenum, compare_record, i - 1);
        file_write_record(leaf_pagenum, compare_record, i);
    }
    file_write_record(leaf_pagenum, insert_record, insertion_point);
    file_read_page(leaf_pagenum, leaf);
    leaf->num_keys++;
    file_write_page(leaf_pagenum, leaf);
    file_read_record(leaf_pagenum, insert_record, insertion_point);
    free(leaf);
    free(compare_record);
}
 
void insert_into_leafpage_after_splitting(pagenum_t leafpage_num, record* insert_record){
    header* head;
	pagenum_t new_leafpage_num;
    page_t* leaf;
    page_t* new_leaf;
    record* compare;
    record** tmp;
    record** compare_records;
    internal_record* giving_parent;
    int insertion_index, split, i, j;
    uint64_t new_key;
 
    head = get_headerpage();
    if(head->free == 0){
        make_new_freepage();
    }
    new_leafpage_num = file_alloc_page();
    make_leafpage(new_leafpage_num);

    leaf = (page_t*)calloc(1, PAGE_SIZE);
    new_leaf = (page_t*)calloc(1, PAGE_SIZE);
    compare = (record*)calloc(1, sizeof(record));
    compare_records = (record**)calloc(LEAF_ORDER - 1, sizeof(record*));
    tmp = (record**)calloc(LEAF_ORDER, sizeof(record*));
    giving_parent = (internal_record*)calloc(1, sizeof(internal_record));
    for(i = 0; i < LEAF_ORDER - 1; i++){
        compare_records[i] = (record*)calloc(1, sizeof(record));
    }
    for(i = 0; i < LEAF_ORDER; i++){
        tmp[i] = (record*)calloc(1, sizeof(record));
    }
    file_read_page(leafpage_num, leaf);
    file_read_page(new_leafpage_num, new_leaf);
    insertion_index = 0;
    file_read_record(leafpage_num, compare, insertion_index);
    while(insertion_index < (LEAF_ORDER - 1) && compare->key < insert_record->key){
        insertion_index++;
        file_read_record(leafpage_num, compare, insertion_index);
    }
	for(i = 0, j = 0;i < leaf->num_keys; i++, j++){
        if(j == insertion_index) j++;
        file_read_record(leafpage_num, compare_records[i], i);
        tmp[j] = compare_records[i];
    }
    tmp[insertion_index] = insert_record;
    leaf->num_keys = 0;
    for(i = 0; i < (PAGE_SIZE - PAGE_HEADER); i++){
        leaf->KeyArea[i] = 0;
    }
    file_write_page(leafpage_num, leaf);
    split = cut(LEAF_ORDER - 1);
    for(i = 0; i < split; i++){
        file_write_record(leafpage_num, tmp[i], i);
        file_read_page(leafpage_num, leaf);
        leaf->num_keys++;
        file_write_page(leafpage_num, leaf);
    }
    for(i = split, j = 0; i < LEAF_ORDER; i++, j++){
        file_write_record(new_leafpage_num, tmp[i], j);
        file_read_page(new_leafpage_num, new_leaf);
        new_leaf->num_keys++;
        file_write_page(new_leafpage_num, new_leaf);
    }
    file_read_page(leafpage_num, leaf);
    file_read_page(new_leafpage_num, new_leaf);
    new_leaf->otherp = leaf->otherp;
    leaf->otherp = new_leafpage_num;
    file_write_page(leafpage_num, leaf);
    file_write_page(new_leafpage_num, new_leaf);
    file_read_page(leafpage_num, leaf);
    file_read_page(new_leafpage_num, new_leaf);
    new_leaf->NoP = leaf->NoP;
    file_write_page(new_leafpage_num, new_leaf);
    file_read_record(new_leafpage_num, compare, 0);
    giving_parent->key = compare->key;
    giving_parent->page_num = new_leafpage_num;
    free(tmp);
    free(compare);
    free(compare_records);
    insert_into_parent(leafpage_num, giving_parent, new_leafpage_num);
}

void insert_into_nodepage(pagenum_t n, int left_index, internal_record* record, pagenum_t right){
    int i;
    internal_record* tmp;
    page_t* page;

    page = (page_t*)calloc(1, PAGE_SIZE);
    tmp = (internal_record*)calloc(1, sizeof(internal_record));
    file_read_page(n, page);
    for(i = page->num_keys; i > left_index; i--){
        file_read_internal_record(n, tmp, i - 1);
        file_write_internal_record(n, tmp, i);
    }
    file_write_internal_record(n, record, left_index);
    file_read_page(n, page);
    page->num_keys++;
    file_write_page(n, page);
    free(tmp);
    free(page);
} 

void insert_into_nodepage_after_splitting(pagenum_t old_node, int left_index, internal_record* record, pagenum_t right){
    int i, j, split;
	header* head;
    page_t* internal;
    page_t* child;
    internal_record* k_prime;
    internal_record* childpage;
    internal_record** compare_records;
    internal_record** tmp;
    pagenum_t new_nodepage;
    pagenum_t parent, childpagenum;

    internal = (page_t*)calloc(1, PAGE_SIZE);
    child = (page_t*)calloc(1, PAGE_SIZE);
    k_prime = (internal_record*)calloc(1, sizeof(internal_record));
    childpage = (internal_record*)calloc(1, sizeof(internal_record));
    compare_records = (internal_record**)calloc(INTERNAL_ORDER - 1, sizeof(internal_record*));
    tmp = (internal_record**)calloc(INTERNAL_ORDER, sizeof(internal_record*));
    for(i = 0; i < INTERNAL_ORDER - 1; i++){
        compare_records[i] = (internal_record*)calloc(1, sizeof(internal_record));
    }
    for(i = 0; i < INTERNAL_ORDER; i++){
        tmp[i] = (internal_record*)calloc(1, sizeof(internal_record));
    }
    file_read_page(old_node, internal);
    parent = internal->NoP;
    for(i = 0, j = 0; i < internal->num_keys; i++, j++){
        if(j == left_index) j++;
        file_read_internal_record(old_node, compare_records[i], i);
        tmp[j] = compare_records[i];
    }
    tmp[left_index] = record;
    internal->num_keys = 0;
    for(i = 0; i < (PAGE_SIZE - PAGE_HEADER); i++){
        internal->KeyArea[i] = 0;
    }
	file_write_page(old_node, internal);
    split = cut(INTERNAL_ORDER);
	head = get_headerpage();
    if(head->free == 0){
        make_new_freepage();
   }
   new_nodepage = file_alloc_page();
   for(i = 0; i < split - 1; i++){
        file_write_internal_record(old_node, tmp[i], i);
        file_read_page(old_node, internal);
        internal->num_keys++;
        file_write_page(old_node, internal);
    }
    for(i++, j = 0; i < INTERNAL_ORDER; i++, j++){
        file_write_internal_record(new_nodepage, tmp[i], j);
        file_read_page(new_nodepage, internal);
        internal->num_keys++;
        file_write_page(new_nodepage, internal);
    }
    file_read_page(new_nodepage, internal);
    internal->NoP = parent;
    file_write_page(new_nodepage, internal);
    file_read_page(new_nodepage, internal);
    internal->otherp = tmp[split - 1]->page_num;
    file_write_page(new_nodepage, internal);
    k_prime->key = tmp[split - 1]->key;
    k_prime->page_num = new_nodepage;
    free(tmp);
    free(compare_records);
    file_read_page(internal->otherp, child);
    child->NoP = new_nodepage;
    file_write_page(internal->otherp, child);
    for(i = 0; i < internal->num_keys; i++){
        file_read_internal_record(new_nodepage, childpage, i);
        childpagenum = childpage->page_num;
        file_read_page(childpagenum, child);
        child->NoP = new_nodepage;
        file_write_page(childpagenum, child);
    }
    insert_into_parent(old_node, k_prime, new_nodepage);
}

void insert_into_parent(pagenum_t leftpagenum, internal_record* key, pagenum_t rightpagenum){
    int left_index;
    pagenum_t parentpagenum;
    page_t* page;

    page = (page_t*)calloc(1, PAGE_SIZE);
    file_read_page(leftpagenum, page);
    parentpagenum = page->NoP;

    if(parentpagenum == 0){
         insert_into_new_rootpage(leftpagenum, key, rightpagenum);
    }
    else{
        left_index = get_left_index(parentpagenum, leftpagenum);
        file_read_page(parentpagenum, page);
        if(page->num_keys < INTERNAL_ORDER - 1)
             insert_into_nodepage(parentpagenum, left_index, key, rightpagenum);
        else
            insert_into_nodepage_after_splitting(parentpagenum, left_index, key, rightpagenum);
    }
}

void insert_into_new_rootpage(pagenum_t leftpagenum, internal_record* key, pagenum_t rightpagenum){
    pagenum_t rootpagenum, freepage_num;
    page_t* page;
    header* head;

    page = (page_t*)calloc(1, PAGE_SIZE);
	head = get_headerpage();
    if(head->free == 0){
        make_new_freepage();
    }
    rootpagenum = file_alloc_page();
    set_other_pagenum(rootpagenum, leftpagenum);
    file_write_internal_record(rootpagenum, key, 0);
    file_read_page(rootpagenum, page);
    page->num_keys++;
    file_write_page(rootpagenum, page);
    file_read_page(leftpagenum, page);
    page->NoP = rootpagenum;
    file_write_page(leftpagenum, page);
    file_read_page(rightpagenum, page);
    page->NoP = rootpagenum;
    file_write_page(rightpagenum, page);
    head = get_headerpage();
    head->root = rootpagenum;
    write_headerpage(head);
    free(page);
    free(head);
}

void start_new_tree(record* insert_record){
    pagenum_t rootpagenum;
    page_t* page;
    header* head;

    page = (page_t*)calloc(1, PAGE_SIZE);
    rootpagenum = file_alloc_page();
    make_leafpage(rootpagenum);
    file_write_record(rootpagenum, insert_record, 0);
    file_read_page(rootpagenum, page);
    page->num_keys++;
    file_write_page(rootpagenum, page);
    head = get_headerpage();
    head->root = rootpagenum;
    write_headerpage(head);
    free(page);
    free(head);
}

//Used by deletion function
int get_neighbor_index(pagenum_t pagenum){
    int i;
    pagenum_t neighbor_index, parent_num;
    page_t* page;
    internal_record* internal;

    page = (page_t*)calloc(1, PAGE_SIZE);
    internal = (internal_record*)calloc(1, PAGE_SIZE);
    file_read_page(pagenum, page);
    parent_num = page->NoP;
    file_read_page(parent_num, page);
    for(i = 0; i < page->num_keys; i++){
        file_read_internal_record(parent_num, internal, i);
        if(internal->page_num == pagenum){
            return i;
        }
    }
    //case : Leftmost Child
    if(i == page->num_keys){
        return 300;
    }
}

void remove_entry_from_node(pagenum_t page_num, uint64_t key){
    int i;
    record* init;
    record* compare;
    internal_record* init_internal;
    internal_record* compare_internal;
    page_t* page;

    page = (page_t*)calloc(1, PAGE_SIZE);
    file_read_page(page_num, page);
    if(!page->isleaf){
        init_internal = (internal_record*)calloc(1, sizeof(internal_record));
        compare_internal = (internal_record*)calloc(1, sizeof(internal_record));
        i = 0;
        file_read_internal_record(page_num, compare_internal, i);
        while(compare_internal->key != key){
            i++;
            file_read_internal_record(page_num, compare_internal, i);
        }
        if(i == 0){
			file_read_page(page_num, page);
			file_read_page(page->otherp, page);
            if(page->num_keys == 0){
                file_read_page(page_num, page);
                page->otherp = compare_internal->page_num;
                file_write_page(page_num, page);
            }
        }
        file_read_page(page_num, page);
        for(++i; i < page->num_keys; i++){
            file_read_internal_record(page_num, compare_internal, i);
            file_write_internal_record(page_num, compare_internal, i - 1);
        }
		if(i == page->num_keys){
            file_write_internal_record(page_num, init_internal, i - 1);
        }
    }
    else{
        init = (record*)calloc(1, sizeof(record));
        compare = (record*)calloc(1, sizeof(record));
        i = 0;
        file_read_record(page_num, compare, i);
        while(compare->key != key){
             i++;
            file_read_record(page_num, compare, i);
        }
        for(++i; i < page->num_keys; i++){
            file_read_record(page_num, compare, i);
            file_write_record(page_num, compare, i - 1);
        }
        if(i == page->num_keys){
            file_write_record(page_num, init, i - 1);
        }
    }
    file_read_page(page_num, page);
    page->num_keys--;
    file_write_page(page_num,page);
}

void moving_isolated_leaf(pagenum_t rootpage){
    header* head;
    page_t* page;
    internal_record* compare;
    record* isolate;
    pagenum_t pagenum, isolated_page;
    pagenum_t i;
    uint64_t key;
    char* value;

    head = get_headerpage();
    page = (page_t*)calloc(1, PAGE_SIZE);
    compare = (internal_record*)calloc(1, PAGE_SIZE);
    isolate = (record*)calloc(1, PAGE_SIZE);
    value = (char*)calloc(120, sizeof(char));
    pagenum = rootpage;
    file_read_page(rootpage, page);
    while(!page->isleaf){
        file_read_internal_record(pagenum, compare, (page->num_keys - 1));
        pagenum = compare->page_num;
        file_read_page(pagenum, page);
    }
    if(page->otherp == 0){
        file_read_page(rootpage, page);
        while(!page->isleaf){
            pagenum = page->otherp;
            file_read_page(pagenum, page);
        }
        i = 0;
        while(page->otherp != pagenum){
            i++;
            file_read_page(i, page);
        }
        isolated_page = i;
        file_read_page(isolated_page, page);
		for(i = 0; i < page->num_keys; i++){
            file_read_record(isolated_page, isolate, i);
			pagenum = find_leafpage(isolate->key);
			file_read_page(pagenum, page);
			if(page->num_keys < LEAF_ORDER - 1){
			   insert_into_leafpage(pagenum, isolate);
			}
		    else
			   insert_into_leafpage_after_splitting(pagenum, isolate);
			file_read_page(isolated_page, page);
        }
        file_free_page(isolated_page);
    }
    else{
        isolated_page = page->otherp;
        page->otherp = 0;
        file_write_page(pagenum, page);
        file_read_page(isolated_page, page);
        for(i = 0; i < page->num_keys; i++){
            file_read_record(isolated_page, isolate, i);
			pagenum = find_leafpage(isolate->key);
	        file_read_page(pagenum, page);
		    if(page->num_keys < LEAF_ORDER - 1){
				insert_into_leafpage(pagenum, isolate);
			}
		    else
				insert_into_leafpage_after_splitting(pagenum, isolate);
			file_read_page(isolated_page, page);
        }
        file_free_page(isolated_page);
    }
}

void adjust_root(){
    header* head;
    page_t* page;

    head = get_headerpage();
    page = (page_t*)calloc(1, PAGE_SIZE);
    file_read_page(head->root, page);
    if(page->num_keys > 0){
        free(head);
        free(page);
    }
    else{
        if(!page->isleaf){
            file_free_page(head->root);
			head = get_headerpage();
            head->root = page->otherp;
            write_headerpage(head);
            file_read_page(head->root, page);
            page->NoP = 0;
            file_write_page(head->root, page);
            file_read_page(head->root, page);
            if(!page->isleaf){
                moving_isolated_leaf(head->root);
            }
        }
        else{
            free(head);
            free(page);
        }
    }
}

void coalesce_nodes(pagenum_t page_num, pagenum_t neighbor_num, int neighbor_index, uint64_t k_prime){
    page_t* page;
    pagenum_t left_sibling_num, right_sibling_num, pagenum, giving_otherp;
    internal_record* compare;

    page = (page_t*)calloc(1, PAGE_SIZE);
    file_read_page(page_num, page);
    if(!page->isleaf){
        file_free_page(page_num);
    }
    else{
        pagenum = get_rootpage_num();
        file_read_page(pagenum, page);
        while(!page->isleaf){
            pagenum = page->otherp;
            file_read_page(pagenum, page);
        }
        if(pagenum == page_num){
            file_read_page(page_num, page);
            file_free_page(page_num);
        }
        else{
            left_sibling_num = pagenum;
            right_sibling_num = page->otherp;
            while(right_sibling_num != page_num){
                file_read_page(page->otherp, page);
                left_sibling_num = right_sibling_num;
                right_sibling_num = page->otherp;
            }
            file_read_page(right_sibling_num, page);
            giving_otherp = page->otherp;
            file_read_page(left_sibling_num, page);
            page->otherp = giving_otherp;
            file_write_page(left_sibling_num, page);
            file_read_page(page_num, page);
            file_free_page(page_num);
        }
    }
    delete_entry(page->NoP, k_prime);
}

void delete_entry(pagenum_t page_num, uint64_t key){
    header* head;
    page_t* page;
    internal_record* internal;
    int capacity, neighbor_index, k_prime_index;
    uint64_t k_prime;
    pagenum_t neighbor, parent, first_neighbor;

    head = get_headerpage();
    page = (page_t*)calloc(1, PAGE_SIZE);
    internal = (internal_record*)calloc(1, sizeof(internal_record));
    remove_entry_from_node(page_num, key);
    if(page_num == head->root){
        adjust_root();
		file_read_page(page_num, page);
		if(page->isleaf && page->num_keys == 0){
			file_free_page(page_num);
			head = get_headerpage();
			head->root = 0;
			write_headerpage(head);
		}
    }
    else{
        neighbor_index = get_neighbor_index(page_num);
        file_read_page(page_num, page);
        parent = page->NoP;
        file_read_page(parent, page);
        if(neighbor_index == 300){
            file_read_internal_record(parent, internal, 0);
            neighbor = internal->page_num;
        }
        else{
            if(neighbor_index == 0){
                neighbor = page->otherp;
            }
            else{
                file_read_internal_record(parent, internal, (neighbor_index - 1));
                neighbor = internal->page_num;
            }
        }
        k_prime_index = neighbor_index == 300 ? 0 : neighbor_index;
        file_read_internal_record(parent, internal, k_prime_index);
        k_prime = internal->key;
        file_read_page(page_num, page);
        if(page->num_keys == 0){ 
            file_read_page(neighbor, page);
            //Coalescence
            coalesce_nodes(page_num, neighbor, neighbor_index, k_prime);
        }
    }
}   

