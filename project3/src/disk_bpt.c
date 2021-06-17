#include "disk_bpt.h"

//Utility function
pagenum_t get_rootpage_num(){
    header* head;
    pagenum_t rootpage_num;

    head = read_buffer_headerpage();
    rootpage_num = head->root;
	unpinning_headerpage();
    return rootpage_num;
}

int cut(int length){
    if(length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

//Used by find function
pagenum_t find_leafpage(uint64_t key){
    pagenum_t pagenum, tmp;
    int i;
    page_t* page;
	internal_record* find_record;
	internal_page* internalpage;

    pagenum = get_rootpage_num();
	if(pagenum == 0)
		return pagenum;
	page = read_buffer_page(pagenum);
    while(!page->isleaf){
		internalpage = (internal_page*)page;
        i = 0;
        while(i < internalpage->num_keys){
			find_record = &internalpage->data[i];
            if(key > find_record->key) i++;
            else break;
        }
        if(i == 0){
            if(key < find_record->key){
				tmp = pagenum;
                pagenum = internalpage->otherp;
				unpinning_page(tmp);
				page = read_buffer_page(pagenum);
            }
            else{
				tmp = pagenum;
                pagenum = find_record->page_num;
				unpinning_page(tmp);
				page = read_buffer_page(pagenum);
            }
        }
        else{
           if(key < find_record->key){
			   i--;
			   find_record = &internalpage->data[i];
			   tmp = pagenum;
               pagenum = find_record->page_num;
			   unpinning_page(tmp);
			   page = read_buffer_page(pagenum);
            }
            else{
				tmp = pagenum;
                pagenum = find_record->page_num;
				unpinning_page(tmp);
				page = read_buffer_page(pagenum);
            }
        }
    }
	unpinning_page(pagenum);
    return pagenum;
}

record* find(uint64_t key){
    int i;
    page_t* page;
	leaf_page* leafpage;
    pagenum_t pagenum;

    pagenum = find_leafpage(key);
	page = read_buffer_page(pagenum);
    if(pagenum == 0){
		unpinning_page(pagenum);
		return NULL;
	}
	leafpage = (leaf_page*)page;
    for(i = 0; i < leafpage->num_keys; i++){
        if(leafpage->data[i].key == key) break;
    }
    if(i == leafpage->num_keys){
		unpinning_page(pagenum);
        return NULL;
    }
    else{
		unpinning_page(pagenum);
        return &leafpage->data[i];
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

int get_left_index(pagenum_t parentpagenum, pagenum_t leftpagenum){
    int left_index = 1;
    page_t* page;
	internal_page* parent;
    internal_record* left;

	page = read_buffer_page(parentpagenum);
	parent = (internal_page*)page;
	left = &parent->data[0];
    while(left_index <= parent->num_keys && left->page_num != leftpagenum){
		left = &parent->data[left_index];
        left_index++;
    }
    if((left_index - 1) == parent->num_keys){
        left_index = 0;
    }
	unpinning_page(parentpagenum);
    return left_index;
}

void insert_into_leafpage(pagenum_t leaf_pagenum, record* insert_record){
    int i;
    int insertion_point;
    page_t* page;
    record* compare_record;
	leaf_page* leafpage;

    insertion_point = 0;
	page = read_buffer_page(leaf_pagenum);
	leafpage = (leaf_page*)page;
	compare_record = &leafpage->data[insertion_point];
    while(insertion_point < leafpage->num_keys && compare_record->key < insert_record->key){
        insertion_point++;
		compare_record = &leafpage->data[insertion_point];
    }
    for(i = leafpage->num_keys; i > insertion_point; i--){
		leafpage->data[i] = leafpage->data[i - 1];
    }
	leafpage->data[insertion_point] = *insert_record;
    leafpage->num_keys++;
	write_buffer_page(leaf_pagenum);
}
 
void insert_into_leafpage_after_splitting(pagenum_t leafpage_num, record* insert_record){
    header* head;
	pagenum_t new_leafpage_num;
    page_t* page;
    page_t* new_page;
	leaf_page* leaf;
	leaf_page* new_leaf;
    record* compare;
	record tmp[LEAF_ORDER];
    internal_record* giving_parent;
    int insertion_index, split, i, j;
 
    head = read_buffer_headerpage();
    if(head->free == 0){
        make_new_freepage();
    }
	unpinning_headerpage();

    giving_parent = (internal_record*)calloc(1, sizeof(internal_record));
	page = read_buffer_page(leafpage_num);
	leaf = (leaf_page*)page;
    new_leafpage_num = file_alloc_page();
	new_page = read_buffer_page(new_leafpage_num);
	new_leaf = (leaf_page*)new_page;
	new_leaf->isleaf = 1;

    insertion_index = 0;
	compare = &leaf->data[insertion_index];
    while(insertion_index < (LEAF_ORDER - 1) && compare->key < insert_record->key){
        insertion_index++;
		compare = &leaf->data[insertion_index];
    }
	for(i = 0, j = 0; i < leaf->num_keys; i++, j++){
        if(j == insertion_index) j++;
		tmp[j] = leaf->data[i];
    }
    tmp[insertion_index] = *insert_record;
    leaf->num_keys = 0;
    for(i = 0; i < (PAGE_SIZE - PAGE_HEADER); i++){
        page->KeyArea[i] = 0;
    }
    split = cut(LEAF_ORDER - 1);
    for(i = 0; i < split; i++){
		leaf->data[i] = tmp[i];
        leaf->num_keys++;
    }
    for(i = split, j = 0; i < LEAF_ORDER; i++, j++){
		new_leaf->data[j] = tmp[i];
        new_leaf->num_keys++;
    }
    new_leaf->otherp = leaf->otherp;
    leaf->otherp = new_leafpage_num;
    new_leaf->NoP = leaf->NoP;
	giving_parent->key = new_leaf->data[0].key;
    giving_parent->page_num = new_leafpage_num;
	write_buffer_page(leafpage_num);
	write_buffer_page(leafpage_num);
    insert_into_parent(leafpage_num, giving_parent, new_leafpage_num);
}

void insert_into_nodepage(pagenum_t parent, int left_index, internal_record* record, pagenum_t right){
    int i;
    page_t* page;
	internal_page* internal;

	page = read_buffer_page(parent);
	internal = (internal_page*)page;
    for(i = internal->num_keys; i > left_index; i--){
		internal->data[i] = internal->data[i - 1];
    }
	internal->data[left_index] = *record;
    internal->num_keys++;
	write_buffer_page(parent);
} 

void insert_into_nodepage_after_splitting(pagenum_t old_node, int left_index, internal_record* record, pagenum_t right){
    int i, j, split;
	header* head;
    page_t* page;
	page_t* new_page;
	internal_page* internal;
	internal_page* new_internal;
    page_t* child;
    internal_record* k_prime;
	internal_record tmp[INTERNAL_ORDER];
    pagenum_t new_nodepage;

    k_prime = (internal_record*)calloc(1, sizeof(internal_record));
	page = read_buffer_page(old_node);
	internal = (internal_page*)page;
    for(i = 0, j = 0; i < internal->num_keys; i++, j++){
        if(j == left_index) j++;
		tmp[j] = internal->data[i];
    }
    tmp[left_index] = *record;
    internal->num_keys = 0;
    for(i = 0; i < (PAGE_SIZE - PAGE_HEADER); i++){
        page->KeyArea[i] = 0;
    }
    split = cut(INTERNAL_ORDER);
	head = read_buffer_headerpage();
    if(head->free == 0){
        make_new_freepage();
    }
	unpinning_headerpage();

    new_nodepage = file_alloc_page();
	new_page = read_buffer_page(new_nodepage);
	new_internal = (internal_page*)new_page;

    for(i = 0; i < split - 1; i++){
		internal->data[i] = tmp[i];
        internal->num_keys++;
    }
    for(i++, j = 0; i < INTERNAL_ORDER; i++, j++){
		new_internal->data[j] = tmp[i];
        new_internal->num_keys++;
    }
    new_internal->NoP = internal->NoP;
    new_internal->otherp = tmp[split - 1].page_num;
    
	k_prime->key = tmp[split - 1].key;
    k_prime->page_num = new_nodepage;
	child = read_buffer_page(new_internal->otherp); 
    child->NoP = new_nodepage;
	write_buffer_page(new_internal->otherp);
    for(i = 0; i < new_internal->num_keys; i++){
		child = read_buffer_page(new_internal->data[i].page_num);
        child->NoP = new_nodepage;
		write_buffer_page(new_internal->data[i].page_num);
    }
	write_buffer_page(old_node);
	write_buffer_page(new_nodepage);
    insert_into_parent(old_node, k_prime, new_nodepage);
}

void insert_into_parent(pagenum_t leftpagenum, internal_record* key, pagenum_t rightpagenum){
    int left_index;
    pagenum_t parentpagenum;
    page_t* page;

	page = read_buffer_page(leftpagenum);
    parentpagenum = page->NoP;
	unpinning_page(leftpagenum);

    if(parentpagenum == 0){
         insert_into_new_rootpage(leftpagenum, key, rightpagenum);
    }
    else{
        left_index = get_left_index(parentpagenum, leftpagenum);
		page = read_buffer_page(parentpagenum);
        if(page->num_keys < INTERNAL_ORDER - 1){
			unpinning_page(parentpagenum);
            insert_into_nodepage(parentpagenum, left_index, key, rightpagenum);
		}
        else{
			unpinning_page(parentpagenum);
            insert_into_nodepage_after_splitting(parentpagenum, left_index, key, rightpagenum);
		}
    }
}

void insert_into_new_rootpage(pagenum_t leftpagenum, internal_record* key, pagenum_t rightpagenum){
    pagenum_t rootpagenum, freepage_num;
    header* head;
    page_t* page;
	internal_page* root;

	head = read_buffer_headerpage();
    if(head->free == 0){
        make_new_freepage();
    }
	unpinning_headerpage();
    rootpagenum = file_alloc_page();
	page = read_buffer_page(rootpagenum);
	root = (internal_page*)page;
	root->data[0] = *key;
    root->num_keys++;
	root->otherp = leftpagenum;
	write_buffer_page(rootpagenum);
    
	page = read_buffer_page(leftpagenum);
    page->NoP = rootpagenum;
	write_buffer_page(leftpagenum);
    
	page = read_buffer_page(rightpagenum);
    page->NoP = rootpagenum;
	write_buffer_page(rightpagenum);
    
	head = read_buffer_headerpage();
    head->root = rootpagenum;
	write_buffer_headerpage();
}

void start_new_tree(record* insert_record){
    pagenum_t rootpagenum;
    header* head;
    page_t* page;
	leaf_page* leaf;

    rootpagenum = file_alloc_page();
	page = read_buffer_page(rootpagenum);
	leaf = (leaf_page*)page;
	leaf->isleaf = 1;
	leaf->data[0] = *insert_record;
    leaf->num_keys++;
	write_buffer_page(rootpagenum);
	head = read_buffer_headerpage();
    head->root = rootpagenum;
	write_buffer_headerpage();
}

//Used by deletion function
int get_neighbor_index(pagenum_t pagenum){
    int i;
    pagenum_t neighbor_index, parent_num;
    page_t* page;
	internal_page* parent;

	page = read_buffer_page(pagenum);
    parent_num = page->NoP;
	unpinning_page(pagenum);
	page = read_buffer_page(parent_num);
	parent = (internal_page*)page;
    for(i = 0; i < parent->num_keys; i++){
        if(parent->data[i].page_num == pagenum){
			unpinning_page(parent_num);
            return i;
        }
    }
    //case : Leftmost Child
    if(i == parent->num_keys){
		unpinning_page(parent_num);
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
	page_t* tmp;
	internal_page* internal;
	leaf_page* leaf;

	page = read_buffer_page(page_num);
    if(!page->isleaf){
		internal = (internal_page*)page;
        init_internal = (internal_record*)calloc(1, sizeof(internal_record));
        i = 0;
		compare_internal = &internal->data[i];
        while(compare_internal->key != key){
            i++;
			compare_internal = &internal->data[i];
        }
        if(i == 0){
			tmp = read_buffer_page(internal->otherp);
            if(tmp->num_keys == 0){
				unpinning_page(internal->otherp);
                internal->otherp = compare_internal->page_num;
            }
        }
        for(++i; i < page->num_keys; i++){
			internal->data[i - 1] = internal->data[i];
        }
		if(i == page->num_keys){
			internal->data[i - 1] = *init_internal;
        }
    }
    else{
		leaf = (leaf_page*)page;
        init = (record*)calloc(1, sizeof(record));
        i = 0;
		compare = &leaf->data[i];
        while(compare->key != key){
             i++;
			 compare = &leaf->data[i];
        }
        for(++i; i < page->num_keys; i++){
			leaf->data[i - 1] = leaf->data[i];
        }
        if(i == page->num_keys){
			leaf->data[i - 1] = *init;
        }
    }
    page->num_keys--;
	write_buffer_page(page_num);
}

void moving_isolated_leaf(pagenum_t rootpage){
    page_t* page;
	page_t* isolate_page;
	internal_page* internal;
	leaf_page* leaf;
    internal_record* compare;
    record* isolate;
    pagenum_t pagenum, isolated_page;
    pagenum_t i, tmp;
    uint64_t key;

	pagenum = rootpage;
	page = read_buffer_page(pagenum);
    while(!page->isleaf){
		internal = (internal_page*)page;
		compare = &internal->data[internal->num_keys - 1];
		tmp = pagenum;
        pagenum = compare->page_num;
		unpinning_page(tmp);
		page = read_buffer_page(pagenum);
    }
    if(page->otherp == 0){
		pagenum = rootpage;
		page = read_buffer_page(pagenum);
        while(!page->isleaf){
			tmp = pagenum;
            pagenum = page->otherp;
			unpinning_page(tmp);
			page = read_buffer_page(pagenum);
        }
        i = 0;
        while(page->otherp != pagenum){
			unpinning_page(i);
            i++;
			page = read_buffer_page(i);
        }
        isolated_page = i;
		unpinning_page(i);
		isolate_page = read_buffer_page(isolated_page);
		leaf = (leaf_page*)isolate_page;
		for(i = 0; i < page->num_keys; i++){
			isolate = &leaf->data[i];
			pagenum = find_leafpage(isolate->key);
			page = read_buffer_page(pagenum);
			if(page->num_keys < LEAF_ORDER - 1){
			   insert_into_leafpage(pagenum, isolate);
			}
		    else
			   insert_into_leafpage_after_splitting(pagenum, isolate);
        }
		unpinning_page(isolated_page);
        file_free_page(isolated_page);
    }
    else{
        isolated_page = page->otherp;
        page->otherp = 0;
		write_buffer_page(pagenum);
		isolate_page = read_buffer_page(isolated_page);
		leaf = (leaf_page*)isolate_page;
        for(i = 0; i < leaf->num_keys; i++){
			isolate = &leaf->data[i];
			pagenum = find_leafpage(isolate->key);
			page = read_buffer_page(pagenum);
		    if(page->num_keys < LEAF_ORDER - 1){
				insert_into_leafpage(pagenum, isolate);
			}
		    else
				insert_into_leafpage_after_splitting(pagenum, isolate);
        }
		unpinning_page(isolated_page);
        file_free_page(isolated_page);
    }
}

void adjust_root(){
    header* head;
    page_t* page;
	pagenum_t rootpage, tmp;
	int isleaf;

    head = read_buffer_headerpage();
	page = read_buffer_page(head->root);
    if(page->num_keys > 0){
		unpinning_page(head->root);
		unpinning_headerpage();
    }
    else{
        if(!page->isleaf){
            tmp = page->otherp;
            file_free_page(head->root);
			head = read_buffer_headerpage();
            head->root = tmp;
			rootpage = head->root;
			write_buffer_headerpage();
			page = read_buffer_page(rootpage);
            page->NoP = 0;
			isleaf = page->isleaf;
			write_buffer_page(rootpage);
            if(!isleaf){
                moving_isolated_leaf(rootpage);
            }
        }
        else{
			unpinning_page(head->root);
			unpinning_headerpage();
        }
    }
}

void coalesce_nodes(pagenum_t page_num, pagenum_t neighbor_num, int neighbor_index, uint64_t k_prime){
    page_t* page;
    pagenum_t left_sibling_num, right_sibling_num, pagenum, tmp, giving_otherp, parent;

	page = read_buffer_page(page_num);
    parent = page->NoP;
    if(!page->isleaf){
		unpinning_page(page_num);
        file_free_page(page_num);
    }
    else{
        pagenum = get_rootpage_num();
		page = read_buffer_page(pagenum);
        while(!page->isleaf){
			tmp = pagenum;
            pagenum = page->otherp;
			unpinning_page(tmp);
			page = read_buffer_page(pagenum);
        }
        if(pagenum == page_num){
			unpinning_page(page_num);
            file_free_page(page_num);
        }
        else{
            left_sibling_num = pagenum;
            right_sibling_num = page->otherp;
			unpinning_page(pagenum);
            while(right_sibling_num != page_num){
				page = read_buffer_page(right_sibling_num);
                left_sibling_num = right_sibling_num;
                right_sibling_num = page->otherp;
				unpinning_page(left_sibling_num);
            }
			page = read_buffer_page(right_sibling_num);
            giving_otherp = page->otherp;
			unpinning_page(right_sibling_num);
			page = read_buffer_page(left_sibling_num);
            page->otherp = giving_otherp;
			write_buffer_page(left_sibling_num);
            file_free_page(page_num);
        }
    }
    delete_entry(parent, k_prime);
}

void delete_entry(pagenum_t page_num, uint64_t key){
    header* head;
    page_t* page;
	internal_page* parent;
    int capacity, neighbor_index, k_prime_index;
    uint64_t k_prime;
    pagenum_t neighbor, parent_num, first_neighbor;

    head = read_buffer_headerpage();
    remove_entry_from_node(page_num, key);
    if(page_num == head->root){
        adjust_root();
		page = read_buffer_page(page_num);
		if(page->isleaf && page->num_keys == 0){
			file_free_page(page_num);
			head = read_buffer_headerpage();
			head->root = 0;
			write_buffer_headerpage();
		}
    }
    else{
        neighbor_index = get_neighbor_index(page_num);
		page = read_buffer_page(page_num);
        parent_num = page->NoP;
		unpinning_page(page_num);
		page = read_buffer_page(parent_num);
		parent = (internal_page*)page;
        if(neighbor_index == 300){
            neighbor = parent->data[0].page_num;
        }
        else{
            if(neighbor_index == 0){
                neighbor = parent->otherp;
            }
            else{
                neighbor = parent->data[neighbor_index - 1].page_num;
            }
        }
        k_prime_index = neighbor_index == 300 ? 0 : neighbor_index;
        k_prime =parent->data[k_prime_index].key;
		unpinning_page(parent_num);
		page = read_buffer_page(page_num);
        if(page->num_keys == 0){ 
			unpinning_page(page_num);
            //Coalescence
            coalesce_nodes(page_num, neighbor, neighbor_index, k_prime);
        }
		else
			unpinning_page(page_num);
    }
}   

