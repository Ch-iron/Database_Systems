#include "interface.h"

 int main(){
    uint64_t key;
    char command;
    int success;
	int i;

	num_buf = 10;
	init_db(num_buf);
    while(1){
        printf("> ");
        scanf("%c", &command);
        switch(command){
            case 's':
                success = shutdown_db();
                if(success == 0)
                    printf("shutdown DB\n");
                else
                        printf("shutdown fail\n");
                break;
			case 'o':
				printf("Input file_name : ");
				scanf("%s", pathname);
				table_id = open_table(pathname);
				if(table_id > 0){
					fd = table_list[table_id - 1]->fd;
				}
				else{
					printf("Open Fail!!!!!!!!!!!!!!!!!!!\n");
				}
				break;
			case 'c':
				printf("Input table_id : ");
				scanf("%d", &table_id);
				success = close_table(table_id);
				if(success == 0)
					printf("%s's fd : %d\n", table_list[table_id - 1]->pathname, table_list[table_id - 1]->fd);
				break;
            case 'i':
				printf("Input table_id : ");
				scanf("%d", &table_id);
                printf("Input key & value : ");
                scanf("%ld %s", &key, value);
                success = db_insert(table_id, key, value);
                if(success == 0)
                    printf("Insertion Success\n");
                else
                    printf("Insertion Fail\n");
                break;
            case 'd':
				printf("Input table_id : ");
				scanf("%d", &table_id);
                printf("Input key : ");
                scanf("%ld", &key);
                success = db_delete(table_id, key);
                if(success == 0){
                    printf("Deletion Success\n");
                }
                else
                    printf("Deletion Fail\n");
                break;
            case 'f':
				printf("Input table_id : ");
				scanf("%d", &table_id);
                printf("Input key : ");
                scanf("%ld", &key);
                success = db_find(table_id, key, value);
                if(success == 0){
                    printf("Find Success\n");
                    printf("Value : %s\n", value);
                }
                else
                    printf("Not Exist\n");
                break;
            case 'q':
                printf("quit\n");
                return 0;
        }
    }
    return 0;
}

