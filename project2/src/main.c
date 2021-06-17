#include "interface.h"

 int main(){
    uint64_t key;
    char* value;
    char command;
    int success;

    pathname = (char*)calloc(1, sizeof(char));
	value = (char*)calloc(1, sizeof(char));
    printf("Input file_name : ");
    scanf("%s", pathname);
    success = open_table(pathname);
	if(success > 0){
		printf("Open Success\nUnique table id : %d\n", success);
	}
	else{
		printf("Open Fail\n");
		return 0;
	}
    while(1){
        printf("> ");
        scanf("%c", &command);
        switch(command){
            case 'i':
                printf("Input key & value : ");
                scanf("%ld %s", &key, value);
                success = db_insert(key, value);
                if(success == 0)
                    printf("Insertion Success\n");
                else
                    printf("Insertion Fail\n");
                break;
            case 'd':
                printf("Input key : ");
                scanf("%ld", &key);
                success = db_delete(key);
                if(success == 0){
                    printf("Deletion Success\n");
                }
                else
                    printf("Deletion Fail\n");
                break;
            case 'f':
                printf("Input key : ");
                scanf("%ld", &key);
                success = db_find(key, value);
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

