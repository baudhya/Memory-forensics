#include <stdio.h>
#include <stdlib.h>


int main(){
    char* M = (char *)malloc(10240 * 1024 * sizeof(char));
    char c = 0x41;
    for(int i = 0; i < 1024; i++){
        for(int j = 0; j < 1024; j++){
            M[i*1024 + j] = c;
            c++;
            if(c > 0x41 + 25) c = 0x41;
        }
    }
    return 0;
}