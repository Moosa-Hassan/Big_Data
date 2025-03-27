

#include <malloc.h>
#include <stdio.h>

#include "../../compress-c.h"

int main() {
    FILE* fp = NULL;
    char* data;

    fp = fopen("/tbase-project/pbc/pattern", "r");
    if (fp == NULL) {
        printf("file empty.");
    }
    fseek(fp, 0, SEEK_END);
    int length = ftell(fp);
    data = reinterpret_cast<char*>(malloc((length + 1) * sizeof(char)));
    rewind(fp);
    length = fread(data, 1, length, fp);
    data[length] = '\0';
    fclose(fp);

    void* pbc = new_PBC();

    setPattern(pbc, data, length);

    printf("pattern num: %d", get_Pattern_num(pbc));
}
