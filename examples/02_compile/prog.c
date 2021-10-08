#include <stdio.h>

char *get_message();

int main(int argc, char** argv) {
    char *msg = get_message();
    printf("prog1: %s", msg);
}
