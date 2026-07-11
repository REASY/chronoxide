#define _POSIX_C_SOURCE 200112L

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int failed = 0;

    for (int i = 1; i < argc; i++) {
        int fd = open(argv[i], O_RDONLY);
        if (fd < 0) {
            fprintf(stderr, "%s: %s\n", argv[i], strerror(errno));
            failed = 1;
            continue;
        }

        int error = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        if (error != 0) {
            fprintf(stderr, "%s: %s\n", argv[i], strerror(error));
            failed = 1;
        }
        close(fd);
    }

    return failed;
}
