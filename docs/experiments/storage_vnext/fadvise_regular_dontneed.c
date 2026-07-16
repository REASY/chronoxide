#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int failed = 0;

    for (int i = 1; i < argc; i++) {
        int fd = open(argv[i], O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (fd < 0) {
            fprintf(stderr, "%s: open: %s\n", argv[i], strerror(errno));
            failed = 1;
            continue;
        }

        struct stat metadata;
        if (fstat(fd, &metadata) != 0) {
            fprintf(stderr, "%s: fstat: %s\n", argv[i], strerror(errno));
            failed = 1;
            close(fd);
            continue;
        }
        if (!S_ISREG(metadata.st_mode)) {
            fprintf(stderr, "%s: not a regular file\n", argv[i]);
            failed = 1;
            close(fd);
            continue;
        }

        int error = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        if (error != 0) {
            fprintf(stderr, "%s: posix_fadvise: %s\n", argv[i], strerror(error));
            failed = 1;
        }
        if (close(fd) != 0) {
            fprintf(stderr, "%s: close: %s\n", argv[i], strerror(errno));
            failed = 1;
        }
    }

    return failed;
}
