#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "gochannel.h"

ptrdiff_t read_until(int fd, void *buf, int size) {
    size_t progress = 0;
    while (progress < size) {
        ptrdiff_t status = read(fd, (char*)buf + progress, size - progress);
        if (status == -1) {
            return -1;
        }
        progress += status;
    }
    return progress;
}

ptrdiff_t write_until(int fd, void *buf, int size) {
    size_t progress = 0;
    while (progress < size) {
        ptrdiff_t status = write(fd, (char*)buf + progress, size - progress);
        if (status == -1) {
            return -1;
        }
        progress += status;
    }
    return progress;
}


// static assert that we have sufficient room in our struct
static char static_assert[sizeof((struct sockaddr_un*)0)->sun_path - GO_CHANNEL_PATH_LENGTH];

struct GoChannel clone_go_channel(struct GoChannel parent) {
    parent.stdout = -1;
    parent.stdin = -1;

    struct sockaddr_un address;
    memset(&address, 0, sizeof(struct sockaddr_un));
    int socket_fd;
    socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
    if(socket_fd < 0) {
        return parent;
    }
    address.sun_family = AF_UNIX;
    memcpy(address.sun_path, parent.path, sizeof(parent.path));
    if (connect(socket_fd, (struct sockaddr *)&address, sizeof(struct sockaddr_un)) != 0) {
        return parent;
    }
    write_until(socket_fd, parent.token, sizeof(parent.token));
    parent.stdout = socket_fd;
    parent.stdin = socket_fd;
    return parent;
}
struct GoChannel launch_go_subprocess(const char* path_to_exe, char *const argv[]) {
    struct GoChannel ret;
    int subprocess_stdin[2];
    int subprocess_stdout[2];
    pipe(subprocess_stdin);
    pipe(subprocess_stdout);
    ret.stdin = subprocess_stdin[1];
    ret.stdout = subprocess_stdout[0];
    if (fork() == 0) {
        fclose(stdin);
        dup(subprocess_stdin[0]);
        close(subprocess_stdin[1]);
        fclose(stdout);
        dup(subprocess_stdout[1]);
        close(subprocess_stdout[0]);
        execvp(path_to_exe, argv);
    }else {
        close(subprocess_stdin[0]);
        close(subprocess_stdout[1]);
        int path_status = read_until(ret.stdout, (unsigned char*)ret.path, sizeof(ret.path));
        ret.path[sizeof(ret.path) - 1] = '\0';
        int token_status = read_until(ret.stdout, ret.token, sizeof(ret.token));
        assert(path_status != -1);
        assert(token_status != -1);
    }
    return ret;
}

void close_go_channel(struct GoChannel *channel) {
    if (channel->stdout != -1) {
        close(channel->stdout);
    }
    if (channel->stdin != -1) {
        close(channel->stdin);
    }
    channel->stdout = -1;
    channel->stdin = -1;
}
