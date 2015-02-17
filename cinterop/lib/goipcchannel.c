// +build none

#ifdef __STDC__
#define _GNU_SOURCE // needed for ssize_t and strdup
#include "goipcchannel.h"

#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

ssize_t read_until(int fd, void *buf, size_t size) {
    size_t progress = 0;
    while (progress < size) {
        ssize_t status = read(fd, (char*)buf + progress, size - progress);
        if (status == 0) { // EOF
            return 0;
        }
        if (status == -1) {
            if (errno != EINTR) {
                return -1;
            }
        } else {
            progress += status;
        }
    }
    return progress;
}

ssize_t write_until(int fd, const void *buf, size_t size) {
    size_t progress = 0;
    while (progress < size) {
        ssize_t status = write(fd, (char*)buf + progress, size - progress);
        if (status == -1) {
            if (status == 0) { // EOF
                return 0;
            }
            if (errno != EINTR) {
                return -1;
            }
        } else {
            progress += status;
        }
    }
    return progress;
}

// the header, as defined in server.go
#define GO_IPC_CHANNEL_HEADER ("58000000" "0100" "60c1" "00000000" "0000000\n")

// static assert that we have sufficient room in our struct
static char static_assert_sockaddr_un_has_sufficient_size[sizeof((struct sockaddr_un*)0)->sun_path
                                                          - GO_IPC_CHANNEL_PATH_LENGTH];

struct GoIPCChannel clone_go_channel(struct GoIPCChannel parent) {
    assert(sizeof(static_assert_sockaddr_un_has_sufficient_size) >= 0);
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

struct GoIPCChannel launch_go_subprocess(const char *const argv[], size_t num_args) {
    struct GoIPCChannel ret;
    int subprocess_stdin[2];
    int subprocess_stdout[2];
    int status = pipe(subprocess_stdin);
    assert(status == 0);
    status = pipe(subprocess_stdout);
    assert(status == 0);
    ret.stdin = subprocess_stdin[1];
    ret.stdout = subprocess_stdout[0];
    if (fork() == 0) {
        fclose(stdin);
        do {
            status = dup(subprocess_stdin[0]);
        } while (status == -1 && errno == EINTR);
        assert (status >= 0);
        close(subprocess_stdin[1]);
        fclose(stdout);
        do {
            status = dup(subprocess_stdout[1]);
        } while (status == -1 && errno == EINTR); // can only fail with retry on EINTR
        assert (status >= 0);
        close(subprocess_stdout[0]);
        char ** new_argv = (char**)malloc((num_args + 1) * sizeof(char*));
        size_t i;
        new_argv[num_args] = NULL;
        for (i = 0; i < num_args; ++i) {
            new_argv[i] = strdup(argv[i]);
        }
        execvp(argv[0], new_argv);
        assert(0 && "spawning subprocess failed");
        exit(0);
    } else {
        close(subprocess_stdin[0]);
        close(subprocess_stdout[1]);
        char header[] = GO_IPC_CHANNEL_HEADER;
        int header_status = read_until(ret.stdout, header, strlen(GO_IPC_CHANNEL_HEADER));
        assert(memcmp(header, GO_IPC_CHANNEL_HEADER, strlen(GO_IPC_CHANNEL_HEADER)) == 0);
        int path_status = read_until(ret.stdout, (unsigned char*)ret.path, sizeof(ret.path));
        ret.path[sizeof(ret.path) - 1] = '\0';
        int token_status = read_until(ret.stdout, ret.token, sizeof(ret.token));
        assert(header_status != -1);
        assert(path_status != -1);
        assert(token_status != -1);
    }
    return ret;
}

void close_go_channel(struct GoIPCChannel *channel) {
    if (channel->stdout != -1) {
        close(channel->stdout);
    }
    if (channel->stdin != channel->stdout && channel->stdin != -1) {
        // only close if stdout is not the same file descriptor as stdin
        // (they are the same for sockets, but different for pipes)
        close(channel->stdin);
    }
    channel->stdout = -1;
    channel->stdin = -1;
}
#endif
