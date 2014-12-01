#ifndef _GO_CHANNEL_H_
#define _GO_CHANNEL_H_
#include <stddef.h>

#define GO_CHANNEL_PATH_LENGTH 32
#define GO_CHANNEL_TOKEN_LENGTH 32

struct GoChannel{
    //pass this token to the server when opening a unix socket to path 
    unsigned char token[GO_CHANNEL_TOKEN_LENGTH];
    // the path to open a unix socket to in order to speak to the process
    char path[GO_CHANNEL_PATH_LENGTH];
    // the stdout of the process (useful for single threaded communication with the process)
    int stdout;
    // the stdin of the process (useful for single threaded communication with the process)
    int stdin;
};


struct GoChannel launch_go_subprocess(const char* path_to_exe, char *const argv[]);
ptrdiff_t read_until(int fd, void *buf, int size);
ptrdiff_t write_until(int fd, void *buf, int size);
struct GoChannel clone_go_channel(struct GoChannel parent);
void close_go_channel(struct GoChannel *channel);

#endif
