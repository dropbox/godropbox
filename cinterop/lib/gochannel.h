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

// This runs a go binary at path_to_exe with the argv terminated with NULL, just like execvp
// This function starts the go binary and establishes a communication channel to it.
// The channel's file descriptors may be read + written with the standard UNIX read/write syscalls
struct GoChannel launch_go_subprocess(const char* path_to_exe, char *const argv[]);
// this is a simple helper function that reads from a file descriptor until size bytes are read
ptrdiff_t read_until(int fd, void *buf, int size);
// this is a simple helper function that writes to a file descriptor until size bytes are written
ptrdiff_t write_until(int fd, void *buf, int size);
// this creates a new goroutine in the go program and returns a new pair of unix filedescriptors
// to call the new go program with
struct GoChannel clone_go_channel(struct GoChannel parent);

// When called upon the toplevel channel, returned from launch_go_subprocess, it terminates the
// go executable and all child channels.
// When called upon any child level channel, just that child goroutine is shut down.
void close_go_channel(struct GoChannel *channel);

#endif
