#ifndef _GO_IPC_CHANNEL_H_
#define _GO_IPC_CHANNEL_H_

#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GO_IPC_CHANNEL_PATH_LENGTH 32
#define GO_IPC_CHANNEL_TOKEN_LENGTH 32

struct GoIPCChannel{
    //pass this token to the server when opening a unix socket to path 
    unsigned char token[GO_IPC_CHANNEL_TOKEN_LENGTH];
    // the path to open a unix socket to in order to speak to the process
    char path[GO_IPC_CHANNEL_PATH_LENGTH];
    // the stdout of the process (useful for single threaded communication with the process)
    int stdout;
    // the stdin of the process (useful for single threaded communication with the process)
    int stdin;
};

// This runs a go binary at argv[0] with the argv passed in and num_args
// This function starts the go binary and establishes a communication channel to it.
// The channel's file descriptors may be read + written with the standard UNIX read/write syscalls
struct GoIPCChannel launch_go_subprocess(const char *const argv[], size_t num_args);
// this is a simple helper function that reads from a file descriptor until size bytes are read
ssize_t read_until(int fd, void *buf, size_t size);
// this is a simple helper function that writes to a file descriptor until size bytes are written
ssize_t write_until(int fd, const void *buf, size_t size);
// this creates a new goroutine in the go program and returns a new pair of unix filedescriptors
// to call the new go program with
struct GoIPCChannel clone_go_channel(struct GoIPCChannel parent);

// When called upon the toplevel channel, returned from launch_go_subprocess, it terminates the
// go executable and all child channels.
// When called upon any child level channel, just that child goroutine is shut down.
void close_go_channel(struct GoIPCChannel *channel);
#ifdef __cplusplus
}
#endif
#endif
