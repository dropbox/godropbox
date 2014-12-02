#include "goipcchannel.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>


int main(int argc, char **argv) {
    char * args[2]={argv[1], NULL};
    struct GoIPCChannel chan = launch_go_subprocess(argv[1], args), cloned_chan;
    char buf[3]={0};
    write(chan.stdin, "hi", 2);
    read_until(chan.stdout, buf, 2);
    printf("BUF: %s\n", buf);
    assert(memcmp(buf, "hi", 2) == 0);
    write(chan.stdin, "ee", 2);
    read_until(chan.stdout, buf, 2);
    printf("BUF: %s\n", buf);
    assert(memcmp(buf, "ee", 2) == 0);
    cloned_chan = clone_go_channel(chan);
    assert(cloned_chan.stdin > 0);
    write(cloned_chan.stdin, "by", 2);
    read_until(cloned_chan.stdout, buf, 2);
    printf("BUF: %s\n", buf);
    assert(memcmp(buf, "by", 2) == 0);
    close_go_channel(&cloned_chan);
    close_go_channel(&chan);
    return 0;
}
