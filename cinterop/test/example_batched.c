// +build none

#ifdef __STDC__
// go 6c compiler does not define __STDC__
#include "goipcchannel.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/types.h>


int main(int argc, char **argv) {
    const char * args[2]={argv[1], NULL};
    struct GoIPCChannel chan = launch_go_subprocess(args, 1), cloned_chan;
    char buf[9]={0};
    int i;
    for (i = 0; i < 4; ++i) {
        fd_set rfds;
        struct timeval tv;
        int retval;

        /* Watch stdin (fd 0) to see when it has input. */
        FD_ZERO(&rfds);
        FD_SET(chan.stdout, &rfds);

        /* Wait up to five seconds. */
        tv.tv_sec = 0;
        tv.tv_usec = 100000;

        retval = select(1, &rfds, NULL, NULL, &tv);
        assert(retval == 0);
        write(chan.stdin, "hi", 2);
    }
    for (i = 0; i < 4; ++i) {
        read_until(chan.stdout, buf + i * 2, 2);
    }
    printf("BUF: %s\n", buf);
    assert(memcmp(buf, "hihihihi", 8) == 0);
    for (i = 0; i < 2; ++i) {
        write(chan.stdin, i==0?"\0i": "i", 2);
        fd_set rfds;
        struct timeval tv;
        int retval;

        /* Watch stdin (fd 0) to see when it has input. */
        FD_ZERO(&rfds);
        FD_SET(chan.stdout, &rfds);

        /* Wait up to five seconds. */
        tv.tv_sec = 0;
        tv.tv_usec = 100000;

        retval = select(1, &rfds, NULL, NULL, &tv);
        assert(retval == 0);
    }
    write(chan.stdin, "\0\0", 2); // this flushes the batch resulting in 3 work items back
    printf("Partial BUF (hex encoded):");
    for (i = 0; i < 3; ++i) {
        read_until(chan.stdout, buf + i * 2, 2);
        printf("%02x ", buf[i*2]);
        printf("%02x ", buf[i*2+1]);
    }
    assert(memcmp(buf, "\0ii\0\0\0", 6) == 0);
    write(chan.stdin, "eeeeeeee", 8);
    read_until(chan.stdout, buf, 8);
    printf("\nBUF: %s\n", buf);
    assert(memcmp(buf, "eeeeeeee", 8) == 0);
    cloned_chan = clone_go_channel(chan);
    assert(cloned_chan.stdin > 0);
    write(cloned_chan.stdin, "bybybyby", 8);
    read_until(cloned_chan.stdout, buf, 8);
    assert(memcmp(buf, "bybybyby", 8) == 0);
    printf("BUF: %s\n", buf);
    close_go_channel(&cloned_chan);
    close_go_channel(&chan);
    return 0;
}
#endif
