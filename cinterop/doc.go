This software package is designed to help interop between legacy C programs and
go programs.
If a primarily C program needs to call a utility function that is only available
in go, this package can assist with interop. In essence this package lets you
call golang functions from C if the go functions take in simple byte arrays as
input.

The server class provides the ability for a c program to establish a "channel"
to the go program.
Each clone of the channel will run on a separate goroutine and keep everything
separate. In the raw interface, the go function needs to be able to process
input from an io.Reader and produce output on an io.Writer.

The example.go program illustrates the use of this interface when combined with
gochannel.c

The buffered_work.go class splits the input into 1 or more classes of size
workSize with a max buffer size of bufferSize.
This allows fixed sized structs to be processed efficicently and it takes care
of reading the input on struct sized boundaries and will never call the bound
function, taking a []byte with less than a full workSize elements.
This can be useful if your helper function operates on simple structs of raw
data.

The example_buffered.go program illustrates the use of this interface.
This interface also provides the opportunity to do some prefetching or
preloading of another command while output is being sent back to the C caller.
The prefetching function gets the last returned key and last data as input and
it is called as data is being written back to the forkexec caller.


The maximally_batched_work.go class waits until at least batch_size of an input
is full and delivers it all at once to the callee, so that the callee is
guaranteed to operate on a batch at a time.
This is useful for batching up large database operations where small batches
could lead to inefficiency.
example_batched.go shows how this can be utilized. example_batched.c verifies
that even if the buffer is flushed every 2 bytes, a full batch of (in this case)
8 bytes is required to invoke the function unless a full work item of zeros or
the end of batch are detected.


