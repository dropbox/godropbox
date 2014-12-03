all:
	cd test && go build unbuffered/example.go; cd ..
	cd test && go build buffered/example_buffered.go; cd ..
	cd test && go build batched/example_batched.go; cd ..
	cd test && gcc -Wall -std=c99 -o forkexec example.c -I../lib ../lib/goipcchannel.c; cd ..
	cd test && gcc -Wall -std=c99 -o example_batched_exec example_batched.c -I../lib ../lib/goipcchannel.c; cd ..
	cd ..

valgrind:
	valgrind test/forkexec test/example_buffered
	valgrind test/forkexec test/example
	echo "hello" | test/example_buffered
	valgrind test/example_batched_exec test/example_batched

unittest:
	test/forkexec test/example_buffered
	test/forkexec test/example
	echo "hello" | test/example_buffered
	test/example_batched_exec test/example_batched
clean:
	rm test/forkexec test/example test/example_buffered test/example_batched test/example_batched_exec