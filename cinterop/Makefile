all:
	cd test && go build example.go; cd ..
	cd test && go build example_buffered.go; cd ..
	cd test && go build example_batched.go; cd ..
	cd test && gcc -o forkexec -std=c99 example.c -I../lib ../lib/goipcchannel.c; cd ..
	cd test && gcc -o example_batched_exec -std=c99 example_batched.c -I../lib ../lib/goipcchannel.c; cd ..
	cd ..

unittest:
	test/forkexec test/example_buffered
	test/forkexec test/example
	echo "hello" | test/example_buffered
	test/example_batched_exec test/example_batched
clean:
	rm test/forkexec test/example test/example_buffered test/example_batched test/example_batched_exec