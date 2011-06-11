CFLAGS ?= -O0 -g -Wall
CFLAGS += -std=gnu99
CFLAGS += `pkg-config --cflags fuse`
LIBS = `pkg-config --libs fuse`

sgfs: sgfs.c
	gcc $(CFLAGS) $(LIBS) $(LDFLAGS) -o $@ $^

clean:
	rm -f sgfs

.PHONY: clean
