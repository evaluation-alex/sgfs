CFLAGS ?= -O2 -g -Wall
CFLAGS += -std=gnu99
CFLAGS += `pkg-config --cflags fuse`
LIBS = `pkg-config --libs fuse`

all: sgfs sgfs_ll

sgfs: sgfs.c
	gcc $(CFLAGS) $(LIBS) $(LDFLAGS) -o $@ $^

sgfs_ll: sgfs_ll.c
	gcc $(CFLAGS) $(LIBS) $(LDFLAGS) -o $@ $^

clean:
	rm -f sgfs sgfs_ll

.PHONY: clean
