CC     = cc
CFLAGS = -O2 -Wall -Wextra
BINS   = client server

all: $(BINS)
client: client.c proto.h
	$(CC) $(CFLAGS) -o $@ client.c -lm
server: server.c proto.h
	$(CC) $(CFLAGS) -o $@ server.c
clean:
	rm -f $(BINS)
.PHONY: all clean
