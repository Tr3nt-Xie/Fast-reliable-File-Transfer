# Fast Reliable File Transfer

EE542 Lab 2 reliable UDP file-transfer program with NACK-based selective
retransmission.

## Setup on Both Ubuntu VMs

First download:

```bash
cd ~
git clone https://github.com/Tr3nt-Xie/Fast-reliable-File-Transfer.git
```

For later updates:

```bash
cd ~/Fast-reliable-File-Transfer
git pull --ff-only origin main
```

Build:

```bash
cd ~/Fast-reliable-File-Transfer/src
make clean
make
```

## Run

Receiver on `ubuntu-client` (`192.168.20.100`):

```bash
cd ~/Fast-reliable-File-Transfer/src
./server 9000 ~/lab2-received.bin 4194304
```

Sender on `ubuntu-server` (`192.168.10.100`):

```bash
cd ~/Fast-reliable-File-Transfer/src
./client 192.168.20.100 9000 ~/lab2-data.bin 1472 90
```

Datagram size: `1472` for MTU 1500, `8972` for MTU 9000, or `8973` for MTU
9001. The final argument is the send rate in Mbit/s, or `auto` (optionally
`auto:<max>`) to let the sender find the link rate itself. On this testbed
the measured knees are 98 for the 100 Mbit cases and 86 for Case 3:

```bash
./client 192.168.20.100 9000 ~/lab2-data.bin 1472 98     # Case 1, Case 2
./client 192.168.20.100 9000 ~/lab2-data.bin 1472 86     # Case 3
./client 192.168.20.100 9000 ~/lab2-data.bin 1472 auto   # any case, no tuning
```

Verify after completion:

```bash
# ubuntu-server
md5sum ~/lab2-data.bin

# ubuntu-client
md5sum ~/lab2-received.bin
```

Protocol: `META -> READY -> DATA (with FIN/NACK feedback interleaved) -> DONE -> DONE_ACK`.

The sender streams the file once at the paced rate. Every RTT it sends a FIN
(report request); the receiver answers with what is still missing below the
highest sequence it has seen, as a list or a bitmap, whichever is smaller.
Repairs are sent inside the same paced stream, so the link never idles.
After the first pass the FIN carries ALL_SENT and the report covers the
whole file; small pending sets are sent with redundant copies so that the
last round trip is usually the last one.

Set `FT_TRACE=1` in the sender's environment to print one line per FIN and
per report on stderr.

## Measuring

```bash
./measure/deploy.sh                       # install ~/ft-base (origin/main) and ~/ft-new (working tree) on both VMs
caffeinate -i ./measure/bench.sh new data.bin 1472 90 "1 2 3" mytag
./measure/bench.sh base data.bin 1472 90 "2" mytag
```

Results append to `measure/bench-results.csv`; raw sender and receiver logs
are kept in `measure/runs/<tag>/`. Synchronize both VM clocks before
measuring one-way transfer time (the harness restarts `systemd-timesyncd` on
both hosts and cross-checks the one-way figure against the receiver's own
span).
