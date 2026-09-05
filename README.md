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
9001. The final argument is the target rate in Mbit/s.

Verify after completion:

```bash
# ubuntu-server
md5sum ~/lab2-data.bin

# ubuntu-client
md5sum ~/lab2-received.bin
```

Protocol: `META -> READY -> DATA -> FIN -> NACK/retransmit or DONE`.

The sender acknowledges DONE with DONE_ACK to finish the transfer. Synchronize
both VM clocks before measuring one-way transfer time.
