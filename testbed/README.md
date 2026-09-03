# Test network

Three VMs, reusing the Lab 1 topology.

| Role | Address | Interfaces |
|---|---|---|
| Router (VyOS) | 192.168.182.50 | eth1 -> server, eth2 -> client |
| Server | 192.168.10.100 | ens160 |
| Client | 192.168.20.100 | ens160 |

```bash
ssh vyos@192.168.182.50
ssh -J vyos@192.168.182.50 ubuntu@192.168.10.100
ssh -J vyos@192.168.182.50 ubuntu@192.168.20.100
```

## Scripts

```bash
# router
./case.sh 2        # switch to Case 2
./case.sh 0        # clear all shaping
./case.sh show     # show what is currently applied

# server / client
./host-limit.sh on              # 100 Mbit egress cap
./verify.sh 192.168.10.100      # measure delay / loss / throughput
```

**Run `./case.sh show` before every measurement.** A leftover qdisc from a
previous case will silently contaminate the next set of numbers.

## The three cases

All use nested shaping: tbf as the root qdisc (`handle 1:0`) with netem
attached below it (`parent 1:1`).

| Case | Rate | One-way delay | Loss |
|---|---|---|---|
| 1 | 100 Mbit | 5 ms | 1% |
| 2 | 100 Mbit | 100 ms | 20% |
| 3 | 80 Mbit | 100 ms | none |

Loss is applied on **both** interfaces, so it is bidirectional: a round trip
has to survive two independent draws. At 20% per direction a ping succeeds
only 0.8 x 0.8 = 64% of the time, which is why `ping` reports ~36-39% loss
rather than 20%. Our ACK/NACK return path is subject to the same thing.

## Verifying

```bash
ping -i 0.2 -c 200 <dst>          # delay and loss
iperf3 -u -c <dst> -b 100M        # UDP throughput and datagram loss
iperf3 -c <dst>                   # TCP throughput
```
