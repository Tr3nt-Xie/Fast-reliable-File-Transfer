#!/bin/bash
# Run on the client: confirm the active case really delivers the delay,
# loss and rate it claims. Never trust a qdisc you have not measured.
#   ./verify.sh <server_ip>
S=${1:-192.168.10.100}
echo "=== ping (200 packets) ==="
ping -i 0.2 -c 200 -q $S 2>/dev/null | tail -4
echo
echo "=== UDP throughput and loss (iperf3, 100 Mbit offered) ==="
iperf3 -u -c $S -b 100M -t 10 -f m 2>/dev/null | grep -E "receiver|Lost" | tail -2
echo
echo "=== TCP throughput (this is the baseline we have to beat) ==="
iperf3 -c $S -t 10 -f m 2>/dev/null | grep receiver
