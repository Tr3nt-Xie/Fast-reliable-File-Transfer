#!/bin/bash
# Run on the VyOS router: switch between the three assignment cases.
#
#   ./case.sh 0      clear all shaping
#   ./case.sh 1      RTT 10ms,  1% loss,  100 Mbit
#   ./case.sh 2      RTT 200ms, 20% loss, 100 Mbit
#   ./case.sh 3      RTT 200ms, no loss,  80 Mbit
#   ./case.sh show   print the active configuration
set -e
IFACES="eth1 eth2"          # eth1 = server side, eth2 = client side

clear_all() { for i in $IFACES; do sudo tc qdisc del dev $i root 2>/dev/null || true; done; }

# tbf is the root qdisc (handle 1:0) with netem nested under it (parent 1:1).
# The Lab 1 handout claimed the two cannot coexist; that is only true of two
# *root* qdiscs. Nesting is required to combine a rate limit with delay/loss.
shape() {  # $1 = rate mbit   $2 = one-way delay ms   $3 = loss % (may be empty)
  for i in $IFACES; do
    sudo tc qdisc add dev $i root handle 1:0 tbf rate ${1}mbit latency 0.001ms burst 9015
    if [ -n "$3" ]; then
      sudo tc qdisc add dev $i parent 1:1 handle 10: netem delay ${2}ms loss ${3}%
    else
      sudo tc qdisc add dev $i parent 1:1 handle 10: netem delay ${2}ms
    fi
  done
}

case "$1" in
  0) clear_all; echo "cleared all shaping" ;;
  1) clear_all; shape 100 5   1  ; echo "Case 1: RTT 10ms  / loss 1%  / 100 Mbit" ;;
  2) clear_all; shape 100 100 20 ; echo "Case 2: RTT 200ms / loss 20% / 100 Mbit" ;;
  3) clear_all; shape 80  100 ""  ; echo "Case 3: RTT 200ms / no loss  / 80 Mbit" ;;
  show) : ;;
  *) echo "usage: $0 {0|1|2|3|show}"; exit 1 ;;
esac

echo "--- active qdiscs ---"
for i in $IFACES; do echo "[$i]"; sudo tc qdisc show dev $i | sed 's/^/  /'; done
