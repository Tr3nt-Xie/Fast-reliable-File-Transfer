#!/bin/bash
# Run on server / client: cap egress at 100 Mbit as the assignment requires.
#   ./host-limit.sh on | off | show
IF=$(ip -o -4 route show default | awk '{print $5}' | head -1)
[ -z "$IF" ] && IF=ens160
case "$1" in
  on)  sudo tc qdisc del dev $IF root 2>/dev/null
       sudo tc qdisc add dev $IF root tbf rate 100mbit latency 0.001ms burst 9015
       echo "$IF egress capped at 100 Mbit" ;;
  off) sudo tc qdisc del dev $IF root 2>/dev/null; echo "$IF shaping cleared" ;;
  show) ;;
  *) echo "usage: $0 {on|off|show}"; exit 1 ;;
esac
echo "--- $IF ---"; tc qdisc show dev $IF | sed 's/^/  /'
