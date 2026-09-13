#!/bin/bash
# Try MTU 9000 end to end, with an automatic revert to 1500 after $HOLD s on
# every interface so that a broken path cannot lock us out. Run from the Mac.
#   ./mtu9000.sh [hold_seconds]
R="vyos@192.168.182.50"; SND=192.168.20.100; RCV=192.168.10.100
S="-o BatchMode=yes -o ConnectTimeout=15"
HOLD=${1:-90}
log(){ echo "[$(date +%H:%M:%S)] $*"; }

arm(){  # $1 = host spec ($R or -J $R ubuntu@host), $2 = interfaces
  ssh $S $1 "sudo sh -c 'nohup sh -c \"sleep $HOLD; for i in $2; do ip link set dev \$i mtu 1500; done\" >/dev/null 2>&1 &'; for i in $2; do sudo ip link set dev \$i mtu 9000; done; ip -o link | grep -E \"($(echo $2 | tr ' ' '|')):\" | awk '{print \$2, \$5}'"
}
log "setting MTU 9000 on router eth1/eth2 and both hosts (auto-revert in ${HOLD}s)"
arm "$R" "eth1 eth2"
arm "-J $R ubuntu@$SND" "ens160"
arm "-J $R ubuntu@$RCV" "ens160"
sleep 2

log "path MTU bisection from router (ping -M do, 3 packets each)"
for SZ in 1472 2000 4000 8000 8972; do
  for H in $SND $RCV; do
    OK=$(ssh $S $R "ping -M do -s $SZ -c 3 -W 1 $H 2>&1 | grep -oE '[0-9]+ received' | cut -d' ' -f1")
    echo "  router -> $H  payload $SZ: ${OK:-0}/3 received"
  done
done
log "host to host across the router"
for SZ in 1472 8972; do
  OK=$(ssh $S -J $R ubuntu@$SND "ping -M do -s $SZ -c 3 -W 1 $RCV 2>&1 | grep -oE '[0-9]+ received' | cut -d' ' -f1")
  echo "  $SND -> $RCV  payload $SZ: ${OK:-0}/3 received"
done

log "reverting to MTU 1500 now"
ssh $S $R "for i in eth1 eth2; do sudo ip link set dev \$i mtu 1500; done"
for H in $SND $RCV; do ssh $S -J $R ubuntu@$H "sudo ip link set dev ens160 mtu 1500; cat /sys/class/net/ens160/mtu"; done
