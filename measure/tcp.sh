#!/bin/bash
# TCP reference for each case with iperf (20 s, MSS default). Run from the Mac.
#   ./tcp.sh "1 2 3"
R="vyos@192.168.182.50"; SND=192.168.20.100; RCV=192.168.10.100
S="-o BatchMode=yes -o ConnectTimeout=20"
OUT=~/Downloads/ee542-lab2/private/measure/tcp-results.txt
for C in ${1:-1 2 3}; do
  ssh $S $R "./case.sh $C" >/dev/null 2>&1; sleep 2
  ssh $S -J $R ubuntu@$RCV 'pkill -x iperf 2>/dev/null; nohup iperf -s >/dev/null 2>&1 &' 2>/dev/null; sleep 2
  LINE=$(ssh $S -J $R ubuntu@$SND "iperf -c $RCV -t 20 -f m" 2>&1 | grep -E "Mbits/sec|Kbits/sec|bits/sec" | tail -1)
  ssh $S -J $R ubuntu@$RCV 'pkill -x iperf' 2>/dev/null
  echo "case $C: $LINE" | tee -a "$OUT"
done
ssh $S $R "./case.sh 0" >/dev/null 2>&1
