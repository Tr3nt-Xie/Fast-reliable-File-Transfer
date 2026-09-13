#!/bin/bash
# Benchmark one implementation across the assignment cases. Run from the Mac;
# drives router, sender and receiver over SSH and keeps every raw log.
#
#   ./bench.sh <impl: base|new> <file_on_sender> [dgram_bytes] [mbit] [cases] [tag]
#   ./bench.sh new data.bin 1472 90 "1 2 3" mtu1500
#
# base = ~/ft-base (teammate's origin/main), new = ~/ft-new (working tree);
# both are installed by deploy.sh. Wrap in `caffeinate -i` for long runs so
# the Mac does not sleep and drop the SSH sessions.
R="vyos@192.168.182.50"; SND=192.168.20.100; RCV=192.168.10.100
IMPL=${1:?impl}; FILE=${2:?file}; DGRAM=${3:-1472}; MBIT=${4:-90}
CASES=${5:-"1 2 3"}; TAG=${6:-$(date +%m%d-%H%M)}
DIR=~/Downloads/ee542-lab2/measure; RUNS=$DIR/../private/measure/runs/$TAG; mkdir -p "$RUNS"
OUT=$DIR/../private/measure/bench-results.csv
S="-o BatchMode=yes -o ConnectTimeout=20 -o ServerAliveInterval=15"
BIN="ft-$IMPL"

[ -f "$OUT" ] || echo "tag,impl,case,mtu,dgram,mbit,file_mb,oneway_s,oneway_mbps,srv_span_s,srv_mbps,retx,rounds,rcvbuf_err,md5_ok,skew_ms" > "$OUT"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$RUNS/bench.log"; }

# Clock hygiene: restart timesyncd on both hosts while the link is clean so
# the one-way timestamp (sender start vs receiver end) is meaningful.
ssh $S $R "./case.sh 0" >/dev/null 2>&1
for H in $SND $RCV; do ssh $S -J $R ubuntu@$H "sudo systemctl restart systemd-timesyncd" 2>/dev/null; done
sleep 3
skew(){  # host clock offsets vs the Mac, back to back
  local a b m
  m=$(python3 -c 'import time;print(int(time.time()*1000))')
  a=$(ssh $S -J $R ubuntu@$SND 'date +%s%3N'); b=$(ssh $S -J $R ubuntu@$RCV 'date +%s%3N')
  echo "$a $b $m"
}
read SA SB SM <<< "$(skew)"
log "clocks: sender-mac $((SA-SM)) ms, receiver-mac $((SB-SM)) ms (includes ~ssh latency)"

SRC_MD5=$(ssh $S -J $R ubuntu@$SND "md5sum ~/ft/$FILE | cut -d' ' -f1")
SIZE=$(ssh $S -J $R ubuntu@$SND "stat -c%s ~/ft/$FILE")
MTU=$(ssh $S -J $R ubuntu@$SND "cat /sys/class/net/ens160/mtu")
SIZE_MB=$((SIZE/1000000))
log "impl=$IMPL file=$FILE ${SIZE_MB}MB md5=${SRC_MD5:0:8} mtu=$MTU dgram=$DGRAM pace=${MBIT}Mbit cases=[$CASES]"

for C in $CASES; do
  log "=== Case $C ($IMPL) ==="
  ssh $S $R "./case.sh $C" >/dev/null 2>&1
  sleep 2
  SL="$RUNS/case$C-$IMPL.srv.log"; CL="$RUNS/case$C-$IMPL.cli.log"

  ssh $S -J $R ubuntu@$RCV 'pkill -x server 2>/dev/null; rm -f ~/recv.bin' 2>/dev/null
  ssh $S -J $R ubuntu@$RCV "cd ~/$BIN && timeout 2400 ./server 9000 ~/recv.bin 16777216" >"$SL" 2>&1 &
  SRVPID=$!
  sleep 3

  T0=$(date +%s)
  ssh $S -J $R ubuntu@$SND "cd ~/$BIN && ${CLIENT_ENV:-} timeout 2400 ./client $RCV 9000 ~/ft/$FILE $DGRAM $MBIT" >"$CL" 2>&1
  CRC=$?
  wait $SRVPID 2>/dev/null
  T1=$(date +%s)

  SEC=$(grep "one-way elapsed"    "$CL" | awk '{print $3}')
  MBPS=$(grep "payload throughput" "$CL" | awk '{print $3}')
  RETX=$(grep "retransmitted"      "$CL" | awk '{print $2}')
  RNDS=$(grep "feedback rounds"    "$CL" | awk '{print $3}')
  SPAN=$(grep "receive span"       "$SL" | awk '{print $3}')
  SMBPS=$(grep "receive goodput"   "$SL" | awk '{print $3}')
  RBE=$(grep "RcvbufErrors"        "$SL" | sed 's/.*RcvbufErrors +\([0-9-]*\).*/\1/')
  DST_MD5=$(ssh $S -J $R ubuntu@$RCV 'md5sum ~/recv.bin 2>/dev/null | cut -d" " -f1')
  OK=$([ -n "$SRC_MD5" ] && [ "$SRC_MD5" = "$DST_MD5" ] && echo yes || echo NO)
  SKEW=$([ -n "$SEC" ] && [ -n "$SPAN" ] && awk -v a="$SEC" -v b="$SPAN" 'BEGIN{printf "%d", (a-b)*1000}')

  log "  one-way ${SEC:-?}s/${MBPS:-?}Mbps | receiver ${SPAN:-?}s/${SMBPS:-?}Mbps | retx=${RETX:-?} rounds=${RNDS:-?} rcvbuf_err=${RBE:-?} md5=$OK wall=$((T1-T0))s client_rc=$CRC"
  # The two measurements must agree; a large gap means the VM clocks drifted
  # and the one-way figure cannot be trusted.
  [ -n "$SKEW" ] && [ "${SKEW#-}" -gt 2000 ] && log "  !! one-way and receiver span differ by ${SKEW} ms - clock skew, use receiver span"
  echo "$TAG,$IMPL,$C,$MTU,$DGRAM,$MBIT,$SIZE_MB,${SEC:-},${MBPS:-},${SPAN:-},${SMBPS:-},${RETX:-},${RNDS:-},${RBE:-},$OK,${SKEW:-}" >> "$OUT"
done

ssh $S $R "./case.sh 0" >/dev/null 2>&1
log "=== done -> $OUT (raw logs in $RUNS) ==="
