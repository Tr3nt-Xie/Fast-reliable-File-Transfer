#!/bin/bash
# Deploy the teammate baseline (origin/main) and the working tree to both VMs
# as ~/ft-base and ~/ft-new, then build. Run from the Mac.
set -e
cd "$(dirname "$0")/.."
J="vyos@192.168.182.50"
STAGE=$(mktemp -d)
mkdir -p "$STAGE/base" "$STAGE/new"
git archive origin/main src | tar -x -C "$STAGE/base" --strip-components=1
cp src/*.c src/*.h src/Makefile "$STAGE/new/"
for H in 192.168.10.100 192.168.20.100; do
  echo "=== $H ==="
  COPYFILE_DISABLE=1 tar --no-xattrs --no-mac-metadata -C "$STAGE" -cf - base new | ssh -o BatchMode=yes -o ConnectTimeout=15 -J "$J" ubuntu@$H '
    rm -rf ~/ft-base ~/ft-new && mkdir -p ~/ft-base ~/ft-new && tar -xf - 2>/dev/null;
    mv base/* ~/ft-base/ && mv new/* ~/ft-new/ && rmdir base new &&
    for d in ft-base ft-new; do (cd ~/$d && make 2>&1 | grep -E "error|warning" || true); done;
    ls -la ~/ft-base/client ~/ft-new/client ~/ft-base/server ~/ft-new/server | awk "{print \$5, \$9}"'
done
rm -rf "$STAGE"
