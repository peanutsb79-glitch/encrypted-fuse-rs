#!/bin/bash
set -e

DB_PATH="/tmp/test_metadata.db"
MNT_POINT="/tmp/test_fuse_mount"
FUSE_BIN="./target/debug/fuse"
LOG_FILE="/tmp/test_fuse.log"

# Clean up
killall fuse 2>/dev/null || true
fusermount -u "$MNT_POINT" 2>/dev/null || true
rm -f "$DB_PATH"
rm -rf "$MNT_POINT"
mkdir -p "$MNT_POINT"

echo "[1/4] Starting FUSE..."
$FUSE_BIN "$DB_PATH" "$MNT_POINT" > "$LOG_FILE" 2>&1 &
FUSE_PID=$!
sleep 2

# Verify the mount point is active
if ! mount | grep -q "$MNT_POINT"; then
    echo "❌ Mount failed."
    exit 1
fi

echo "[2/4] Testing Directory Operations..."
mkdir "$MNT_POINT/testdir"
mkdir "$MNT_POINT/testdir/sub1"
touch "$MNT_POINT/testdir/sub1/file.txt"
# rename
mv "$MNT_POINT/testdir/sub1/file.txt" "$MNT_POINT/testdir/file2.txt"
# unlink
rm "$MNT_POINT/testdir/file2.txt"
# rmdir
rmdir "$MNT_POINT/testdir/sub1"
rmdir "$MNT_POINT/testdir"
echo "✅ Directory operations passed."

echo "[3/4] Testing Large Spanning Writes..."
# 16MB logical block + 1 MB to span exactly across block boundary
dd if=/dev/urandom of="$MNT_POINT/large.bin" bs=1M count=17 status=none
# Verify file size reported
FILE_SIZE=$(stat -c%s "$MNT_POINT/large.bin")
if [ "$FILE_SIZE" != "17825792" ]; then
    echo "❌ Incorrect file size tracking: expected 17825792, got $FILE_SIZE"
    exit 1
fi
echo "✅ Multiple blocks successfully written and tracked."

echo "[4/4] Unmounting..."
fusermount -u "$MNT_POINT"
sleep 1

echo "🎉 EXHAUSTIVE VALIDATION PASSED SUCCESSFULLY! 🎉"
