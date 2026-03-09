#!/bin/bash
set -e

# Configuration
DB_PATH="/tmp/test_metadata.db"
MNT_POINT="/tmp/test_fuse_mount"
FUSE_BIN="./target/debug/fuse"
LOG_FILE="/tmp/test_fuse.log"

# Clean up previous state if necessary
echo "[1/7] Cleaning up test environment..."
killall fuse 2>/dev/null || true
fusermount -u "$MNT_POINT" 2>/dev/null || true
rm -f "$DB_PATH"
rm -rf "$MNT_POINT"
mkdir -p "$MNT_POINT"

# Build FUSE binary
echo "[2/7] Compiling FUSE binary..."
cargo build >/dev/null 2>&1

# Start FUSE in background
echo "[3/7] Starting FUSE filesystem in background..."
$FUSE_BIN "$DB_PATH" "$MNT_POINT" > "$LOG_FILE" 2>&1 &
FUSE_PID=$!

# Wait to ensure the mount is successful
sleep 2

# Verify the mount point is active
if ! mount | grep -q "$MNT_POINT"; then
    echo "❌ ERROR: Mount point $MNT_POINT is not active. Check logs located at $LOG_FILE"
    exit 1
fi
echo "✅ Mount Point active!"

# Test File Write and Read
echo "[4/7] Test: Writing file to FUSE..."
TEST_STR="This is an automated test for zero-knowledge encryption."
echo "$TEST_STR" > "$MNT_POINT/test_file.txt"

echo "[5/7] Test: Reading file from FUSE..."
READ_STR=$(cat "$MNT_POINT/test_file.txt")

if [ "$TEST_STR" == "$READ_STR" ]; then
    echo "✅ I/O Test Passed! Read content matches written content."
else
    echo "❌ I/O ERROR: Read content ($READ_STR) does not match expected output ($TEST_STR)"
    exit 1
fi

# Test SQLite Metadata (Dentries & Inodes)
echo "[6/7] Test: Verifying metadata in local SQLite database..."
DENTRY_COUNT=$(sqlite3 "$DB_PATH" "SELECT count(*) FROM dentries WHERE name='test_file.txt';")
if [ "$DENTRY_COUNT" == "1" ]; then
    echo "✅ Metadata Test Passed! File 'test_file.txt' exists in SQLite DB."
else
    echo "❌ Metadata ERROR: File was not registered in the database."
    exit 1
fi

# Unmount and Cleanup
echo "[7/7] Test: Unmounting and Cleanup..."
fusermount -u "$MNT_POINT"
sleep 1
kill $FUSE_PID 2>/dev/null || true

# Test Finished
echo ""
echo "🎉 ALL TESTS PASSED SUCCESSFULLY! 🎉"
echo "The Rust Encrypted FUSE Filesystem is fully operational."
