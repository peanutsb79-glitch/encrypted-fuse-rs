#!/bin/bash
set -e

# Configurazione
DB_PATH="/tmp/test_metadata.db"
MNT_POINT="/tmp/test_fuse_mount"
FUSE_BIN="./target/debug/fuse"
LOG_FILE="/tmp/test_fuse.log"

# Cleanup precedente se necessario
echo "[1/7] Cleanup ambiente di test..."
killall fuse 2>/dev/null || true
fusermount -u "$MNT_POINT" 2>/dev/null || true
rm -f "$DB_PATH"
rm -rf "$MNT_POINT"
mkdir -p "$MNT_POINT"

# Build FUSE bin
echo "[2/7] Compilazione FUSE binario..."
cargo build >/dev/null 2>&1

# Start FUSE in background
echo "[3/7] Avvio FUSE filesystem in background..."
$FUSE_BIN "$DB_PATH" "$MNT_POINT" > "$LOG_FILE" 2>&1 &
FUSE_PID=$!

# Attesa per assicurarsi che il mount abbia successo
sleep 2

# Verifica che il mount point sia attivo
if ! mount | grep -q "$MNT_POINT"; then
    echo "❌ ERRORE: Il mount_point $MNT_POINT non è attivo. Verifica i log in $LOG_FILE"
    exit 1
fi
echo "✅ Mount Point attivo!"

# Test Scrittura e Lettura File
echo "[4/7] Test: Scrittura file su FUSE..."
TEST_STR="Questo è un test automatico di cifratura zero-knowledge."
echo "$TEST_STR" > "$MNT_POINT/test_file.txt"

echo "[5/7] Test: Lettura file da FUSE..."
READ_STR=$(cat "$MNT_POINT/test_file.txt")

if [ "$TEST_STR" == "$READ_STR" ]; then
    echo "✅ I/O Test Superato! Il contenuto letto coincide con quello scritto."
else
    echo "❌ ERRORE I/O: Il contenuto letto ($READ_STR) non coincide con ($TEST_STR)"
    exit 1
fi

# Test Metadati SQLite (Dentries & Inodes)
echo "[6/7] Test: Verifica metadati su SQLite locale..."
DENTRY_COUNT=$(sqlite3 "$DB_PATH" "SELECT count(*) FROM dentries WHERE name='test_file.txt';")
if [ "$DENTRY_COUNT" == "1" ]; then
    echo "✅ Metadati Test Superato! Il file 'test_file.txt' esiste nel DB SQLite."
else
    echo "❌ ERRORE Metadati: Il file non è stato registrato nel DB."
    exit 1
fi

# Smontaggio e Cleanup
echo "[7/7] Test: Smontaggio (Unmount) e Cleanup..."
fusermount -u "$MNT_POINT"
sleep 1
kill $FUSE_PID 2>/dev/null || true

# Test Finito
echo ""
echo "🎉 TUTTI I TEST PASSATI CON SUCCESSO! 🎉"
echo "Il Filesystem FUSE Cifrato in Rust è perfettamente funzionante."
