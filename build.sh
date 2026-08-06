#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PHP_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET="$ROOT/target/x86_64-unknown-linux-gnu/release"

cd "$PHP_DIR"

echo "Building Rust cdylib (libvireon_php.so)..."
cargo build --release --target x86_64-unknown-linux-gnu -p vireon-php

echo
echo "Checking PHP availability..."
if ! command -v php &>/dev/null; then
    echo "WARNING: php not found in PATH. Install with:"
    echo "  sudo apt install php8.3-cli php8.3-common"
else
    PHP_VER=$(php -r 'echo PHP_VERSION;')
    echo "  PHP $PHP_VER found"
    if php -m 2>/dev/null | grep -q '^FFI$'; then
        echo "  FFI extension: loaded"
    else
        echo "  WARNING: FFI extension not loaded."
        echo "  Enable in php.ini: ffi.enable=true"
        echo "  Or run examples with: php -d ffi.enable=true example.php"
    fi
    if php -m 2>/dev/null | grep -q '^pcntl$'; then
        echo "  pcntl extension: loaded"
    else
        echo "  WARNING: pcntl extension not loaded (needed for fork-based drain)"
    fi
fi

echo
echo "Build complete."
echo "  Native:   $TARGET/libvireon_php.so"
echo "  Examples: $PHP_DIR/examples/ (PHP is interpreted, no compilation)"
echo
echo "Run with:"
echo "  LD_LIBRARY_PATH=$TARGET VIREON_ADDR=127.0.0.1:4433 \\"
echo "    php -d ffi.enable=true $PHP_DIR/examples/quickstart.php"
