# Vireon PHP SDK

PHP binding for the Vireon QUIC-native pub/sub runtime, via FFI (Foreign
Function Interface) calling into a Rust C ABI cdylib.

PHP FFI (available in PHP 7.4+, mature in 8.x) loads `libvireon_php.so` at
runtime via `FFI::cdef()` and calls the `vireon_*` functions directly. A
single-file PHP wrapper class provides OOP semantics: RAII via `__destruct()`,
exceptions on error, fluent builder.

## Architecture

| Layer | Description |
|---|---|
| **Rust cdylib** (`libvireon_php.so`) | `extern "C"` functions with `vireon_` prefix. Global tokio runtime. `block_on()` bridges async→sync. |
| **PHP wrapper** (`php/VireonSdk.php`) | OOP classes wrapping all 34 FFI functions. RAII via `__destruct()`. Constants for TLS modes and delivery policies. Throws `VireonException` on error. |

## Prerequisites

- Rust 1.85+ (workspace pins `x86_64-unknown-linux-gnu`)
- PHP 8.1+ with FFI + pcntl extensions (`sudo apt install php8.3-cli php8.3-common`)

## Build

```bash
bash build.sh
```

This produces:
- PHP examples are interpreted — no compilation step

## Quickstart

```php
<?php
require 'path/to/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;

$sub = (new VireonClientBuilder('127.0.0.1:4433'))
    ->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)
    ->connect();

$pub = (new VireonClientBuilder('127.0.0.1:4433'))
    ->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)
    ->connect();

$s = $sub->subscribe('sensor.*');
usleep(200000);

$pub->publish('sensor.temp', '42C');
$msg = $s->recv();
echo "{$msg->topic}: {$msg->payload}\n";
```

## Fork-based concurrency

PHP is single-threaded and `recv()` blocks indefinitely. The C ABI's
`block_on(recv)` cannot be interrupted. Examples use `pcntl_fork` to create
drain child processes:

1. **Fork first** — before `vireon_init()`, so each process gets its own tokio runtime
2. **Child process**: connects as subscriber, drains messages, writes count to temp file
3. **Parent process**: connects as publisher, publishes, polls temp file for count
4. **Cleanup**: parent kills child via `posix_kill(SIGTERM)`

## Examples

| Example | Description |
|---|---|
| **quickstart** | Basic pub/sub + all delivery policies (no fork) |
| **ordering** | Verify in-order delivery (500 frames, 1 drain child) |
| **consumer_group** | 3-member round-robin distribution (3 drain children) |
| **hol_congestion** | Head-of-line blocking isolation (5 drain children) |
| **pool_multiplex** | 4-connection pool, 1000 messages (1 drain child) |
| **bench** | Throughput benchmark (stream/broadcast/group, 1 drain child) |

### Run an example

```bash

# Run Quickstart
VIREON_ADDR=127.0.0.1:4433 \
php -d ffi.enable=true bindings/php/examples/quickstart.php
```

> **Note:** `-d ffi.enable=true` is required if FFI is not enabled in `php.ini`.

## API

### VireonClient

| Method | Description |
|---|---|
| `publish(topic, payload)` | Publish on default channel |
| `tryPublish(topic, payload)` | Fire-and-forget publish |
| `subscribe(pattern)` | Subscribe to topic pattern |
| `unsubscribe(pattern)` | Remove subscription |
| `openStream(policy, topic?)` | Open dedicated stream |
| `subscribeGroup(topic, group, consumer)` | Join consumer group |
| `leaveGroup(topic, group, consumer)` | Leave consumer group |
| `rpc(reqTopic, payload, replyTopic, timeout)` | Request/reply RPC |
| `migrate(bindAddr)` | Trigger connection migration |
| `pendingBytes()` | Transport buffered bytes |

### VireonClientBuilder

Fluent builder: `tlsVerify`, `sni`, `clientIdentity`, `reconnect`,
`maxMessageSize`, `subscriberBuffer`, `cmdChannelCap`, `maxIdleTimeout`.
Terminal: `connect()` / `connectPool(n)`.

### Constants

| Class | Values |
|---|---|
| `VireonDeliveryPolicy` | `ReliableOrdered=0`, `ReliableUnordered=1`, `RealtimeDropOld=2`, `LatestOnly=3` |
| `VireonTlsVerify` | `Tofu=0`, `DangerAcceptInvalid=1`, `Strict=2`, `Pinned=3` |

## Server no-echo design

The server filters `conn_idx` on fan-out — a client never receives its own
publishes. Tests and examples use **two connections** (sub + pub).

## FFI recv() blocking note

The C ABI `block_on(recv)` blocks the calling thread indefinitely. Calling
`close()` on a handle while another process is blocked in `recv()` is
use-after-free. Fork-based examples use `posix_kill(SIGTERM)` to terminate
drain children rather than attempting clean shutdown.

## Fork-based publish: use `publish()` not `tryPublish()`

In a fork-based pattern, the publisher's tokio runtime only runs during FFI
calls (`block_on`). `tryPublish()` completes instantly without yielding to
the runtime, so incoming QUIC flow control updates (MAX_DATA frames) are
never processed — the publisher stalls after exhausting the initial window.

Use `publish()` (blocking) in fork-based bench/throughput scenarios. It blocks
on flow control, naturally processing window updates during the await.
