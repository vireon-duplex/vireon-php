<?php
// Bench — throughput benchmark with three modes
// Fork-based: 1 drain child + parent publisher.

require __DIR__ . '/../php/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;
use Vireon\VireonDeliveryPolicy;

$addr = getenv('VIREON_ADDR') ?: '127.0.0.1:4433';
$mode = $argv[1] ?? 'stream';
$size = (int)($argv[2] ?? 1024);
$count = (int)($argv[3] ?? 5000);
const BATCH_SIZE = 256;

printf("bench: mode=%s size=%dB count=%d\n", $mode, $size, $count);

$tmpfile = tempnam(sys_get_temp_dir(), 'vireon_bench');
$pid = pcntl_fork();
if ($pid < 0) { fwrite(STDERR, "fork failed\n"); exit(1); }

if ($pid === 0) {
    // ── Child: drain ──
    $sub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->maxIdleTimeout(120.0)->connect();

    $recvHandle = null;
    $recvFn = null;

    if ($mode === 'stream') {
        $recvHandle = $sub->openStream(VireonDeliveryPolicy::ReliableOrdered, 'bench.stream');
    } elseif ($mode === 'broadcast') {
        $recvHandle = $sub->subscribe('bench.broadcast');
    } elseif ($mode === 'group') {
        $recvHandle = $sub->subscribeGroup('bench.group', 'workers', 'c0');
    } else {
        fwrite(STDERR, "Unknown mode: $mode\n"); exit(1);
    }

    usleep(300000);
    file_put_contents($tmpfile, 'ready');

    $received = 0;
    while ($received < $count) {
        $batch = $recvHandle->recvBatch(BATCH_SIZE);
        if (empty($batch)) break;
        $received += count($batch);
        if ($received % 200 === 0) file_put_contents($tmpfile, (string)$received);
    }
    file_put_contents($tmpfile, (string)$received);
    exit(0);
}

// ── Parent: wait for ready, publish ──
while (trim(file_get_contents($tmpfile)) !== 'ready') usleep(10000);

$pub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->maxIdleTimeout(120.0)->connect();
$payload = '';
for ($i = 0; $i < $size; $i++) $payload .= chr($i & 0xFF);

$benchTopic = $mode === 'stream' ? 'bench.stream'
    : ($mode === 'broadcast' ? 'bench.broadcast' : 'bench.group');

$start = hrtime(true);

// Use publish() (blocking) instead of tryPublish() — in a fork-based pattern,
// the publisher's tokio runtime only runs during FFI calls. publish() blocks
// on flow control, letting the runtime process incoming MAX_DATA window updates.
// tryPublish() completes instantly without yielding to the runtime, causing stalls.
$sent = 0;
while ($sent < $count) {
    try { $pub->publish($benchTopic, $payload); $sent++; }
    catch (\Vireon\VireonException $e) { usleep(500); }
}

// Poll for count
$deadline = time() + 120;
while (time() < $deadline) {
    $val = trim(file_get_contents($tmpfile));
    $recvCount = (int)$val;
    if ($recvCount >= $count) break;
    usleep(50000);
}
$elapsed = (hrtime(true) - $start) / 1e9;

posix_kill($pid, SIGTERM);
pcntl_waitpid($pid, $status);
$received = (int)trim(file_get_contents($tmpfile));
unlink($tmpfile);

$mib = (float)$received * (float)$size / (1024.0 * 1024.0);
$mibPerSec = $elapsed > 0 ? $mib / $elapsed : 0.0;
$tput = $elapsed > 0 ? (int)($received / $elapsed) : 0;

printf("bench: mode=%s received=%d/%d  %g MiB\n", $mode, $received, $count, $mib);
printf("throughput: %d msg/s  %g MiB/s\n", $tput, $mibPerSec);
