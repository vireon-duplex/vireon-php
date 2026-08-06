<?php
// Ordering — 500-frame in-order delivery verification
// Fork-based drain: child drains, parent publishes.

require __DIR__ . '/../php/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;
use Vireon\VireonDeliveryPolicy;

const N = 500;

$addr = getenv('VIREON_ADDR') ?: '127.0.0.1:4433';

$tmpfile = tempnam(sys_get_temp_dir(), 'vireon_ord');
$pid = pcntl_fork();
if ($pid < 0) { fwrite(STDERR, "fork failed\n"); exit(1); }

if ($pid === 0) {
    // ── Child: drain ──
    $sub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();
    $stream = $sub->openStream(VireonDeliveryPolicy::ReliableOrdered, 'ordering.test');
    usleep(300000);
    file_put_contents($tmpfile, 'ready');

    $received = 0;
    $gaps = 0;
    $dup = 0;
    $lastSeq = -1;

    while ($received < N) {
        $msg = $stream->recv();
        if ($msg === null) break;
        $seq = $msg->seq;
        if ($seq <= $lastSeq) $dup++;
        if ($seq > $lastSeq + 1 && $lastSeq >= 0) $gaps++;
        $lastSeq = $seq;
        $received++;
        if ($received % 50 === 0) {
            file_put_contents($tmpfile, "$received,$gaps,$dup");
        }
    }
    file_put_contents($tmpfile, "$received,$gaps,$dup");
    exit(0);
}

// ── Parent: wait for ready, publish ──
while (trim(file_get_contents($tmpfile)) === '') usleep(10000);

$pub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();

$payload = str_repeat("\0", 256);
for ($i = 0; $i < N; $i++) {
    $packed = pack('V', $i);  // 4-byte little-endian
    $payload[0] = $packed[0]; $payload[1] = $packed[1];
    $payload[2] = $packed[2]; $payload[3] = $packed[3];
    try { $pub->tryPublish('ordering.test', $payload); }
    catch (\Vireon\VireonException $e) { usleep(1000); $i--; }
}

// Poll for result
$deadline = time() + 30;
$result = null;
while (time() < $deadline) {
    $val = trim(file_get_contents($tmpfile));
    if ($val !== '' && $val !== 'ready' && strpos($val, ',') !== false) {
        $result = $val;
        [$recv, $gaps, $dup] = explode(',', $val);
        if ((int)$recv >= N) break;
    }
    usleep(50000);
}

posix_kill($pid, SIGTERM);
pcntl_waitpid($pid, $status);
unlink($tmpfile);

$recv = (int)($recv ?? 0);
$gaps = (int)($gaps ?? 0);
$dup = (int)($dup ?? 0);

printf("ordering: received: %d/%d\n", $recv, N);
printf("  gaps: %d\n", $gaps);
printf("  duplicates: %d\n", $dup);

if ($recv !== N) { fwrite(STDERR, "FAIL: expected " . N . ", got $recv\n"); exit(1); }
if ($gaps > 0 || $dup > 0) { fwrite(STDERR, "FAIL: gaps=$gaps duplicates=$dup\n"); exit(1); }
echo "PASS: all frames delivered in order, no gaps, no duplicates\n";
