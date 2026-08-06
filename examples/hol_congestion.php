<?php
// HoL Congestion — 5-stream head-of-line blocking isolation
// Fork-based: 5 drain children + parent publisher.

require __DIR__ . '/../php/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;
use Vireon\VireonDeliveryPolicy;

const HEAVY_COUNT = 2000;
const NUM_STREAMS = 5;

$addr = getenv('VIREON_ADDR') ?: '127.0.0.1:4433';

// Fork drain children
$tmpfiles = [];
$pids = [];
for ($i = 0; $i < NUM_STREAMS; $i++) {
    $tmpfiles[$i] = tempnam(sys_get_temp_dir(), "vireon_hol$i");
    $pid = pcntl_fork();
    if ($pid < 0) { fwrite(STDERR, "fork failed\n"); exit(1); }
    if ($pid === 0) {
        // ── Child i: open stream and drain ──
        $sub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();
        $topic = "hol.stream$i";
        $stream = $sub->openStream(VireonDeliveryPolicy::ReliableOrdered, $topic);
        usleep(300000);
        file_put_contents($tmpfiles[$i], '0');

        $count = 0;
        while (true) {
            $msg = $stream->recv();
            if ($msg === null) break;
            $count++;
            file_put_contents($tmpfiles[$i], (string)$count);
        }
        exit(0);
    }
    $pids[$i] = $pid;
}

// ── Parent: wait for all children ready ──
$deadline = time() + 10;
while (time() < $deadline) {
    $allReady = true;
    for ($i = 0; $i < NUM_STREAMS; $i++) {
        if (trim(file_get_contents($tmpfiles[$i])) === '') { $allReady = false; break; }
    }
    if ($allReady) break;
    usleep(20000);
}

// Flood heavy stream (0), interleaving light publishes
$pub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();
$payload = str_repeat('x', 1024);

for ($i = 0; $i < HEAVY_COUNT; $i++) {
    if ($i % 100 === 0 && $i > 0) {
        for ($s = 1; $s < NUM_STREAMS; $s++) {
            $pub->publish("hol.stream$s", $payload);
        }
    }
    try { $pub->tryPublish('hol.stream0', $payload); }
    catch (\Vireon\VireonException $e) { usleep(500); $i--; }
}

// Give drain children time to catch up
sleep(2);

// Collect counts
$counts = [];
for ($i = 0; $i < NUM_STREAMS; $i++) {
    $counts[$i] = (int)trim(file_get_contents($tmpfiles[$i]));
    posix_kill($pids[$i], SIGTERM);
    pcntl_waitpid($pids[$i], $status);
    unlink($tmpfiles[$i]);
}

$heavy = $counts[0];
$lightest = PHP_INT_MAX;
$total = 0;
for ($s = 1; $s < NUM_STREAMS; $s++) {
    $lightest = min($lightest, $counts[$s]);
    $total += $counts[$s];
}
$total += $heavy;
if ($lightest === PHP_INT_MAX) $lightest = 0;

printf("hol_congestion: heavy=%dmsgs lightest=%dmsgs total=%dmsgs\n", $heavy, $lightest, $total);

for ($s = 1; $s < NUM_STREAMS; $s++) {
    if ($counts[$s] === 0) {
        fwrite(STDERR, "FAIL: light stream $s got 0 messages (HOL blocked)\n");
        exit(1);
    }
}
echo "PASS: HOL isolation verified — light streams delivered during congestion\n";
exit(0);
