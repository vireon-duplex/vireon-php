<?php
// Consumer Group — 3-member round-robin distribution
// Fork-based: 3 drain children + parent publisher.

require __DIR__ . '/../php/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;

const N = 30;
const NUM_CONSUMERS = 3;

$addr = getenv('VIREON_ADDR') ?: '127.0.0.1:4433';

// Create per-child temp files for counts
$tmpfiles = [];
$pids = [];
for ($i = 0; $i < NUM_CONSUMERS; $i++) {
    $tmpfiles[$i] = tempnam(sys_get_temp_dir(), "vireon_cg$i");
    $pid = pcntl_fork();
    if ($pid < 0) { fwrite(STDERR, "fork failed\n"); exit(1); }
    if ($pid === 0) {
        // ── Child i: join group and drain ──
        $c = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();
        $gs = $c->subscribeGroup('task.jobs', 'workers', "c$i");
        usleep(500000);
        file_put_contents($tmpfiles[$i], '0');

        $count = 0;
        while (true) {
            $msg = $gs->recv();
            if ($msg === null) break;
            $count++;
            file_put_contents($tmpfiles[$i], (string)$count);
        }
        exit(0);
    }
    $pids[$i] = $pid;
}

// ── Parent: wait for all children ready ──
$allReady = false;
$deadline = time() + 10;
while (!$allReady && time() < $deadline) {
    $allReady = true;
    for ($i = 0; $i < NUM_CONSUMERS; $i++) {
        if (trim(file_get_contents($tmpfiles[$i])) === '') { $allReady = false; break; }
    }
    if (!$allReady) usleep(20000);
}

// Publish N jobs
$pub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();
$payload = 'job';
for ($i = 0; $i < N; $i++) {
    $pub->publish('task.jobs', $payload);
}

// Poll for delivery
$deadline = time() + 10;
$total = 0;
$counts = [0, 0, 0];
while (time() < $deadline) {
    $total = 0;
    for ($i = 0; $i < NUM_CONSUMERS; $i++) {
        $counts[$i] = (int)trim(file_get_contents($tmpfiles[$i]));
        $total += $counts[$i];
    }
    if ($total >= N) break;
    usleep(50000);
}

// Kill children
for ($i = 0; $i < NUM_CONSUMERS; $i++) {
    posix_kill($pids[$i], SIGTERM);
    pcntl_waitpid($pids[$i], $status);
    unlink($tmpfiles[$i]);
}

printf("consumer_group: delivered: %d/%d\n", $total, N);
printf("  balance: g0=%dmsgs g1=%dmsgs g2=%dmsgs\n", $counts[0], $counts[1], $counts[2]);

if ($total !== N) { fwrite(STDERR, "FAIL: expected " . N . ", got $total\n"); exit(1); }
for ($i = 0; $i < NUM_CONSUMERS; $i++) {
    if ($counts[$i] === 0) { fwrite(STDERR, "FAIL: member c$i got 0 messages\n"); exit(1); }
}
echo "PASS: all jobs distributed, all members received work\n";
exit(0);
