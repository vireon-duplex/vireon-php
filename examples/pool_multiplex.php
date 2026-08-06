<?php
// Pool Multiplex — 4-connection pool throughput test
// Fork-based: 1 drain child + parent pool publisher.

require __DIR__ . '/../php/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;

const N = 1000;

$addr = getenv('VIREON_ADDR') ?: '127.0.0.1:4433';

$tmpfile = tempnam(sys_get_temp_dir(), 'vireon_pool');
$pid = pcntl_fork();
if ($pid < 0) { fwrite(STDERR, "fork failed\n"); exit(1); }

if ($pid === 0) {
    // ── Child: drain ──
    $sub = (new VireonClientBuilder($addr))->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)->connect();
    $s = $sub->subscribe('pool.test');
    usleep(300000);
    file_put_contents($tmpfile, '0');

    $count = 0;
    while ($count < N) {
        $msg = $s->recv();
        if ($msg === null) break;
        $count++;
        if ($count % 50 === 0) file_put_contents($tmpfile, (string)$count);
    }
    file_put_contents($tmpfile, (string)$count);
    exit(0);
}

// ── Parent: wait for ready, publish via pool ──
while (trim(file_get_contents($tmpfile)) === '') usleep(10000);

$pool = (new VireonClientBuilder($addr))
    ->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)
    ->connectPool(4);

$payload = 'x';
$start = hrtime(true);
for ($i = 0; $i < N; $i++) {
    while (true) {
        try { $pool->tryPublish('pool.test', $payload); break; }
        catch (\Vireon\VireonException $e) { usleep(500); }
    }
}

// Poll for count
$deadline = time() + 30;
while (time() < $deadline) {
    $count = (int)trim(file_get_contents($tmpfile));
    if ($count >= N) break;
    usleep(50000);
}
$elapsed = (hrtime(true) - $start) / 1e9;

posix_kill($pid, SIGTERM);
pcntl_waitpid($pid, $status);
$count = (int)trim(file_get_contents($tmpfile));
unlink($tmpfile);

$tput = $elapsed > 0 ? (int)($count / $elapsed) : 0;
printf("pool_multiplex: received %d/%d messages | throughput: %d msg/s\n", $count, N, $tput);

if ($count !== N) { fwrite(STDERR, "FAIL: expected " . N . ", got $count\n"); exit(1); }
echo "PASS: all messages delivered via 4-connection pool\n";
exit(0);
