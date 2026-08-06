<?php
// Quickstart — basic pub/sub + all delivery policies
// Sequential, no fork needed.

require __DIR__ . '/../php/VireonSdk.php';

use Vireon\VireonClientBuilder;
use Vireon\VireonTlsVerify;
use Vireon\VireonDeliveryPolicy;

$addr = getenv('VIREON_ADDR') ?: '127.0.0.1:4433';

$connect = function() use ($addr): \Vireon\VireonClient {
    return (new VireonClientBuilder($addr))
        ->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)
        ->connect();
};

$sub = $connect();
$pub = $connect();

// 1. Default-channel pub/sub
$s = $sub->subscribe('sensor.*');
usleep(200000);

$pub->publish('sensor.temp', '42C');
$msg = $s->recv();
if ($msg === null) { fwrite(STDERR, "FAIL: no message\n"); exit(1); }
printf("  pub/sub: %s = %s\n", $msg->topic, $msg->payload);

// 2. All delivery policies
$policies = [
    [VireonDeliveryPolicy::ReliableOrdered,   'RELIABLE_ORDERED'],
    [VireonDeliveryPolicy::ReliableUnordered, 'RELIABLE_UNORDERED'],
    [VireonDeliveryPolicy::RealtimeDropOld,   'REALTIME_DROP_OLD'],
    [VireonDeliveryPolicy::LatestOnly,        'LATEST_ONLY'],
];

foreach ($policies as [$policy, $name]) {
    $topic = "test.$name";
    $stream = $sub->openStream($policy, $topic);
    usleep(200000);

    $data = "data-$name";
    $pub->publish($topic, $data);
    $m = $stream->recv();
    if ($m === null) { fwrite(STDERR, "FAIL: no message on stream $name\n"); exit(1); }
    printf("  %s: topic=%s payload=%s streamId=%d\n", $name, $m->topic, $m->payload, $m->stream_id);
}

echo "\nall 5 delivery policies verified\n";
