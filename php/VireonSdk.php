<?php
declare(strict_types=1);

/**
 * Vireon PHP SDK — OOP wrapper around the Rust C ABI cdylib.
 *
 * Uses PHP FFI to call into libvireon_php.so. Each handle-holding class
 * has __destruct() for RAII cleanup. Errors throw VireonException.
 *
 * Usage:
 *   use Vireon\VireonClientBuilder, Vireon\VireonTlsVerify;
 *   $sub = (new VireonClientBuilder('127.0.0.1:4433'))
 *       ->tlsVerify(VireonTlsVerify::DangerAcceptInvalid)
 *       ->connect();
 */

namespace Vireon;

class VireonException extends \Exception {}

/* ── Constants ─────────────────────────────────────────────────────── */

final class VireonDeliveryPolicy {
    const ReliableOrdered   = 0;
    const ReliableUnordered = 1;
    const RealtimeDropOld   = 2;
    const LatestOnly        = 3;
}

final class VireonTlsVerify {
    const Tofu              = 0;  // trust-on-first-use (default)
    const DangerAcceptInvalid = 1; // no verification (dev only)
    const Strict            = 2;  // verify against CA bundle
    const Pinned            = 3;  // pin single certificate
}

/* ── Message ──────────────────────────────────────────────────────── */

final class VireonMessage {
    public string $topic;
    public string $payload;
    public int $seq;
    public int $stream_id;

    public function __construct(string $topic, string $payload, int $seq, int $stream_id) {
        $this->topic = $topic;
        $this->payload = $payload;
        $this->seq = $seq;
        $this->stream_id = $stream_id;
    }
}

/* ── FFI engine ──────────────────────────────────────────────────── */

/**
 * Internal: loads libvireon_php.so via FFI and provides check helpers.
 * Not part of the public API — users interact via VireonClient etc.
 */
final class VireonFFI {
    private static ?\FFI $ffi = null;
    private static bool $initialized = false;

    public static function init(): void {
        if (self::$initialized) return;
        // Try: VIREON_LIB env, then dlopen by name (respects LD_LIBRARY_PATH),
        // then common relative paths from this file.
        $candidates = [
            getenv('VIREON_LIB') ?: '',
            'libvireon_php.so',  // dlopen via LD_LIBRARY_PATH
            __DIR__ . '/../../../target/x86_64-unknown-linux-gnu/release/libvireon_php.so',
        ];
        $lib = null;
        foreach ($candidates as $c) {
            if ($c === '') continue;
            if ($c === 'libvireon_php.so' || file_exists($c)) { $lib = $c; break; }
        }
        if ($lib === null) {
            throw new VireonException("libvireon_php.so not found. Set VIREON_LIB or LD_LIBRARY_PATH.");
        }

        $decl = <<<CDECL
typedef struct { const char* topic; const uint8_t* payload; size_t payload_len; uint64_t seq; uint64_t stream_id; } VireonMessage;
typedef struct { VireonMessage* msgs; size_t count; } VireonMsgBatch;

int vireon_init(void);
const char* vireon_last_error(void);
void vireon_msg_free(VireonMessage* msg);
void vireon_batch_free(VireonMsgBatch* batch);

intptr_t vireon_connect(const char*, int, const char*, const char*, uint64_t, uint64_t, uint64_t, double, int, int, double, double, const char*, const char*);
intptr_t vireon_pool_connect(const char*, int, const char*, const char*, uint64_t, uint64_t, uint64_t, double, int, int, double, double, const char*, const char*, int);

int vireon_client_publish(intptr_t, const char*, const uint8_t*, size_t);
int vireon_client_try_publish(intptr_t, const char*, const uint8_t*, size_t);
intptr_t vireon_client_subscribe(intptr_t, const char*);
int vireon_client_unsubscribe(intptr_t, const char*);
intptr_t vireon_client_open_stream(intptr_t, int, const char*);
intptr_t vireon_client_subscribe_group(intptr_t, const char*, const char*, const char*);
int vireon_client_leave_group(intptr_t, const char*, const char*, const char*);
int vireon_client_rpc(intptr_t, const char*, const uint8_t*, size_t, const char*, double, VireonMessage*);
int vireon_client_close(intptr_t);
int vireon_client_migrate(intptr_t, const char*);
uint64_t vireon_client_pending_bytes(intptr_t);

int vireon_sub_recv(intptr_t, VireonMessage*);
int vireon_sub_recv_batch(intptr_t, int, VireonMsgBatch*);
void vireon_sub_close(intptr_t);

int vireon_group_sub_recv(intptr_t, VireonMessage*);
int vireon_group_sub_recv_batch(intptr_t, int, VireonMsgBatch*);
void vireon_group_sub_close(intptr_t);

int vireon_stream_recv(intptr_t, VireonMessage*);
int vireon_stream_recv_batch(intptr_t, int, VireonMsgBatch*);
int vireon_stream_publish(intptr_t, const char*, const uint8_t*, size_t);
int vireon_stream_try_publish(intptr_t, const char*, const uint8_t*, size_t);
uint64_t vireon_stream_id(intptr_t);
uint64_t vireon_stream_pending_bytes(intptr_t);
void vireon_stream_close(intptr_t);

int vireon_pool_len(intptr_t);
intptr_t vireon_pool_member(intptr_t, int);
int vireon_pool_publish(intptr_t, const char*, const uint8_t*, size_t);
int vireon_pool_try_publish(intptr_t, const char*, const uint8_t*, size_t);
uint64_t vireon_pool_pending_bytes(intptr_t);
int vireon_pool_close(intptr_t);
CDECL;

        self::$ffi = \FFI::cdef($decl, $lib);
        self::$ffi->vireon_init();
        self::$initialized = true;
    }

    public static function ffi(): \FFI {
        self::init();
        return self::$ffi;
    }

    /** Throw VireonException with the thread-local last error. */
    public static function checkError(int $rc): void {
        if ($rc < 0) {
            // vireon_last_error() returns const char* — PHP FFI auto-converts to string
            $msg = self::$ffi->vireon_last_error();
            throw new VireonException(is_string($msg) && $msg !== '' ? $msg : 'unknown error');
        }
    }

    /** Check a handle (0 means error). */
    public static function checkHandle(int $h): int {
        if ($h === 0) {
            $msg = self::$ffi->vireon_last_error();
            throw new VireonException(is_string($msg) && $msg !== '' ? $msg : 'unknown error');
        }
        return $h;
    }

    /** Convert a FFI VireonMessage to a PHP VireonMessage and free the FFI copy. */
    public static function msgFromNative(\FFI\CData $m): ?VireonMessage {
        $topic = $m->topic !== null ? \FFI::string($m->topic) : '';
        $payload = '';
        if ($m->payload !== null && $m->payload_len > 0) {
            $payload = \FFI::string($m->payload, $m->payload_len);
        }
        $msg = new VireonMessage($topic, $payload, $m->seq, $m->stream_id);
        self::$ffi->vireon_msg_free(\FFI::addr($m));
        return $msg;
    }
}

/* ── Client ───────────────────────────────────────────────────────── */

final class VireonClient {
    private int $handle = 0;

    public function __construct(int $handle) {
        $this->handle = $handle;
    }

    public function __destruct() {
        if ($this->handle !== 0) {
            VireonFFI::ffi()->vireon_client_close($this->handle);
            $this->handle = 0;
        }
    }

    public function publish(string $topic, string $payload): void {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        VireonFFI::checkError($ffi->vireon_client_publish($this->handle, $topic, $buf, $len));
    }

    public function tryPublish(string $topic, string $payload): void {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        VireonFFI::checkError($ffi->vireon_client_try_publish($this->handle, $topic, $buf, $len));
    }

    public function subscribe(string $pattern): VireonSubscription {
        $h = VireonFFI::checkHandle(VireonFFI::ffi()->vireon_client_subscribe($this->handle, $pattern));
        return new VireonSubscription($h);
    }

    public function unsubscribe(string $pattern): void {
        VireonFFI::checkError(VireonFFI::ffi()->vireon_client_unsubscribe($this->handle, $pattern));
    }

    public function openStream(int $policy, string $topic = ''): VireonStream {
        $h = VireonFFI::checkHandle(
            VireonFFI::ffi()->vireon_client_open_stream($this->handle, $policy, $topic ?: null));
        return new VireonStream($h);
    }

    public function subscribeGroup(string $topic, string $group, string $consumer): VireonGroupSubscription {
        $h = VireonFFI::checkHandle(
            VireonFFI::ffi()->vireon_client_subscribe_group($this->handle, $topic, $group, $consumer));
        return new VireonGroupSubscription($h);
    }

    public function leaveGroup(string $topic, string $group, string $consumer): void {
        VireonFFI::checkError(
            VireonFFI::ffi()->vireon_client_leave_group($this->handle, $topic, $group, $consumer));
    }

    public function rpc(string $reqTopic, string $payload, string $replyTopic, float $timeoutSecs): VireonMessage {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        $out = $ffi->new("VireonMessage");
        VireonFFI::checkError($ffi->vireon_client_rpc($this->handle, $reqTopic, $buf, $len, $replyTopic, $timeoutSecs, \FFI::addr($out)));
        return VireonFFI::msgFromNative($out);
    }

    public function migrate(string $bindAddr): void {
        VireonFFI::checkError(VireonFFI::ffi()->vireon_client_migrate($this->handle, $bindAddr));
    }

    public function pendingBytes(): int {
        return VireonFFI::ffi()->vireon_client_pending_bytes($this->handle);
    }
}

/* ── Subscription ─────────────────────────────────────────────────── */

final class VireonSubscription {
    private int $handle = 0;

    public function __construct(int $handle) { $this->handle = $handle; }

    public function __destruct() {
        if ($this->handle !== 0) {
            VireonFFI::ffi()->vireon_sub_close($this->handle);
            $this->handle = 0;
        }
    }

    public function recv(): ?VireonMessage {
        $ffi = VireonFFI::ffi();
        $out = $ffi->new("VireonMessage");
        $rc = $ffi->vireon_sub_recv($this->handle, \FFI::addr($out));
        if ($rc === 0) return VireonFFI::msgFromNative($out);
        if ($rc === 1) return null;
        VireonFFI::checkError($rc);
        return null;
    }

    public function recvBatch(int $maxCount = 256): array {
        $ffi = VireonFFI::ffi();
        $batch = $ffi->new("VireonMsgBatch");
        $rc = $ffi->vireon_sub_recv_batch($this->handle, $maxCount, \FFI::addr($batch));
        if ($rc < 0) {
            VireonFFI::checkError($rc);
            return [];
        }
        $result = [];
        if ($batch->count > 0 && $batch->msgs !== null) {
            for ($i = 0; $i < $batch->count; $i++) {
                $m = $batch->msgs[$i];
                $topic = $m->topic !== null ? \FFI::string($m->topic) : '';
                $payload = ($m->payload !== null && $m->payload_len > 0)
                    ? \FFI::string($m->payload, $m->payload_len) : '';
                $result[] = new VireonMessage($topic, $payload, $m->seq, $m->stream_id);
            }
        }
        $ffi->vireon_batch_free(\FFI::addr($batch));
        return $result;
    }
}

/* ── GroupSubscription ────────────────────────────────────────────── */

final class VireonGroupSubscription {
    private int $handle = 0;

    public function __construct(int $handle) { $this->handle = $handle; }

    public function __destruct() {
        if ($this->handle !== 0) {
            VireonFFI::ffi()->vireon_group_sub_close($this->handle);
            $this->handle = 0;
        }
    }

    public function recv(): ?VireonMessage {
        $ffi = VireonFFI::ffi();
        $out = $ffi->new("VireonMessage");
        $rc = $ffi->vireon_group_sub_recv($this->handle, \FFI::addr($out));
        if ($rc === 0) return VireonFFI::msgFromNative($out);
        if ($rc === 1) return null;
        VireonFFI::checkError($rc);
        return null;
    }

    public function recvBatch(int $maxCount = 256): array {
        $ffi = VireonFFI::ffi();
        $batch = $ffi->new("VireonMsgBatch");
        $rc = $ffi->vireon_group_sub_recv_batch($this->handle, $maxCount, \FFI::addr($batch));
        if ($rc < 0) {
            VireonFFI::checkError($rc);
            return [];
        }
        $result = [];
        if ($batch->count > 0 && $batch->msgs !== null) {
            for ($i = 0; $i < $batch->count; $i++) {
                $m = $batch->msgs[$i];
                $topic = $m->topic !== null ? \FFI::string($m->topic) : '';
                $payload = ($m->payload !== null && $m->payload_len > 0)
                    ? \FFI::string($m->payload, $m->payload_len) : '';
                $result[] = new VireonMessage($topic, $payload, $m->seq, $m->stream_id);
            }
        }
        $ffi->vireon_batch_free(\FFI::addr($batch));
        return $result;
    }
}

/* ── Stream ───────────────────────────────────────────────────────── */

final class VireonStream {
    private int $handle = 0;

    public function __construct(int $handle) { $this->handle = $handle; }

    public function __destruct() {
        if ($this->handle !== 0) {
            VireonFFI::ffi()->vireon_stream_close($this->handle);
            $this->handle = 0;
        }
    }

    public function recv(): ?VireonMessage {
        $ffi = VireonFFI::ffi();
        $out = $ffi->new("VireonMessage");
        $rc = $ffi->vireon_stream_recv($this->handle, \FFI::addr($out));
        if ($rc === 0) return VireonFFI::msgFromNative($out);
        if ($rc === 1) return null;
        VireonFFI::checkError($rc);
        return null;
    }

    public function recvBatch(int $maxCount = 256): array {
        $ffi = VireonFFI::ffi();
        $batch = $ffi->new("VireonMsgBatch");
        $rc = $ffi->vireon_stream_recv_batch($this->handle, $maxCount, \FFI::addr($batch));
        if ($rc < 0) {
            VireonFFI::checkError($rc);
            return [];
        }
        $result = [];
        if ($batch->count > 0 && $batch->msgs !== null) {
            for ($i = 0; $i < $batch->count; $i++) {
                $m = $batch->msgs[$i];
                $topic = $m->topic !== null ? \FFI::string($m->topic) : '';
                $payload = ($m->payload !== null && $m->payload_len > 0)
                    ? \FFI::string($m->payload, $m->payload_len) : '';
                $result[] = new VireonMessage($topic, $payload, $m->seq, $m->stream_id);
            }
        }
        $ffi->vireon_batch_free(\FFI::addr($batch));
        return $result;
    }

    public function publish(string $topic, string $payload): void {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        VireonFFI::checkError($ffi->vireon_stream_publish($this->handle, $topic, $buf, $len));
    }

    public function tryPublish(string $topic, string $payload): void {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        VireonFFI::checkError($ffi->vireon_stream_try_publish($this->handle, $topic, $buf, $len));
    }

    public function streamId(): int {
        return VireonFFI::ffi()->vireon_stream_id($this->handle);
    }

    public function pendingBytes(): int {
        return VireonFFI::ffi()->vireon_stream_pending_bytes($this->handle);
    }
}

/* ── ClientPool ───────────────────────────────────────────────────── */

final class VireonClientPool {
    private int $handle = 0;

    public function __construct(int $handle) { $this->handle = $handle; }

    public function __destruct() {
        if ($this->handle !== 0) {
            VireonFFI::ffi()->vireon_pool_close($this->handle);
            $this->handle = 0;
        }
    }

    public function len(): int {
        return VireonFFI::ffi()->vireon_pool_len($this->handle);
    }

    public function publish(string $topic, string $payload): void {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        VireonFFI::checkError($ffi->vireon_pool_publish($this->handle, $topic, $buf, $len));
    }

    public function tryPublish(string $topic, string $payload): void {
        $ffi = VireonFFI::ffi();
        $len = strlen($payload);
        $buf = _vireon_str_to_ffi($payload);
        VireonFFI::checkError($ffi->vireon_pool_try_publish($this->handle, $topic, $buf, $len));
    }

    public function pendingBytes(): int {
        return VireonFFI::ffi()->vireon_pool_pending_bytes($this->handle);
    }
}

/* ── ClientBuilder ────────────────────────────────────────────────── */

final class VireonClientBuilder {
    private string $addr;
    private int $tlsMode = VireonTlsVerify::Tofu;
    private ?string $tlsPath = null;
    private ?string $sni = null;
    private int $maxMsgSize = 1024 * 1024;
    private int $subscriberBuffer = 65536;
    private int $cmdChannelCap = 1024;
    private float $idleTimeoutSecs = 60.0;
    private bool $reconnectEnabled = false;
    private int $reconnectMaxAttempts = 0;
    private float $reconnectInitialSecs = 0.5;
    private float $reconnectMaxSecs = 10.0;
    private ?string $identityCert = null;
    private ?string $identityKey = null;

    public function __construct(string $addr) { $this->addr = $addr; }

    public function tlsVerify(int $mode, ?string $path = null): self {
        $this->tlsMode = $mode; $this->tlsPath = $path; return $this;
    }
    public function sni(string $s): self { $this->sni = $s; return $this; }
    public function clientIdentity(string $cert, string $key): self {
        $this->identityCert = $cert; $this->identityKey = $key; return $this;
    }
    public function reconnect(int $max, float $init = 0.5, float $maxc = 10.0): self {
        $this->reconnectEnabled = $max > 0; $this->reconnectMaxAttempts = $max;
        $this->reconnectInitialSecs = $init; $this->reconnectMaxSecs = $maxc; return $this;
    }
    public function maxMessageSize(int $n): self { $this->maxMsgSize = $n; return $this; }
    public function subscriberBuffer(int $n): self { $this->subscriberBuffer = $n; return $this; }
    public function cmdChannelCap(int $n): self { $this->cmdChannelCap = $n; return $this; }
    public function maxIdleTimeout(float $s): self { $this->idleTimeoutSecs = $s; return $this; }

    private function ffiArgs(): array {
        return [
            $this->addr, $this->tlsMode, $this->tlsPath, $this->sni,
            $this->maxMsgSize, $this->subscriberBuffer, $this->cmdChannelCap,
            $this->idleTimeoutSecs,
            $this->reconnectEnabled ? 1 : 0, $this->reconnectMaxAttempts,
            $this->reconnectInitialSecs, $this->reconnectMaxSecs,
            $this->identityCert, $this->identityKey,
        ];
    }

    public function connect(): VireonClient {
        VireonFFI::init();
        $h = VireonFFI::checkHandle(VireonFFI::ffi()->vireon_connect(...$this->ffiArgs()));
        return new VireonClient($h);
    }

    public function connectPool(int $n): VireonClientPool {
        VireonFFI::init();
        $args = $this->ffiArgs();
        $args[] = $n;
        $h = VireonFFI::checkHandle(VireonFFI::ffi()->vireon_pool_connect(...$args));
        return new VireonClientPool($h);
    }
}

/* ── Helper: wrap PHP string as FFI uint8_t* for publish calls ───── */

/**
 * Returns an FFI uint8_t array view of the string, or null for empty payloads.
 * The caller must keep the returned CData alive for the duration of the FFI call.
 */
function _vireon_str_to_ffi(string $s): ?\FFI\CData {
    $len = strlen($s);
    if ($len === 0) return null;
    $buf = VireonFFI::ffi()->new("uint8_t[$len]");
    \FFI::memcpy($buf, $s, $len);
    return $buf;
}
