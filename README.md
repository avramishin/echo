# Echo

Echo is a lightweight Node.js service over WebSocket that can replace Redis in small closed-contour deployments where running Redis is impossible or unnecessary.

It keeps data in memory and supports simple cache, lock, and pub/sub commands:

- `set`
- `get`
- `delete`
- `setnx`
- `increment`
- `decrement`
- `lock`
- `release`
- `publish`
- `subscribe`
- `flush`
- `list`

No connection authentication is implemented by design. Run Echo only inside a trusted private network.

## Requirements

- Node.js 18+
- One runtime dependency: `ws`

## Install

Install Echo directly from GitHub:

```bash
npm install git+https://github.com/avramishin/echo.git
```

Or add it to `package.json`:

```json
{
  "dependencies": {
    "echo": "git+https://github.com/avramishin/echo.git"
  }
}
```

Then install dependencies:

```bash
npm install
```

Echo ships bundled TypeScript declarations. In TypeScript projects, importing `echo` resolves to the typed client API without extra setup.

## Start Server

```bash
npm start
```

Default address:

```text
ws://localhost:7070
```

Environment variables:

- `ECHO_HOST` binds the server host, default `0.0.0.0`
- `ECHO_PORT` or `PORT` sets the server port, default `7070`
- `ECHO_SWEEP_INTERVAL` sets expired key cleanup interval in milliseconds, default `60000`

Example:

```bash
ECHO_PORT=8080 npm start
```

You can also run the server file directly:

```bash
node server.js
```

## Client Usage

```js
const EchoClient = require('echo');

async function main() {
  const echo = new EchoClient('ws://localhost:7070');
  await echo.connect();

  await echo.set('user:1', { name: 'Ada' }, 10_000);
  const user = await echo.get('user:1');
  const visits = await echo.increment('visits');

  const created = await echo.setnx('job:42', 'running', 30_000);
  const keys = await echo.list('user:');

  console.log({ user, visits, created, keys });
  echo.close();
}

main().catch(console.error);
```

## TypeScript Usage

```ts
import EchoClient from 'echo';

type User = {
  name: string;
};

async function main() {
  const echo = new EchoClient('ws://localhost:7070', { timeout: 5_000 });
  await echo.connect();

  await echo.set<User>('user:1', { name: 'Ada' }, 10_000);
  const user = await echo.get<User>('user:1');
  const balance = await echo.decrement('credits', 5);

  await echo.subscribe<{ type: string }>('events', (message, channel) => {
    console.log(channel, message.type);
  });

  console.log(user?.name, balance);
  echo.close();
}

main().catch(console.error);
```

For server-side imports, use:

```ts
import { createEchoServer } from 'echo/server';
```

## Pub/Sub

```js
const EchoClient = require('echo');

async function main() {
  const subscriber = new EchoClient('ws://localhost:7070');
  const publisher = new EchoClient('ws://localhost:7070');

  await subscriber.connect();
  await publisher.connect();

  await subscriber.subscribe('events', (message, channel) => {
    console.log(channel, message);
  });

  const delivered = await publisher.publish('events', { type: 'ready' });
  console.log(`delivered to ${delivered} subscriber(s)`);
}

main().catch(console.error);
```

## Benchmark

Run a local benchmark:

```bash
npm run benchmark
```

Useful options:

```bash
npm run benchmark -- --command=set --ops=50000 --clients=4 --pipeline=64 --payload-size=128
npm run benchmark -- --command=get --ops=50000 --clients=4 --pipeline=64 --keys=10000
npm run benchmark -- --command=mixed --ops=50000 --clients=4 --pipeline=64 --keys=10000
```

Supported commands are `set`, `setttl`, `get`, `setnx`, and `mixed`.
The benchmark starts an in-process Echo server on `127.0.0.1` and reports throughput plus p50/p95/p99 request latency.

### Benchmark Results

These measurements were made on a MacBook Neo with Node.js `v26.0.0`.
The benchmark used local loopback `127.0.0.1`, so real network latency and deployment limits are not included.

| Scenario | Ops | Clients | Pipeline | Payload | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `set` | 200k | 4 | 64 | 128B | 132,759 ops/sec | 1.60ms | 3.52ms | 5.83ms |
| `get` | 200k | 4 | 64 | 128B | 142,869 ops/sec | 1.59ms | 2.65ms | 5.50ms |
| `setttl` | 200k | 4 | 64 | 128B | 124,054 ops/sec | 1.64ms | 4.02ms | 6.86ms |
| `mixed` | 200k | 4 | 64 | 128B | 127,297 ops/sec | 1.71ms | 3.38ms | 7.36ms |
| `set` | 200k | 4 | 64 | 1KB | 118,495 ops/sec | 1.97ms | 3.08ms | 3.74ms |
| `set` | 50k | 4 | 1 | 128B | 73,233 ops/sec | 0.05ms | 0.08ms | 0.18ms |

## Locks

`lock()` returns a token string when the lock is acquired, or `false` when the key is already locked. Pass the same token to `release()` to unlock.

```js
const token = await echo.lock('billing:run', 60_000);

if (token) {
  try {
    // protected work
  } finally {
    await echo.release('billing:run', token);
  }
}
```

## Commands

### set(key, value, ttl)

Stores a JSON-serializable value by key.

- `key`: non-empty string
- `value`: any JSON-serializable value
- `ttl`: optional time to live in milliseconds
- returns `true`

If `ttl` is `0`, the key is immediately expired and not stored.

### get(key)

Reads a value by key.

- returns the stored value
- returns `null` when the key does not exist or expired

### delete(key)

Deletes a cache key.

- returns `true` when the key existed and was deleted
- returns `false` when the key did not exist

### setnx(key, value, ttl)

Stores a value only when the key does not exist.

- returns `true` when the value was stored
- returns `false` when the key already exists

### lock(key, ttl, token)

Acquires a named lock.

- `ttl`: optional lock time to live in milliseconds
- `token`: optional caller-provided token
- returns the lock token when acquired
- returns `false` when the key is already locked

Use a TTL for locks to avoid stale locks after process failures.

### increment(key, amount)

Increments a numeric value and returns the updated result.

- `key`: non-empty string
- `amount`: optional finite number, default `1`
- returns updated numeric value

If the key does not exist, Echo starts from `0`. If the stored value is not a finite number, the command returns `BAD_REQUEST`. Existing TTL is preserved.

### decrement(key, amount)

Decrements a numeric value and returns the updated result.

- `key`: non-empty string
- `amount`: optional finite number, default `1`
- returns updated numeric value

If the key does not exist, Echo starts from `0`. If the stored value is not a finite number, the command returns `BAD_REQUEST`. Existing TTL is preserved.

### release(key, token)

Releases a lock.

- returns `true` when the token matches and the lock was released
- returns `false` when the lock does not exist or the token does not match

### publish(channel, message)

Publishes a JSON-serializable message to a channel.

- returns the number of connected subscribers that received the message

### subscribe(channel, handler)

Subscribes the current client connection to a channel.

- `handler(message, channel)` is called for every published message
- returns `true`

### flush()

Clears all cache keys and locks.

- returns `true`

Subscriptions are not removed by `flush()`.

### list(prefix)

Lists existing non-expired cache keys.

- `prefix`: optional string prefix filter
- returns `string[]`

## Wire Protocol

Clients send JSON messages:

```json
{
  "id": 1,
  "command": "set",
  "args": {
    "key": "hello",
    "value": "world",
    "ttl": 1000
  }
}
```

Successful response:

```json
{
  "id": 1,
  "ok": true,
  "result": true
}
```

Error response:

```json
{
  "id": 1,
  "ok": false,
  "error": {
    "code": "BAD_REQUEST",
    "message": "key must be a non-empty string"
  }
}
```

Published messages are pushed to subscribers:

```json
{
  "type": "message",
  "channel": "events",
  "message": {
    "type": "ready"
  }
}
```

## Operational Notes

- Echo stores everything in process memory. Restarting the server clears all data.
- TTL is checked lazily when keys are read and expired keys behave as missing.
- Expired cache keys and locks are physically removed by a centralized cleanup pass every minute by default.
- Echo is intended for small deployments and simple coordination tasks, not as a durable database.
- Run a single Echo instance when clients need a shared cache or shared locks.
- Put Echo behind private network boundaries. There is no authentication or authorization layer.
