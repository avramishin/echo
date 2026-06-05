# Echo

Echo is a lightweight Node.js service over WebSocket that can replace Redis in small closed-contour deployments where running Redis is impossible or unnecessary.

It keeps data in memory and supports simple cache, lock, and pub/sub commands:

- `set`
- `get`
- `delete`
- `setnx`
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

  const created = await echo.setnx('job:42', 'running', 30_000);
  const keys = await echo.list('user:');

  console.log({ user, created, keys });
  echo.close();
}

main().catch(console.error);
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
