#!/usr/bin/env node
'use strict';

const crypto = require('node:crypto');
const { WebSocketServer } = require('ws');

const DEFAULT_PORT = Number(process.env.ECHO_PORT || process.env.PORT || 7070);
const DEFAULT_HOST = process.env.ECHO_HOST || '0.0.0.0';
const DEFAULT_SWEEP_INTERVAL = Number(process.env.ECHO_SWEEP_INTERVAL || 60_000);
const DEFAULT_SECRET_TOKEN = process.env.ECHO_SECRET_TOKEN;
const KNOWN_COMMANDS = new Set([
  'set',
  'get',
  'delete',
  'setnx',
  'increment',
  'decrement',
  'lock',
  'release',
  'publish',
  'subscribe',
  'flush',
  'list',
  'metrics'
]);

function now() {
  return Date.now();
}

function makeError(message, code) {
  const error = new Error(message);
  error.code = code || 'ERR';
  return error;
}

function assertString(value, name) {
  if (typeof value !== 'string' || value.length === 0) {
    throw makeError(`${name} must be a non-empty string`, 'BAD_REQUEST');
  }
}

function assertFiniteNumber(value, name) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw makeError(`${name} must be a finite number`, 'BAD_REQUEST');
  }
}

function tokensEqual(actual, expected) {
  if (typeof actual !== 'string' || typeof expected !== 'string') return false;
  const actualBuffer = Buffer.from(actual);
  const expectedBuffer = Buffer.from(expected);
  if (actualBuffer.length !== expectedBuffer.length) return false;
  return crypto.timingSafeEqual(actualBuffer, expectedBuffer);
}

function isKnownCommand(command) {
  return KNOWN_COMMANDS.has(command);
}

function ttlToExpiresAt(ttl) {
  if (ttl === undefined || ttl === null) return null;
  if (!Number.isFinite(ttl) || ttl < 0) {
    throw makeError('ttl must be a non-negative number in milliseconds', 'BAD_REQUEST');
  }
  return now() + ttl;
}

function createEchoServer(options = {}) {
  const port = options.port ?? DEFAULT_PORT;
  const host = options.host ?? DEFAULT_HOST;
  const sweepInterval = options.sweepInterval ?? DEFAULT_SWEEP_INTERVAL;
  const secretToken = options.secretToken ?? DEFAULT_SECRET_TOKEN;
  const store = new Map();
  const locks = new Map();
  const subscribers = new Map();
  const metrics = {
    connectedClients: 0,
    totalConnections: 0,
    commandCalls: Object.create(null),
    unknownCommands: Object.create(null)
  };
  const wss = new WebSocketServer({
    port,
    host,
    verifyClient: (info, done) => {
      if (!secretToken) {
        done(true);
        return;
      }

      const requestUrl = new URL(info.req.url, `ws://${info.req.headers.host || 'localhost'}`);
      const token = requestUrl.searchParams.get('secretToken');
      done(tokensEqual(token, secretToken), 401, 'Unauthorized');
    }
  });
  const sweeper = setInterval(sweepExpired, sweepInterval);
  if (typeof sweeper.unref === 'function') sweeper.unref();

  function isExpired(entry) {
    return Boolean(entry && entry.expiresAt !== null && entry.expiresAt <= now());
  }

  function deleteKey(key) {
    return store.delete(key);
  }

  function getEntry(key) {
    const entry = store.get(key);
    if (isExpired(entry)) return undefined;
    return entry;
  }

  function setKey(key, value, ttl) {
    const expiresAt = ttlToExpiresAt(ttl);
    store.set(key, { value, expiresAt });
    return true;
  }

  function changeNumber(key, amount) {
    assertFiniteNumber(amount, 'amount');
    const entry = getEntry(key);
    const currentValue = entry ? entry.value : 0;

    if (typeof currentValue !== 'number' || !Number.isFinite(currentValue)) {
      throw makeError('stored value must be a finite number', 'BAD_REQUEST');
    }

    const nextValue = currentValue + amount;
    store.set(key, { value: nextValue, expiresAt: entry ? entry.expiresAt : null });
    return nextValue;
  }

  function deleteLock(key) {
    return locks.delete(key);
  }

  function getLock(key) {
    const lock = locks.get(key);
    if (isExpired(lock)) return undefined;
    return lock;
  }

  function lockKey(key, ttl, token) {
    if (getLock(key)) return false;
    const lockToken = token || crypto.randomUUID();
    const expiresAt = ttlToExpiresAt(ttl);
    if (ttl === 0) return false;
    locks.set(key, { token: lockToken, expiresAt });
    return lockToken;
  }

  function sweepExpired() {
    for (const [key, entry] of store) {
      if (isExpired(entry)) store.delete(key);
    }
    for (const [key, lock] of locks) {
      if (isExpired(lock)) locks.delete(key);
    }
  }

  function unsubscribeSocket(ws) {
    for (const [channel, clients] of subscribers) {
      clients.delete(ws);
      if (clients.size === 0) subscribers.delete(channel);
    }
  }

  function send(ws, payload) {
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(payload));
    }
  }

  function incrementCounter(counters, name) {
    counters[name] = (counters[name] || 0) + 1;
  }

  function readMetrics() {
    return {
      connectedClients: metrics.connectedClients,
      totalConnections: metrics.totalConnections,
      commandCalls: { ...metrics.commandCalls },
      unknownCommands: { ...metrics.unknownCommands }
    };
  }

  function handleCommand(ws, command, args) {
    switch (command) {
      case 'set': {
        const { key, value, ttl } = args;
        assertString(key, 'key');
        return setKey(key, value, ttl);
      }
      case 'get': {
        const { key } = args;
        assertString(key, 'key');
        const entry = getEntry(key);
        return entry ? entry.value : null;
      }
      case 'delete': {
        const { key } = args;
        assertString(key, 'key');
        return deleteKey(key);
      }
      case 'setnx': {
        const { key, value, ttl } = args;
        assertString(key, 'key');
        if (getEntry(key)) return false;
        return setKey(key, value, ttl);
      }
      case 'increment': {
        const { key, amount = 1 } = args;
        assertString(key, 'key');
        return changeNumber(key, amount);
      }
      case 'decrement': {
        const { key, amount = 1 } = args;
        assertString(key, 'key');
        return changeNumber(key, -amount);
      }
      case 'lock': {
        const { key, ttl, token } = args;
        assertString(key, 'key');
        if (token !== undefined && typeof token !== 'string') {
          throw makeError('token must be a string', 'BAD_REQUEST');
        }
        return lockKey(key, ttl, token);
      }
      case 'release': {
        const { key, token } = args;
        assertString(key, 'key');
        assertString(token, 'token');
        const lock = getLock(key);
        if (!lock || lock.token !== token) return false;
        return deleteLock(key);
      }
      case 'publish': {
        const { channel, message } = args;
        assertString(channel, 'channel');
        const clients = subscribers.get(channel);
        if (!clients) return 0;
        let delivered = 0;
        for (const client of clients) {
          if (client.readyState === client.OPEN) {
            send(client, { type: 'message', channel, message });
            delivered += 1;
          }
        }
        return delivered;
      }
      case 'subscribe': {
        const { channel } = args;
        assertString(channel, 'channel');
        if (!subscribers.has(channel)) subscribers.set(channel, new Set());
        subscribers.get(channel).add(ws);
        return true;
      }
      case 'flush': {
        store.clear();
        locks.clear();
        return true;
      }
      case 'list': {
        const { prefix } = args;
        if (prefix !== undefined && typeof prefix !== 'string') {
          throw makeError('prefix must be a string', 'BAD_REQUEST');
        }
        const keys = [];
        for (const key of store.keys()) {
          if (!getEntry(key)) continue;
          if (prefix === undefined || key.startsWith(prefix)) keys.push(key);
        }
        return keys;
      }
      case 'metrics': {
        return readMetrics();
      }
      default:
        throw makeError(`unknown command: ${command}`, 'UNKNOWN_COMMAND');
    }
  }

  wss.on('connection', (ws, request) => {
    const requestUrl = new URL(request.url, `ws://${request.headers.host || 'localhost'}`);
    ws.clientName = requestUrl.searchParams.get('clientName') || 'unknown';
    metrics.connectedClients += 1;
    metrics.totalConnections += 1;
    console.log(`Echo client connected: ${ws.clientName}`);

    ws.on('message', (raw) => {
      let request;
      try {
        request = JSON.parse(raw.toString());
      } catch {
        send(ws, { ok: false, error: { code: 'BAD_JSON', message: 'message must be JSON' } });
        return;
      }

      const id = request && request.id;
      try {
        if (!request || typeof request.command !== 'string') {
          throw makeError('command must be a string', 'BAD_REQUEST');
        }
        if (request.command in metrics.commandCalls || isKnownCommand(request.command)) {
          incrementCounter(metrics.commandCalls, request.command);
        } else {
          incrementCounter(metrics.unknownCommands, request.command);
        }
        const result = handleCommand(ws, request.command, request.args || {});
        send(ws, { id, ok: true, result });
      } catch (error) {
        send(ws, {
          id,
          ok: false,
          error: {
            code: error.code || 'ERR',
            message: error.message || 'unknown error'
          }
        });
      }
    });

    ws.on('close', () => {
      metrics.connectedClients = Math.max(0, metrics.connectedClients - 1);
      unsubscribeSocket(ws);
    });
    ws.on('error', () => unsubscribeSocket(ws));
  });

  wss.on('listening', () => {
    const address = wss.address();
    const shownHost = address.address === '0.0.0.0' ? 'localhost' : address.address;
    console.log(`Echo WS server listening on ws://${shownHost}:${address.port}`);
  });

  return {
    wss,
    close: (callback) => {
      clearInterval(sweeper);
      wss.close(callback);
    }
  };
}

if (require.main === module) {
  createEchoServer();
}

module.exports = {
  createEchoServer
};
