#!/usr/bin/env node
'use strict';

const crypto = require('node:crypto');
const { WebSocketServer } = require('ws');

const DEFAULT_PORT = Number(process.env.ECHO_PORT || process.env.PORT || 7070);
const DEFAULT_HOST = process.env.ECHO_HOST || '0.0.0.0';
const DEFAULT_SWEEP_INTERVAL = Number(process.env.ECHO_SWEEP_INTERVAL || 60_000);

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
  const store = new Map();
  const locks = new Map();
  const subscribers = new Map();
  const wss = new WebSocketServer({ port, host });
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
      default:
        throw makeError(`unknown command: ${command}`, 'UNKNOWN_COMMAND');
    }
  }

  wss.on('connection', (ws) => {
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

    ws.on('close', () => unsubscribeSocket(ws));
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
