'use strict';

const { EventEmitter } = require('node:events');
const WebSocket = require('ws');

class EchoClient extends EventEmitter {
  /**
   * Creates an Echo WebSocket client.
   *
   * @param {string} url WebSocket URL, for example ws://localhost:7070.
   * @param {{timeout?: number}} [options] Client options.
   */
  constructor(url, options = {}) {
    super();
    this.url = url;
    this.timeout = options.timeout || 5000;
    this.ws = null;
    this.nextId = 1;
    this.pending = new Map();
    this.handlers = new Map();
  }

  /**
   * Opens the WebSocket connection.
   *
   * @returns {Promise<EchoClient>} Resolves with this client when connected.
   */
  connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return Promise.resolve(this);
    }

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.url);
      this.ws = ws;

      const cleanup = () => {
        ws.off('open', onOpen);
        ws.off('error', onError);
      };
      const onOpen = () => {
        cleanup();
        this._attach(ws);
        resolve(this);
      };
      const onError = (error) => {
        cleanup();
        reject(error);
      };

      ws.once('open', onOpen);
      ws.once('error', onError);
    });
  }

  _attach(ws) {
    ws.on('message', (raw) => {
      let payload;
      try {
        payload = JSON.parse(raw.toString());
      } catch (error) {
        this._emitError(error);
        return;
      }

      if (payload.type === 'message') {
        this.emit('message', payload.channel, payload.message);
        const handlers = this.handlers.get(payload.channel);
        if (handlers) {
          for (const handler of handlers) handler(payload.message, payload.channel);
        }
        return;
      }

      const pending = this.pending.get(payload.id);
      if (!pending) return;
      clearTimeout(pending.timer);
      this.pending.delete(payload.id);

      if (payload.ok) {
        pending.resolve(payload.result);
      } else {
        const error = new Error(payload.error ? payload.error.message : 'Echo command failed');
        error.code = payload.error ? payload.error.code : 'ERR';
        pending.reject(error);
      }
    });

    ws.on('close', () => this._rejectPending('connection closed'));
    ws.on('error', (error) => {
      this._rejectPending(error.message);
      this._emitError(error);
    });
  }

  _emitError(error) {
    if (this.listenerCount('error') > 0) {
      this.emit('error', error);
    }
  }

  _rejectPending(message) {
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(new Error(message));
      this.pending.delete(id);
    }
  }

  _request(command, args = {}) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('Echo client is not connected'));
    }

    const id = this.nextId++;
    const payload = JSON.stringify({ id, command, args });

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Echo command timed out: ${command}`));
      }, this.timeout);
      if (typeof timer.unref === 'function') timer.unref();

      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(payload, (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error);
      });
    });
  }

  /**
   * Stores a value by key.
   *
   * @param {string} key Cache key.
   * @param {*} value JSON-serializable value.
   * @param {number} [ttl] Optional time to live in milliseconds.
   * @returns {Promise<boolean>} True when the value was stored.
   */
  set(key, value, ttl) {
    return this._request('set', { key, value, ttl });
  }

  /**
   * Reads a value by key.
   *
   * @param {string} key Cache key.
   * @returns {Promise<*>} Stored value, or null when the key does not exist or expired.
   */
  get(key) {
    return this._request('get', { key });
  }

  /**
   * Deletes a key.
   *
   * @param {string} key Cache key.
   * @returns {Promise<boolean>} True when the key existed and was deleted.
   */
  delete(key) {
    return this._request('delete', { key });
  }

  /**
   * Stores a value only when the key does not exist.
   *
   * @param {string} key Cache key.
   * @param {*} value JSON-serializable value.
   * @param {number} [ttl] Optional time to live in milliseconds.
   * @returns {Promise<boolean>} True when the value was stored, false when the key already exists.
   */
  setnx(key, value, ttl) {
    return this._request('setnx', { key, value, ttl });
  }

  /**
   * Acquires a named lock.
   *
   * @param {string} key Lock key.
   * @param {number} [ttl] Optional lock time to live in milliseconds.
   * @param {string} [token] Optional caller-provided lock token.
   * @returns {Promise<string|false>} Lock token when acquired, false when already locked.
   */
  lock(key, ttl, token) {
    return this._request('lock', { key, ttl, token });
  }

  /**
   * Releases a lock acquired by token.
   *
   * @param {string} key Lock key.
   * @param {string} token Lock token returned by lock().
   * @returns {Promise<boolean>} True when the lock was released.
   */
  release(key, token) {
    return this._request('release', { key, token });
  }

  /**
   * Publishes a message to a channel.
   *
   * @param {string} channel Channel name.
   * @param {*} message JSON-serializable message.
   * @returns {Promise<number>} Number of connected subscribers that received the message.
   */
  publish(channel, message) {
    return this._request('publish', { channel, message });
  }

  /**
   * Subscribes to a channel and optionally registers a message handler.
   *
   * @param {string} channel Channel name.
   * @param {(message: *, channel: string) => void} [handler] Message handler.
   * @returns {Promise<boolean>} True when subscription was registered.
   */
  subscribe(channel, handler) {
    if (handler) {
      if (!this.handlers.has(channel)) this.handlers.set(channel, new Set());
      this.handlers.get(channel).add(handler);
    }
    return this._request('subscribe', { channel });
  }

  /**
   * Clears all cache keys and locks.
   *
   * @returns {Promise<boolean>} True when storage was cleared.
   */
  flush() {
    return this._request('flush');
  }

  /**
   * Lists cache keys.
   *
   * @param {string} [prefix] Optional key prefix filter.
   * @returns {Promise<string[]>} Existing non-expired cache keys.
   */
  list(prefix) {
    return this._request('list', { prefix });
  }

  /**
   * Closes the WebSocket connection.
   *
   * @param {number} [code] Optional WebSocket close code.
   * @param {string} [reason] Optional close reason.
   * @returns {void}
   */
  close(code, reason) {
    if (this.ws) this.ws.close(code, reason);
  }
}

module.exports = EchoClient;
module.exports.EchoClient = EchoClient;
