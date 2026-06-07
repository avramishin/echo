#!/usr/bin/env node
'use strict';

const WebSocket = require('ws');
const { createEchoServer } = require('./server');

const DEFAULTS = {
  command: 'set',
  ops: 50_000,
  clients: 4,
  pipeline: 64,
  payloadSize: 128,
  keys: 10_000,
  ttl: 60_000,
  host: '127.0.0.1',
  clientName: 'benchmark',
  secretToken: ''
};

function readOption(name, fallback) {
  const flag = `--${name}=`;
  const arg = process.argv.find((item) => item.startsWith(flag));
  if (!arg) return process.env[`ECHO_BENCH_${name.toUpperCase().replace(/-/g, '_')}`] ?? fallback;
  return arg.slice(flag.length);
}

function readNumber(name, fallback) {
  const value = Number(readOption(name, fallback));
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number`);
  }
  return value;
}

function formatNumber(value) {
  return Math.round(value).toLocaleString('en-US');
}

function percentile(sorted, percent) {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.ceil((percent / 100) * sorted.length) - 1);
  return sorted[index];
}

function createPayload(size) {
  return {
    data: 'x'.repeat(size)
  };
}

function connectionUrl(url, clientName, secretToken) {
  const connection = new URL(url);
  connection.searchParams.set('clientName', clientName);
  if (secretToken) connection.searchParams.set('secretToken', secretToken);
  return connection.toString();
}

function argsFor(command, index, options, value) {
  const key = `bench:${index % options.keys}`;

  if (command === 'set') {
    return { key, value };
  }
  if (command === 'setttl') {
    return { key, value, ttl: options.ttl };
  }
  if (command === 'get') {
    return { key };
  }
  if (command === 'setnx') {
    return { key: `bench:nx:${index}`, value, ttl: options.ttl };
  }
  if (command === 'mixed') {
    return index % 2 === 0 ? { key, value } : { key };
  }

  throw new Error(`Unsupported benchmark command: ${command}`);
}

function commandFor(command, index) {
  if (command === 'mixed') return index % 2 === 0 ? 'set' : 'get';
  if (command === 'setttl') return 'set';
  return command;
}

async function openSocket(url) {
  const ws = new WebSocket(url);
  await new Promise((resolve, reject) => {
    ws.once('open', resolve);
    ws.once('error', reject);
  });
  return ws;
}

async function preload(url, options, value) {
  if (options.command !== 'get' && options.command !== 'mixed') return;

  const ws = await openSocket(connectionUrl(url, 'benchmark-preload', options.secretToken));
  let nextId = 1;
  const pending = new Map();

  ws.on('message', (raw) => {
    const response = JSON.parse(raw.toString());
    const resolve = pending.get(response.id);
    if (!resolve) return;
    pending.delete(response.id);
    resolve();
  });

  function request(args) {
    const id = nextId++;
    ws.send(JSON.stringify({ id, command: 'set', args }));
    return new Promise((resolve) => pending.set(id, resolve));
  }

  const batch = [];
  for (let index = 0; index < options.keys; index += 1) {
    batch.push(request({ key: `bench:${index}`, value }));
    if (batch.length >= options.pipeline) {
      await Promise.all(batch.splice(0, batch.length));
    }
  }
  await Promise.all(batch);
  ws.close();
}

async function runClient(url, clientIndex, options, value, latencies) {
  const ws = await openSocket(connectionUrl(url, `${options.clientName}-${clientIndex}`, options.secretToken));
  const pending = new Map();
  let nextLocal = clientIndex;
  let sent = 0;
  let completed = 0;
  let rejected = null;
  const target = Math.floor(options.ops / options.clients) + (clientIndex < options.ops % options.clients ? 1 : 0);

  function sendNext() {
    while (!rejected && sent - completed < options.pipeline && sent < target) {
      const opIndex = clientIndex + sent * options.clients;
      const id = nextLocal;
      nextLocal += options.clients;
      const command = commandFor(options.command, opIndex);
      const args = argsFor(options.command, opIndex, options, value);
      pending.set(id, process.hrtime.bigint());
      ws.send(JSON.stringify({ id, command, args }));
      sent += 1;
    }
  }

  await new Promise((resolve, reject) => {
    rejected = null;

    ws.on('message', (raw) => {
      const response = JSON.parse(raw.toString());
      const startedAt = pending.get(response.id);
      if (!startedAt) return;
      pending.delete(response.id);
      if (!response.ok) {
        rejected = new Error(response.error ? response.error.message : 'benchmark command failed');
        reject(rejected);
        return;
      }

      const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
      latencies.push(elapsedMs);
      completed += 1;

      if (completed >= target) {
        resolve();
        return;
      }
      sendNext();
    });

    ws.once('error', reject);
    sendNext();
  });

  ws.close();
}

async function main() {
  const options = {
    command: readOption('command', DEFAULTS.command),
    ops: readNumber('ops', DEFAULTS.ops),
    clients: readNumber('clients', DEFAULTS.clients),
    pipeline: readNumber('pipeline', DEFAULTS.pipeline),
    payloadSize: readNumber('payload-size', DEFAULTS.payloadSize),
    keys: readNumber('keys', DEFAULTS.keys),
    ttl: readNumber('ttl', DEFAULTS.ttl),
    host: readOption('host', DEFAULTS.host),
    clientName: readOption('client-name', DEFAULTS.clientName),
    secretToken: readOption('secret-token', process.env.ECHO_SECRET_TOKEN || DEFAULTS.secretToken)
  };

  if (options.clients < 1) throw new Error('clients must be at least 1');
  if (options.pipeline < 1) throw new Error('pipeline must be at least 1');
  if (options.ops < 1) throw new Error('ops must be at least 1');
  if (options.keys < 1) throw new Error('keys must be at least 1');

  const server = createEchoServer({ host: options.host, port: 0, sweepInterval: 60_000 });
  await new Promise((resolve, reject) => {
    server.wss.once('listening', resolve);
    server.wss.once('error', reject);
  });

  const address = server.wss.address();
  const url = `ws://${options.host}:${address.port}`;
  const value = createPayload(options.payloadSize);
  const latencies = [];

  await preload(url, options, value);

  const startedAt = process.hrtime.bigint();
  await Promise.all(
    Array.from({ length: options.clients }, (_, index) => runClient(url, index, options, value, latencies))
  );
  const elapsedSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9;

  await new Promise((resolve) => server.close(resolve));

  latencies.sort((a, b) => a - b);
  const result = {
    command: options.command,
    ops: options.ops,
    clients: options.clients,
    pipeline: options.pipeline,
    payloadSize: options.payloadSize,
    keys: options.keys,
    seconds: elapsedSeconds,
    opsPerSecond: options.ops / elapsedSeconds,
    latencyMs: {
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99)
    }
  };

  console.log(`command=${result.command} ops=${formatNumber(result.ops)} clients=${result.clients} pipeline=${result.pipeline} payload=${result.payloadSize}B keys=${formatNumber(result.keys)}`);
  console.log(`throughput=${formatNumber(result.opsPerSecond)} ops/sec duration=${result.seconds.toFixed(2)}s`);
  console.log(`latency p50=${result.latencyMs.p50.toFixed(2)}ms p95=${result.latencyMs.p95.toFixed(2)}ms p99=${result.latencyMs.p99.toFixed(2)}ms`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
