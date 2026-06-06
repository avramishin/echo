import { EventEmitter } from 'node:events';

export interface EchoClientOptions {
  timeout?: number;
}

export type EchoMessageHandler<TMessage = unknown> = (message: TMessage, channel: string) => void;

export default class EchoClient extends EventEmitter {
  constructor(url: string, options?: EchoClientOptions);

  readonly url: string;
  readonly timeout: number;

  connect(): Promise<this>;
  set<TValue = unknown>(key: string, value: TValue, ttl?: number): Promise<boolean>;
  get<TValue = unknown>(key: string): Promise<TValue | null>;
  delete(key: string): Promise<boolean>;
  setnx<TValue = unknown>(key: string, value: TValue, ttl?: number): Promise<boolean>;
  lock(key: string, ttl?: number, token?: string): Promise<string | false>;
  release(key: string, token: string): Promise<boolean>;
  publish<TMessage = unknown>(channel: string, message: TMessage): Promise<number>;
  subscribe<TMessage = unknown>(channel: string, handler?: EchoMessageHandler<TMessage>): Promise<boolean>;
  flush(): Promise<boolean>;
  list(prefix?: string): Promise<string[]>;
  close(code?: number, reason?: string): void;
}

export { EchoClient };
