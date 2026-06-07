export interface EchoServerOptions {
  port?: number;
  host?: string;
  sweepInterval?: number;
  secretToken?: string;
}

export interface EchoServerInstance {
  wss: unknown;
  close(callback?: (error?: Error) => void): void;
}

export function createEchoServer(options?: EchoServerOptions): EchoServerInstance;
