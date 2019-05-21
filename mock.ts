import {
  Reader,
  Writer,
  Closer,
  Dialer,
  getDialer,
  ReadResult,
  Buffer
} from "./util.ts";

export class MockConn {
  constructor(public bytes?: Uint8Array) {}

  async write(p: Uint8Array): Promise<number> {
    this.bytes = p;
    return p.length;
  }

  async read(p: Uint8Array): Promise<ReadResult> {
    p.set(this.bytes, 0);
    return { eof: false, nread: this.bytes.length };
  }
  close() {}
}

export function MockDialer(conn: Reader & Writer & Closer): Dialer {
  return async function(): Promise<Reader & Writer & Closer> {
    return conn;
  };
}

export function mockDialer(conn: Reader & Writer & Closer): Dialer {
  return async function(): Promise<Reader & Writer & Closer> {
    return conn;
  };
}
