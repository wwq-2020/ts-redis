import { Reader, Writer, Closer, Dialer, Auth } from "./util.ts";

export interface Pool {
  get(): Promise<Reader & Writer & Closer>;
  put(conn: Reader & Writer & Closer);
}

class Conn {
  constructor(readonly conn: Reader & Writer & Closer, public authed = true) {}
}

export class PoolImpl {
  idleConns: any[] = [];

  constructor(readonly dialer: Dialer, readonly auth?: Auth) {}

  async get(): Promise<Reader & Writer & Closer> {
    if (!this.idleConns || this.idleConns.length == 0) {
      const conn = await this.dialer();
      await this.authIfNeeded(conn).catch(err => {
        conn.close();
        throw err;
      });

      return conn;
    }
    return this.idleConns.pop();
  }

  async authIfNeeded(conn: Reader & Writer & Closer): Promise<void> {
    if (this.auth) {
      const result = await this.auth(conn);
      if (!result) {
        throw "auth err";
      }
    }
  }

  put(conn: Reader & Writer & Closer) {
    this.idleConns.push(conn);
  }
}

export function createPool(dialer: Dialer, auth?: Auth): Pool {
  return new PoolImpl(dialer, auth);
}
