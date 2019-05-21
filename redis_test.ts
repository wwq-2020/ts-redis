import { runTests, test } from "https://deno.land/std/testing/mod.ts";
import { assertEquals } from "https://deno.land/std/testing/asserts.ts";
import {
  Reader,
  Writer,
  Closer,
  Dialer,
  getDialer,
  Buffer,
  Auth,
  str2ab,
  Scanner
} from "./util.ts";
import { createEx } from "./redis.ts";
import { RedisReply } from "./codec.ts";
import { MockConn } from "./mock.ts";

class MockPool {
  constructor(readonly conn: Reader & Writer & Closer) {}
  async get(): Promise<Reader & Writer & Closer> {
    return this.conn;
  }

  put(conn: Reader & Writer & Closer) {
    poolPutCallCnt += 1;
  }
}

class MockCodec {
  constructor(readonly result: string) {}
  encode(command: string, args: (string | number)[]) {}

  async encodeTo(writer: Writer, command: string, args: (string | number)[]) {
    await writer.write(str2ab(command));
  }

  async decode(reader: Reader): Promise<RedisReply> {
    return this.result;
  }
}

let poolPutCallCnt = 0;
const expectedPoolPutCallCnt = 1;
const expectedResp = "+OK\r\n";
const codec = new MockCodec(expectedResp);

const expectedCmd = "set";
const expectedKey = "hello";
const expectedVal = "world";

test(async function testCreateEx() {
  const mockConn = new MockConn();
  const pool = new MockPool(mockConn);

  const redis = createEx(pool, codec);
  const gotResp = await redis.set(expectedKey, expectedVal);
  assertEquals(expectedResp, gotResp);
});
