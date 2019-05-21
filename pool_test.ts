import { runTests, test } from "https://deno.land/std/testing/mod.ts";
import { assertEquals } from "https://deno.land/std/testing/asserts.ts";
import { createPool, PoolImpl } from "./pool.ts";
import { Reader, Writer, Closer, Dialer, str2ab, Buffer } from "./util.ts";
import { MockDialer, MockConn } from "./mock.ts";

let redis = undefined;
const expectedData = "hello";
const expectedDataBytes = str2ab(expectedData);

test(async function testGetPut() {
  const conn = new MockConn();
  const dialer = MockDialer(conn);
  const pool = createPool(dialer) as PoolImpl;
  const gotConn = await pool.get();
  assertEquals(0, pool.idleConns.length);
  await gotConn.write(expectedDataBytes);
  pool.put(gotConn);
  assertEquals(1, pool.idleConns.length);
  assertEquals(new Buffer(conn.bytes).toString(), expectedData);
});
