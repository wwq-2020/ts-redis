import { Reader, Writer, str2ab, Buffer } from "./util.ts";
import { runTests, test } from "https://deno.land/std/testing/mod.ts";
import { assertEquals } from "https://deno.land/std/testing/asserts.ts";
import { MockConn } from "./mock.ts";
import { codec } from "./codec.ts";

const expectedCmd = "set";
const expectedCmdKey = "hello";
const expectedCmdVal = "world";
const args = [expectedCmdKey, expectedCmdVal];
const expectedMsg = `*3\r\n$3\r\nset\r\n$5\r\n${expectedCmdKey}\r\n$5\r\n${expectedCmdVal}\r\n`;
const expectedResp = "OK";
const resp = str2ab(`+${expectedResp}\r\n`);

test(async function testEncode() {
  const msg = codec.encode(expectedCmd, args);
  assertEquals(msg, expectedMsg);
});

test(async function testEncodeTo() {
  const mockConn = new MockConn();
  await codec.encodeTo(mockConn, expectedCmd, args);
  assertEquals(new Buffer(mockConn.bytes).toString(), expectedMsg);
});

test(async function testDecode() {
  const mockConn = new MockConn(resp);
  const gotResp = (await codec.decode(mockConn)) as string;
  assertEquals(expectedResp, gotResp);
});
