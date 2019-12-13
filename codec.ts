import { Reader, Writer, Buffer, str2ab } from "./util.ts";
import { BufReader, BufWriter } from "https://deno.land/std/io/bufio.ts";
import { ErrorReply } from "./errors.ts";

const LR = "\r".charCodeAt(0);
const integerReply = ":".charCodeAt(0);
const bulkReply = "$".charCodeAt(0);
const statusReply = "+".charCodeAt(0);
const arrayReply = "*".charCodeAt(0);
const errorReply = "-".charCodeAt(0);
const Nil = "Nil";

export type RedisReply = string | number | string | any[] | ErrorReply;

export interface Codec {
  encode(command: string, args: (string | number)[]);
  encodeTo(writer: Writer, command: string, args: (string | number)[]);
  decode(reader: Reader): Promise<RedisReply>;
}

async function decodeStatus(reader: BufReader): Promise<RedisReply> {
  const [line] = await reader.readLine();
  const statusBytes = line.subarray(1, line.length);
  return new Buffer(statusBytes).toString();
}

async function decodeBulkReply(reader: BufReader): Promise<RedisReply> {
  const [line] = await reader.readLine();
  const bulkLenBytes = line.subarray(1, line.length);
  const bulkLenStr = new Buffer(bulkLenBytes).toString();
  const bulkLen = parseInt(bulkLenStr);

  if (isNaN(bulkLen)) {
    throw "unexpected protocol";
  }
  if (bulkLen < 0) {
    return Nil;
  }
  const data = new Uint8Array(bulkLen + 2);
  await reader.readFull(data);
  const bulkReplyBytes = data.subarray(0, data.length - 2);
  return new Buffer(bulkReplyBytes).toString();
}

async function decodeErrorReply(reader: BufReader): Promise<RedisReply> {

  const [line] = await reader.readLine();
  const bytes = line.subarray(1, line.length);
  return new Buffer(bytes).toString();
}

async function decodeIntegerReply(reader: BufReader): Promise<RedisReply> {
  const [line] = await reader.readLine();
  const integerBytes = line.subarray(1, line.length);
  const integerStr = new Buffer(integerBytes).toString();
  return parseInt(integerStr);
}

export async function decodeArrayReply(reader: BufReader): Promise<RedisReply> {
  const [line] = await reader.readLine();
  const multibulklenBytes = line.subarray(1, line.length);
  const multibulklenStr = new Buffer(multibulklenBytes).toString();
  let multibulklen = parseInt(multibulklenStr);
  const results: any[] = [];

  while (multibulklen > 0) {
    const [typ] = await reader.peek(1);
    switch (typ[0]) {
      case statusReply:
        results.push((await decodeStatus(reader)) as string);
        break;
      case bulkReply:
        const data = (await decodeBulkReply(reader)) as string;
        results.push(data);
        break;
      case integerReply:
        results.push((await decodeIntegerReply(reader)) as number);
        break;
      case arrayReply:
        results.push((await decodeArrayReply(reader)) as any[]);
        break;
      case errorReply:
        break;
      default:
        throw "unsupported";
    }
    multibulklen--;
  }
  return results;
}

class codecImpl {
  encode(command: string, args: (string | number)[]): string {
    let msg = `*${1 + args.length}\r\n$${command.length}\r\n${command}\r\n`;
    const encoder = new TextEncoder();

    for (const arg of args) {
      const val = String(arg);
      const bytesLen = encoder.encode(val).byteLength;
      msg += `$${bytesLen}\r\n`;
      msg += `${val}\r\n`;
    }
    return msg;
  }

  async encodeTo(writer: Writer, command: string, args: (string | number)[]) {
    const bufWriter = new BufWriter(writer);
    const msg = this.encode(command, args);
    const encoder = new TextEncoder();
    await bufWriter.write(encoder.encode(msg));
    await bufWriter.flush();
  }

  async decode(reader: Reader): Promise<RedisReply> {
    const bufReader = new BufReader(reader);

    const [typ] = await bufReader.peek(1);
    switch (typ[0]) {
      case statusReply:
        return await decodeStatus(bufReader);
      case bulkReply:
        return await decodeBulkReply(bufReader);
      case integerReply:
        return await decodeIntegerReply(bufReader);
      case arrayReply:
        return await decodeArrayReply(bufReader);

      case errorReply:

        const data = await decodeErrorReply(bufReader);
        return data
      default:
        throw "unsupported";
    }
  }
}

const codec = new codecImpl();

export { codec, Nil };
