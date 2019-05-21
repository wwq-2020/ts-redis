import { createPool, Pool } from "./pool.ts";
import { codec, Codec, RedisReply } from "./codec.ts";

import {
  Reader,
  Writer,
  Closer,
  Dialer,
  getDialer,
  Buffer,
  Auth,
  Scanner
} from "./util.ts";

interface migrateOptions {
  copy?: boolean;
  replace?: boolean;
  keys?: (string | number)[];
}

interface sortLimitOptions {
  offset: number;
  count: number;
}

interface scanOptions {
  pattern?: string;
  count?: number;
}

interface bitfieldGetOptions {
  type: string;
  offset: number;
}

interface bitfieldSetOptions {
  type: string;
  offset: number;
  value: number;
}

interface bitfieldIncrbyOptions {
  type: string;
  offset: number;
  increment: number;
}

interface bitfieldOptions {
  get?: bitfieldGetOptions;
  set?: bitfieldSetOptions;
  incrby?: bitfieldIncrbyOptions;
  overflow?: string;
}

interface sortOptions {
  by?: string;
  limit?: sortLimitOptions;
  get?: string[];
  order?: string;
  alpha?: boolean;
  destination?: number;
}

interface zaddOptions {
  exist: "nx" | "xx";
  ch: boolean;
  incr: boolean;
}

interface zaddItem {
  member: string | number;
  score: number;
}

interface zinterunionstoreOptions {
  weights: number | number[];
  aggregate: string;
}

interface zrangebylexOptions {
  offset: number;
  count: number;
}

interface zrangebyscoreOptions {
  offset: number;
  count: number;
  withscores?: boolean;
}

interface geoItem {
  longitude: number;
  latitude: number;
  member: number;
}
//[WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [AS

interface georadiusOptions {
  withcoord: boolean;
  withdist: boolean;
  withhash: boolean;
  count: number;
  order: string;
  store: string;
  storedist: string;
}

interface RedisCommand {
  command: string;
  args?: (string | number)[];
}

interface setOptions {
  ex?: number;
  px?: number;
  exist: string;
}

interface roundtrip {
  (command: string, args: (string | number)[]): Promise<RedisReply>;
}

interface startEndOptions {
  start: number;
  end: number;
}

class ScannerImpl {
  private hasNext: boolean;
  private prevPos: number;
  private err: Error;
  constructor(
    private roundtrip: roundtrip,
    private cmd: string,
    private args: (number | string)[]
  ) {}

  Next(): boolean {
    return !this.err && this.hasNext;
  }

  async Scan(): Promise<string[]> {
    if (this.err) {
      return;
    }
    let args: (number | string)[] = [this.prevPos];
    args.push(...this.args);
    const resp = (await this.roundtrip(this.cmd, args).catch(err => {
      this.err = err;
    })) as string[];
    if (this.err) {
      return;
    }
    this.prevPos = parseInt(resp[0]);
    if (isNaN(this.prevPos)) {
      throw "unexpected protocol";
    }
    if (this.prevPos == 0) {
      this.hasNext = false;
    }
    return resp.slice(1);
  }

  Err(): Error {
    return this.err;
  }
}

class RedisBase {
  constructor(readonly pool: Pool, readonly codec: Codec) {}

  protected async roundtrip(
    command: string,
    args: (string | number)[]
  ): Promise<RedisReply> {
    const conn = await this.pool.get();

    return await this.roundtripEx(conn, command, args);
  }

  protected async sendCommand(
    conn: Reader & Writer & Closer,
    command: string,
    args: (string | number)[]
  ) {
    await this.codec.encodeTo(conn, command, args).catch(err => {
      conn.close();
      throw err;
    });
  }

  protected async readResp(
    conn: Reader & Writer & Closer
  ): Promise<RedisReply> {
    return await this.codec.decode(conn).catch(err => {
      conn.close();
      throw err;
    });
  }

  protected async roundtripEx(
    conn: Reader & Writer & Closer,
    command: string,
    args: (string | number)[]
  ): Promise<RedisReply> {
    await this.sendCommand(conn, command, args);

    const resp = await this.readResp(conn);

    this.pool.put(conn);
    return resp;
  }

  async set(
    key: string | number,
    val: string | number,
    options?: setOptions
  ): Promise<string> {
    let args = [key, val];
    if (options) {
      if (options.ex) {
        args.push("ex", options.ex);
      }
      if (options.px) {
        args.push("px", options.px);
      }

      if (options.exist) {
        args.push(options.exist);
      }
    }
    return (await this.roundtrip("set", args)) as string;
  }

  async append(key: string | number, val: string | number): Promise<number> {
    const args = [key, val];
    return (await this.roundtrip("append", args)) as number;
  }

  async bitcount(
    key: string | number,
    options?: startEndOptions
  ): Promise<number> {
    const args = [key];
    if (options) {
      args.push(options.start);
      args.push(options.end);
    }
    return (await this.roundtrip("bitcount", args)) as number;
  }

  async get(key: string | number): Promise<string> {
    const args = [key];
    return (await this.roundtrip("get", args)) as string;
  }

  async del(...keys: (string | number)[]): Promise<number> {
    return (await this.roundtrip("del", keys)) as number;
  }

  async hdel(
    key: string | number,
    ...fields: (string | number)[]
  ): Promise<number> {
    const args = [key, ...fields];
    return (await this.roundtrip("hdel", args)) as number;
  }

  async hget(key: string | number, field: string | number): Promise<string> {
    const args = [key, field];
    return (await this.roundtrip("hget", args)) as string;
  }

  async hgetall(key: string | number): Promise<string[]> {
    const args = [key];
    return (await this.roundtrip("hgetall", args)) as string[];
  }

  async hincrby(
    key: string | number,
    field: string | number,
    increment: number
  ): Promise<number> {
    const args = [key, field, increment];
    return (await this.roundtrip("hincrby", args)) as number;
  }

  async hincrbyfloat(
    key: string | number,
    field: string | number,
    increment: number
  ): Promise<string> {
    const args = [key, field, increment];
    return (await this.roundtrip("hincrbyfloat", args)) as string;
  }

  async hexists(key: string | number, field: string | number): Promise<number> {
    const args = [key, field];
    return (await this.roundtrip("hexists", args)) as number;
  }

  async hkeys(key: string | number): Promise<string[]> {
    const args = [key];
    return (await this.roundtrip("hkeys", args)) as string[];
  }

  async hvals(key: string | number): Promise<string[]> {
    const args = [key];
    return (await this.roundtrip("hvals", args)) as string[];
  }

  async hlen(key: string | number): Promise<number> {
    const args = [key];
    return (await this.roundtrip("hlen", args)) as number;
  }

  async hscan(
    key: string | number,
    cursor: number,
    options?: scanOptions
  ): Promise<any[]> {
    let args: (number | string)[] = [key, cursor];

    if (!options) {
      return (await this.roundtrip("hscan", args)) as any[];
    }

    if (options.pattern) {
      args.push("match", options.pattern);
    }

    if (options.count) {
      args.push("count", options.count);
    }

    return (await this.roundtrip("hscan", args)) as any[];
  }

  async geoadd(key: string | number, ...items: geoItem[]): Promise<number> {
    let args: (number | string)[] = [key];
    for (let item of items) {
      args.push(item.longitude, item.latitude, item.member);
    }
    return (await this.roundtrip("geoadd", args)) as number;
  }

  async geopos(key: string | number, ...members: string[]): Promise<string[]> {
    let args = [key, ...members];
    return (await this.roundtrip("geopos", args)) as string[];
  }

  async geohash(key: string | number, ...members: string[]): Promise<string[]> {
    let args = [key, ...members];
    return (await this.roundtrip("geohash", args)) as string[];
  }

  async geodist(
    key: string | number,
    member1: string,
    member2: string,
    unit?: string
  ): Promise<string> {
    let args = [key, member1, member2];
    if (unit) {
      args.push("unit");
    }
    return (await this.roundtrip("geodist", args)) as string;
  }

  async georadius(
    key: string | number,
    longitude: number,
    latitude: number,
    radius: number,
    unit: string,
    options?: georadiusOptions
  ): Promise<string[]> {
    let args = [key, longitude, latitude, radius, unit];
    if (options) {
      if (options.withcoord) {
        args.push("withcoord");
      }
      if (options.withdist) {
        args.push("withdist");
      }
      if (options.withhash) {
        args.push("withhash");
      }
      if (options.count) {
        args.push("count", options.count);
      }
      if (options.order) {
        args.push(options.order);
      }
      if (options.store) {
        args.push("store", options.store);
      }
      if (options.storedist) {
        args.push("storedist", options.storedist);
      }
    }
    return (await this.roundtrip("georadius", args)) as string[];
  }

  async georadiusbymember(
    key: string | number,
    member: string,
    radius: number,
    unit: string,
    options?: georadiusOptions
  ): Promise<string[]> {
    let args = [key, member, radius, unit];
    if (options) {
      if (options.withcoord) {
        args.push("withcoord");
      }
      if (options.withdist) {
        args.push("withdist");
      }
      if (options.withhash) {
        args.push("withhash");
      }
      if (options.count) {
        args.push("count", options.count);
      }
      if (options.order) {
        args.push(options.order);
      }
      if (options.store) {
        args.push("store", options.store);
      }
      if (options.storedist) {
        args.push("storedist", options.storedist);
      }
    }
    return (await this.roundtrip("georadiusbymember", args)) as string[];
  }

  async pfadd(
    key: string | number,
    ...elements: (string | number)[]
  ): Promise<string[]> {
    let args = [key, ...elements];
    return (await this.roundtrip("pfadd", args)) as string[];
  }

  async pfcount(...keys: (string | number)[]): Promise<number> {
    let args = [...keys];
    return (await this.roundtrip("pfcount", args)) as number;
  }

  async pfmerge(
    destkey: string | number,
    ...sourcekeys: (string | number)[]
  ): Promise<string> {
    let args = [destkey, ...sourcekeys];
    return (await this.roundtrip("pfmerge", args)) as string;
  }

  async hmget(
    key: string | number,
    ...fields: (string | number)[]
  ): Promise<string[]> {
    const args = [key, ...fields];
    return (await this.roundtrip("hmget", args)) as string[];
  }

  async hmset(
    key: string | number,
    ...fieldvalues: (string | number)[]
  ): Promise<string[]> {
    if (fieldvalues.length % 2 != 0) {
      throw "unmatched fieldvalues pair";
    }

    const args = [key, ...fieldvalues];
    return (await this.roundtrip("hmset", args)) as string[];
  }

  async hset(
    key: string | number,
    field: string | number,
    value: string | number
  ): Promise<number> {
    const args = [key, field, value];
    return (await this.roundtrip("hset", args)) as number;
  }

  async hstrlen(key: string | number, field: string | number): Promise<number> {
    const args = [key, field];
    return (await this.roundtrip("hstrlen", args)) as number;
  }

  async hsetnx(
    key: string | number,
    field: string | number,
    value: string | number
  ): Promise<string> {
    const args = [key, field, value];
    return (await this.roundtrip("hsetnx", args)) as string;
  }

  async blpop(
    keys: string | number | (string | number)[],
    timeout: number
  ): Promise<string[]> {
    let args: (number | string)[] = [];
    if (typeof keys == "string" || typeof keys == "number") {
      args = [keys, timeout];
    } else {
      args = [...keys, timeout];
    }
    return (await this.roundtrip("blpop", args)) as string[];
  }

  async brpop(
    keys: string | number | (string | number)[],
    timeout: number
  ): Promise<string[]> {
    let args: (number | string)[] = [];
    if (typeof keys == "string" || typeof keys == "number") {
      args = [keys, timeout];
    } else {
      args = [...keys, timeout];
    }
    return (await this.roundtrip("brpop", args)) as string[];
  }

  async lpop(key: string | number): Promise<string> {
    let args = [key];
    return (await this.roundtrip("lpop", args)) as string;
  }

  async rpop(key: string | number): Promise<string> {
    let args = [key];
    return (await this.roundtrip("rpop", args)) as string;
  }

  async lpush(
    key: string | number,
    ...value: (string | number)[]
  ): Promise<number> {
    let args = [key, ...value];
    return (await this.roundtrip("lpush", args)) as number;
  }

  async rpush(
    key: string | number,
    ...value: (string | number)[]
  ): Promise<number> {
    let args = [key, ...value];
    return (await this.roundtrip("rpush", args)) as number;
  }

  async sadd(
    key: string | number,
    ...value: (string | number)[]
  ): Promise<number> {
    let args = [key, ...value];
    return (await this.roundtrip("sadd", args)) as number;
  }

  async scard(key: string | number): Promise<number> {
    let args = [key];
    return (await this.roundtrip("scard", args)) as number;
  }

  async zcard(key: string | number): Promise<number> {
    let args = [key];
    return (await this.roundtrip("zcard", args)) as number;
  }

  async zrangebylex(
    key: string | number,
    min: string,
    max: string,
    options?: zrangebylexOptions
  ): Promise<(string | number)[]> {
    let args = [key, min, max];
    if (options) {
      args.push("limit", options.offset, options.count);
    }
    return (await this.roundtrip("zrangebylex", args)) as (string | number)[];
  }

  async zremrangebylex(
    key: string | number,
    min: string,
    max: string
  ): Promise<number> {
    let args = [key, min, max];
    return (await this.roundtrip("zremrangebylex", args)) as number;
  }

  async zremrangebyscore(
    key: string | number,
    min: number,
    max: number
  ): Promise<number> {
    let args = [key, min, max];
    return (await this.roundtrip("zremrangebyscore", args)) as number;
  }

  async zremrangebyrank(
    key: string | number,
    start: number,
    stop: number
  ): Promise<number> {
    let args = [key, start, stop];
    return (await this.roundtrip("zremrangebyrank", args)) as number;
  }

  async zrevrangebylex(
    key: string | number,
    max: string,
    min: string,
    options?: zrangebylexOptions
  ): Promise<(string | number)[]> {
    let args = [key, max, min];
    if (options) {
      args.push("limit", options.offset, options.count);
    }
    return (await this.roundtrip("zrevrangebylex", args)) as (
      | string
      | number)[];
  }

  async zrangebyscore(
    key: string | number,
    min: number,
    max: number,
    options: zrangebyscoreOptions
  ): Promise<(string | number)[]> {
    let args = [key, min, max];
    if (options) {
      args.push("limit", options.offset, options.count);
      if (options.withscores) {
        args.push("withscores");
      }
    }
    return (await this.roundtrip("zrangebyscore", args)) as (string | number)[];
  }

  async zrevrangebyscore(
    key: string | number,
    max: number,
    min: number,
    options: zrangebyscoreOptions
  ): Promise<(string | number)[]> {
    let args = [key, max, min];
    if (options) {
      args.push("limit", options.offset, options.count);
      if (options.withscores) {
        args.push("withscores");
      }
    }
    return (await this.roundtrip("zrevrangebyscore", args)) as (
      | string
      | number)[];
  }

  async zrank(key: string | number, member: string | number): Promise<number> {
    const args = [key, member];
    return (await this.roundtrip("zrank", args)) as number;
  }

  async zrevrank(
    key: string | number,
    member: string | number
  ): Promise<number> {
    const args = [key, member];
    return (await this.roundtrip("zrevrank", args)) as number;
  }

  async zscore(key: string | number, member: string | number): Promise<string> {
    const args = [key, member];
    return (await this.roundtrip("zscore", args)) as string;
  }

  async zscan(
    key: string | number,
    cursor: number,
    options?: scanOptions
  ): Promise<any[]> {
    let args: (number | string)[] = [key, cursor];
    if (!options) {
      return (await this.roundtrip("zscan", args)) as any[];
    }
    if (options.pattern) {
      args.push("match", options.pattern);
    }
    if (options.count) {
      args.push("count", options.count);
    }
    return (await this.roundtrip("zscan", args)) as any[];
  }

  async zrem(
    key: string | number,
    ...members: (string | number)[]
  ): Promise<number> {
    const args = [key, ...members];
    return (await this.roundtrip("zrem", args)) as number;
  }

  async zadd(
    key: string | number,
    items: zaddItem[],
    options?: zaddOptions
  ): Promise<number> {
    let args: (number | string)[] = [key];

    if (options) {
      if (options.exist) {
        args.push(options.exist);
      }
      if (options.ch) {
        args.push("ch");
      }
      if (options.incr) {
        args.push("incr");
      }
    }
    for (let item of items) {
      args.push(item.score, item.member);
    }
    return (await this.roundtrip("zadd", args)) as number;
  }

  async zcount(
    key: string | number,
    min: number,
    max: number
  ): Promise<number> {
    let args = [key, min, max];
    return (await this.roundtrip("zcount", args)) as number;
  }

  async zlexcount(
    key: string | number,
    min: string,
    max: string
  ): Promise<number> {
    let args = [key, min, max];
    return (await this.roundtrip("zlexcount", args)) as number;
  }

  async zincrby(
    key: string | number,
    increment: number,
    member: string | number
  ): Promise<string> {
    let args = [key, increment, member];
    return (await this.roundtrip("zincrby", args)) as string;
  }

  async zinterstore(
    destination: string | number,
    numkeys: number,
    keys: string | number | (string | number)[],
    options?: zinterunionstoreOptions
  ): Promise<number> {
    let args = [destination, numkeys];

    if (typeof keys == "string" || typeof keys == "number") {
      args.push(keys);
    } else {
      args.push(...keys);
    }

    if (options) {
      if (options.weights) {
        if (typeof options.weights == "number") {
          args.push(options.weights);
        } else {
          args.push(...options.weights);
        }
      }
      if (options.aggregate) {
        args.push(options.aggregate);
      }
    }
    return (await this.roundtrip("zinterstore", args)) as number;
  }

  async zunionstore(
    destination: string | number,
    numkeys: number,
    keys: string | number | (string | number)[],
    options?: zinterunionstoreOptions
  ): Promise<number> {
    let args = [destination, numkeys];
    if (typeof keys == "string" || typeof keys == "number") {
      args.push(keys);
    } else {
      args.push(...keys);
    }
    if (options) {
      if (options.weights) {
        if (typeof options.weights == "number") {
          args.push(options.weights);
        } else {
          args.push(...options.weights);
        }
      }
      if (options.aggregate) {
        args.push(options.aggregate);
      }
    }
    return (await this.roundtrip("zunionstore", args)) as number;
  }

  async sdiff(
    key: string | number,
    ...keys: (string | number)[]
  ): Promise<string[]> {
    let args = [key, ...keys];
    return (await this.roundtrip("sdiff", args)) as string[];
  }

  async zpopmax(
    key: string | number,
    count?: number
  ): Promise<(string | number)[]> {
    let args = [key];
    if (count) {
      args.push(count);
    }
    return (await this.roundtrip("zpopmax", args)) as (string | number)[];
  }

  async zpopmin(
    key: string | number,
    count?: number
  ): Promise<(string | number)[]> {
    let args = [key];
    if (count) {
      args.push(count);
    }
    return (await this.roundtrip("zpopmin", args)) as (string | number)[];
  }

  async zrange(
    key: string | number,
    start: number,
    stop: number,
    withscores?: boolean
  ): Promise<(string | number)[]> {
    let args = [key, start, stop];
    if (withscores) {
      args.push("withscores");
    }

    return (await this.roundtrip("zrange", args)) as (string | number)[];
  }

  async zrevrange(
    key: string | number,
    start: number,
    stop: number,
    withscores?: boolean
  ): Promise<(string | number)[]> {
    let args = [key, start, stop];
    if (withscores) {
      args.push("withscores");
    }

    return (await this.roundtrip("zrevrange", args)) as (string | number)[];
  }

  async sdiffstore(
    destination: string | number,
    ...keys: (string | number)[]
  ): Promise<string[]> {
    let args = [destination, ...keys];
    return (await this.roundtrip("sdiffstore", args)) as string[];
  }

  async sunion(
    key: string | number,
    ...keys: (string | number)[]
  ): Promise<string[]> {
    let args = [key, ...keys];
    return (await this.roundtrip("sunion", args)) as string[];
  }

  async sunionstore(
    destination: string | number,
    ...keys: (string | number)[]
  ): Promise<string[]> {
    let args = [destination, ...keys];
    return (await this.roundtrip("sunionstore", args)) as string[];
  }

  async sinter(
    key: string | number,
    ...keys: (string | number)[]
  ): Promise<string[]> {
    let args = [key, ...keys];
    return (await this.roundtrip("sinter", args)) as string[];
  }

  async sinterstore(
    destination: string | number,
    ...keys: (string | number)[]
  ): Promise<string[]> {
    let args = [destination, ...keys];
    return (await this.roundtrip("sinterstore", args)) as string[];
  }

  async sismember(
    key: string | number,
    member: string | number
  ): Promise<number> {
    let args = [key, member];
    return (await this.roundtrip("sismember", args)) as number;
  }

  async smembers(key: string | number): Promise<string[]> {
    let args = [key];
    return (await this.roundtrip("smembers", args)) as string[];
  }

  async smove(
    source: string | number,
    destination: string | number,
    member: string | number
  ): Promise<number> {
    let args = [source, destination, member];
    return (await this.roundtrip("smove", args)) as number;
  }

  async spop(key: string | number, count?: number): Promise<string[]> {
    let args = [key];
    if (count) {
      args.push(count);
    }
    return (await this.roundtrip("spop", args)) as string[];
  }

  async srandmember(key: string | number, count: number): Promise<string[]> {
    const args = [key, count];
    return (await this.roundtrip("srandmember", args)) as string[];
  }

  async srem(
    key: string | number,
    member: string | number,
    ...members: (string | number)[]
  ): Promise<number> {
    const args = [key, member, ...members];
    return (await this.roundtrip("srem", args)) as number;
  }

  async bzpopmin(keys: string | number | (string | number)[], timeout: number) {
    let args: (number | string)[] = [];
    if (typeof keys == "string" || typeof keys == "number") {
      args = [keys, timeout];
    } else {
      args = [...keys, timeout];
    }
    return (await this.roundtrip("bzpopmin", args)) as string[];
  }

  async bzpopmax(keys: string | number | (string | number)[], timeout: number) {
    let args: (number | string)[] = [];
    if (typeof keys == "string" || typeof keys == "number") {
      args = [keys, timeout];
    } else {
      args = [...keys, timeout];
    }
    return (await this.roundtrip("bzpopmax", args)) as string[];
  }

  async lpushx(key: string | number, value: string | number): Promise<number> {
    const args = [key, value];
    return (await this.roundtrip("lpushx", args)) as number;
  }

  async lrange(
    key: string | number,
    start: number,
    stop: number
  ): Promise<string[]> {
    let args = [key, start, stop];
    return (await this.roundtrip("lrange", args)) as string[];
  }

  async lrem(
    key: string | number,
    count: number,
    value: string | number
  ): Promise<number> {
    let args = [key, count, value];
    return (await this.roundtrip("lrem", args)) as number;
  }

  async lset(
    key: string | number,
    index: number,
    value: number
  ): Promise<number> {
    let args = [key, index, value];
    return (await this.roundtrip("lset", args)) as number;
  }

  async ltrim(
    key: string | number,
    start: number,
    stop: number
  ): Promise<string[]> {
    let args = [key, start, stop];
    return (await this.roundtrip("ltrim", args)) as string[];
  }

  async rpoplpush(
    source: string | number,
    destination: string | number
  ): Promise<string> {
    let args = [source, destination];
    return (await this.roundtrip("rpoplpush", args)) as string;
  }

  async rpushx(key: string | number, value: string | number): Promise<number> {
    let args = [key, value];
    return (await this.roundtrip("rpushx", args)) as number;
  }

  async brpoplpush(
    source: string | number,
    destination: string | number,
    timeout: number
  ): Promise<string> {
    let args = [source, destination, timeout];
    return (await this.roundtrip("brpoplpush", args)) as string;
  }

  async lindex(key: string | number, index: number): Promise<string> {
    let args = [key, index];
    return (await this.roundtrip("lindex", args)) as string;
  }

  async linsert(
    key: string | number,
    pos: "before" | "after",
    pivot: string | number,
    value: string | number
  ): Promise<number> {
    let args = [key, pos, pivot, value];
    return (await this.roundtrip("linsert", args)) as number;
  }

  async llen(key: string | number): Promise<number> {
    let args = [key];
    return (await this.roundtrip("llen", args)) as number;
  }

  async bitfield(
    key: string | number,
    options: bitfieldOptions
  ): Promise<string[]> {
    let args: (number | string)[] = [key];
    if (!options) {
      return (await this.roundtrip("bitfield", args)) as string[];
    }
    const getOptions = options.get;
    if (getOptions) {
      args.push("get", getOptions.type, getOptions.offset);
    }
    const setOptions = options.set;
    if (setOptions) {
      args.push("set", setOptions.type, setOptions.offset, setOptions.value);
    }
    const incrbyOptions = options.incrby;
    if (incrbyOptions) {
      args.push(
        "incrby",
        incrbyOptions.type,
        incrbyOptions.offset,
        incrbyOptions.increment
      );
    }
    if (options.overflow) {
      args.push("overflow", options.overflow);
    }
    return (await this.roundtrip("bitfield", args)) as string[];
  }

  async bitop(
    operation: string,
    destkey: string | number,
    ...keys: (string | number)[]
  ): Promise<number> {
    const args = [operation, destkey, ...keys];
    return (await this.roundtrip("bitop", args)) as number;
  }

  async incr(key: string | number): Promise<number> {
    const args = [key];
    return (await this.roundtrip("incr", args)) as number;
  }

  async incrby(key: string | number, increment: number): Promise<number> {
    const args = [key, increment];
    return (await this.roundtrip("incrby", args)) as number;
  }

  async decr(key: string | number): Promise<number> {
    const args = [key];
    return (await this.roundtrip("decr", args)) as number;
  }

  async getbit(key: string | number, offset: number): Promise<number> {
    const args = [key, offset];
    return (await this.roundtrip("getbit", args)) as number;
  }

  async decrby(key: string | number, increment: number): Promise<number> {
    const args = [key, increment];
    return (await this.roundtrip("decrby", args)) as number;
  }

  async setrange(
    key: string | number,
    offset: number,
    value: string | number
  ): Promise<number> {
    const args = [key, offset, value];
    return (await this.roundtrip("setrange", args)) as number;
  }

  async getrange(
    key: string | number,
    start: number,
    end: number
  ): Promise<string> {
    const args = [key, start, end];
    return (await this.roundtrip("getrange", args)) as string;
  }

  async setnx(key: string | number, val: string | number): Promise<number> {
    const args = [key, val];
    return (await this.roundtrip("setnx", args)) as number;
  }

  async setex(
    key: string | number,
    seconds: number,
    val: string | number
  ): Promise<string> {
    const args = [key, seconds, val];
    return (await this.roundtrip("setex", args)) as string;
  }

  async setbit(
    key: string | number,
    offset: number,
    value: number
  ): Promise<number> {
    const args = [key, offset, value];
    return (await this.roundtrip("setbit", args)) as number;
  }

  async psetex(
    key: string | number,
    milliseconds: number,
    val: string | number
  ): Promise<string> {
    const args = [key, milliseconds, val];
    return (await this.roundtrip("psetex", args)) as string;
  }

  async msetnx(...kvs: (string | number)[]): Promise<number> {
    if (kvs.length % 2 != 0) {
      throw "unmatched kv pair";
    }
    return (await this.roundtrip("msetnx", kvs)) as number;
  }

  async strlen(key: string | number): Promise<number> {
    const args = [key];
    return (await this.roundtrip("strlen", args)) as number;
  }

  async incrbyfloat(key: string | number, increment: number): Promise<number> {
    const args = [key, increment];
    return (await this.roundtrip("incrbyfloat", args)) as number;
  }

  async bitpos(key: string | number, bit: number, options?: startEndOptions) {
    const args = [key, bit];
    if (options) {
      args.push(options.start);
      args.push(options.end);
    }
    return (await this.roundtrip("bitpos", args)) as number;
  }

  async getset(key: string | number, val: string | number): Promise<string> {
    const args = [key, val];
    return (await this.roundtrip("getset", args)) as string;
  }

  scanner(cmd: "scan" | "zscan" | "hscan", options?: scanOptions): Scanner {
    let args: (number | string)[] = [];

    if (options.pattern) {
      args.push("match", options.pattern);
    }
    if (options.count) {
      args.push("count", options.count);
    }
    return new ScannerImpl(this.roundtrip, cmd, args);
  }

  async scan(cursor: number, options?: scanOptions): Promise<string[]> {
    let args: (number | string)[] = [cursor];
    if (!options) {
      return (await this.roundtrip("scan", args)) as string[];
    }
    if (options.pattern) {
      args.push("match", options.pattern);
    }
    if (options.count) {
      args.push("count", options.count);
    }
    return (await this.roundtrip("scan", args)) as string[];
  }

  async wait(numreplicas: number, timeout: number): Promise<number> {
    const args = [numreplicas, timeout];
    return (await this.roundtrip("wait", args)) as number;
  }

  async unlink(...keys: (string | number)[]): Promise<number> {
    return (await this.roundtrip("unlink", keys)) as number;
  }

  async mget(...keys: (string | number)[]): Promise<string[]> {
    return (await this.roundtrip("mget", keys)) as string[];
  }

  async mset(...kvs: (string | number)[]): Promise<string> {
    if (kvs.length % 2 != 0) {
      throw "unmatched kv pair";
    }
    return (await this.roundtrip("mset", kvs)) as string;
  }

  async dump(key: string | number): Promise<string> {
    const args = [key];
    return (await this.roundtrip("dump", args)) as string;
  }

  async exists(...keys: (string | number)[]): Promise<number> {
    return (await this.roundtrip("exists", keys)) as number;
  }

  async expire(key: string | number, seconds: number): Promise<number> {
    const args = [key, seconds];
    return (await this.roundtrip("expire", args)) as number;
  }

  async pexpire(key: string | number, milliseconds: number): Promise<number> {
    const args = [key, milliseconds];
    return (await this.roundtrip("pexpire", args)) as number;
  }

  async expireAt(key: string | number, timestamp: number): Promise<number> {
    const args = [key, timestamp];
    return (await this.roundtrip("expireat", args)) as number;
  }

  async pexpireAt(key: string | number, timestamp: number): Promise<number> {
    const args = [key, timestamp];
    return (await this.roundtrip("pexpireat", args)) as number;
  }

  async randomKey(): Promise<string> {
    const args = [];
    return (await this.roundtrip("randomkey", args)) as string;
  }

  async ttl(key: string | number): Promise<number> {
    const args = [key];
    return (await this.roundtrip("ttl", args)) as number;
  }

  async rename(src: string | number, dst: string | number): Promise<string> {
    const args = [src, dst];
    return (await this.roundtrip("rename", args)) as string;
  }

  async renamenx(src: string | number, dst: string | number): Promise<number> {
    const args = [src, dst];
    return (await this.roundtrip("renamenx", args)) as number;
  }

  async pttl(key: string | number): Promise<number> {
    const args = [key];
    return (await this.roundtrip("pttl", args)) as number;
  }
  async restore(
    key: string | number,
    ttl: number,
    serialized_value: string,
    replace?: boolean
  ): Promise<string> {
    let args = [key, ttl, serialized_value];
    if (replace) {
      args.push("replace");
    }
    return (await this.roundtrip("restore", args)) as string;
  }

  async sort(key: string, options: sortOptions): Promise<number | any[]> {
    let args: (string | number)[] = [key];
    if (!options) {
      return (await this.roundtrip("sort", args)) as string[];
    }
    if (options.by) {
      args.push("by", options.by);
    }
    if (options.limit)
      args.push("limit", options.limit.offset, options.limit.count);

    if (options.get) {
      options.get.forEach(each => {
        args.push("get", each);
      });
    }
    if (options.order) {
      args.push(options.order);
    }
    if (options.alpha) {
      args.push("alpha");
    }
    if (options.destination) {
      args.push("store", options.destination);
      return (await this.roundtrip("sort", args)) as number;
    }
    return (await this.roundtrip("sort", args)) as string[];
  }
  async keys(...keys: (string | number)[]): Promise<string[]> {
    return (await this.roundtrip("keys", keys)) as string[];
  }

  async auth(password: string | number): Promise<string> {
    const args = [password];
    return (await this.roundtrip("auth", args)) as string;
  }

  async echo(key: string | number): Promise<string> {
    const args = [key];
    return (await this.roundtrip("echo", args)) as string;
  }

  async ping(): Promise<string> {
    const args = [];
    return (await this.roundtrip("ping", args)) as string;
  }

  async migrate(
    host: string,
    port: number,
    destination_db: number,
    timeout: number,
    opts?: migrateOptions
  ): Promise<string> {
    let args = [host, port, destination_db, timeout];
    if (!opts) {
      return (await this.roundtrip("migrate", args)) as string;
    }
    if (opts.copy) {
      args.push("copy");
    }
    if (opts.replace) {
      args.push("replace");
    }
    if (opts.keys) {
      args.push(...opts.keys);
    }
    return (await this.roundtrip("migrate", args)) as string;
  }

  async move(key: string | number, db: number): Promise<string> {
    const args = [key, db];
    return (await this.roundtrip("move", args)) as string;
  }

  async persist(key: string | number): Promise<string> {
    const args = [key];
    return (await this.roundtrip("persist", args)) as string;
  }

  async publish(channel: string | number, message: string | number) {
    const args = [channel, message];
    return (await this.roundtrip("publish", args)) as string;
  }

  async channels(...args: (string | number)[]): Promise<string[]> {
    const argsdup = ["channels", ...args];
    return (await this.roundtrip("pubsub", argsdup)) as string[];
  }

  async numsub(...args: (string | number)[]): Promise<number> {
    const argsdup = ["numsub", ...args];

    return (await this.roundtrip("pubsub", argsdup)) as number;
  }
  async numpat(...args: (string | number)[]): Promise<number> {
    const argsdup = ["numpat", ...args];

    return (await this.roundtrip("pubsub", argsdup)) as number;
  }

  async eval(
    script: string,
    keys: (string | number)[],
    args: (string | number)[]
  ): Promise<string> {
    const argsdup: (string | number)[] = [
      script,
      keys.length,
      ...keys,
      ...args
    ];
    return (await this.roundtrip("eval", argsdup)) as string;
  }

  async evalsha(
    sha: string,
    keys: (string | number)[],
    args: (string | number)[]
  ): Promise<string> {
    const argsdup: (string | number)[] = [sha, keys.length, ...keys, ...args];
    return (await this.roundtrip("eval", argsdup)) as string;
  }
}

class Redis extends RedisBase {
  async pipeline(): Promise<Pipeline> {
    const conn = await this.pool.get();
    return new Pipeline(conn, this.pool, this.codec);
  }

  async subscribe(...channels: (string | number)[]): Promise<PubSuber> {
    const conn = await this.pool.get();
    const pubsuber = new PubSuber(conn, this.pool, this.codec);
    await pubsuber.subscribe(...channels);
    return pubsuber;
  }
}

class Tx extends RedisBase {
  commands: RedisCommand[] = [];
  constructor(
    protected conn: Reader & Writer & Closer,
    readonly pool: Pool,
    readonly codec: Codec
  ) {
    super(pool, codec);
  }

  close() {
    this.pool.put(this.conn);
    this.conn = undefined;
  }
}

class PubSuber extends Tx {
  constructor(
    protected conn: Reader & Writer & Closer,
    readonly pool: Pool,
    readonly codec: Codec
  ) {
    super(conn,pool, codec);
  }
  async subscribe(...channels: (string | number)[]) {
    const args = [...channels];
    await this.sendCommand(this.conn, "subscribe", args);
  }

  async punsubscribe(...patterns: (string | number)[]) {
    const args = [...patterns];
    await this.sendCommand(this.conn, "punsubscribe", args);
  }
  async unsubscribe(...channels: (string | number)[]) {
    const args = [...channels];
    await this.sendCommand(this.conn, "punsubscribe", args);
  }

  async recv(): Promise<string> {
    while (true) {
      let message = await this.readResp(this.conn) as string[];
      if (message.length != 3) {
        
        throw "unexpected protocol"
      }
      
      if (message[0] == "subscribe"){
        continue
      }
      
      return message[2]
    }
  }

  close() {
    this.conn.close()
  }
}


class Pipeline extends Tx {
  constructor(    protected conn: Reader & Writer & Closer,
    readonly pool: Pool,
    readonly codec: Codec){super(conn,pool,codec)}

  async exec(): Promise<any[]> {
    await this.roundtripEx(this.conn, "multi",[])
    for (let command of this.commands) {
      await this.roundtripEx(this.conn, command.command, command.args);
    }
    this.commands = [];
    return (await this.roundtripEx(this.conn, "exec", [])) as any[];
  }

  protected async roundtrip(
    command: string,
    args: (string | number)[]
  ): Promise<RedisReply> {
    this.commands.push({"command":command,"args":args})
    return
  }

  async watch(...keys: (string | number)[]): Promise<string> {
    const args = [...keys];
    return (await this.roundtripEx(this.conn, "watch", args)) as string;
  }

  async discard(): Promise<string> {
    return (await this.roundtripEx(this.conn, "discard", [])) as string;
  }

  async unwatch(...keys: (string | number)[]): Promise<string> {
    const args = [...keys];
    return (await this.roundtripEx(this.conn, "unwatch", args)) as string;
  }
}

function auth(password: string, codec: Codec): Auth {
  return async function(conn: Reader & Writer & Closer): Promise<boolean> {
    const args = [password];
    await codec.encodeTo(conn, "auth", args);

    const gotResp = (await codec.decode(conn)) as string;
    return gotResp == "OK";
  };
}

export async function connect(addr: string, password?: string): Promise<Redis> {
  const redis = create(addr, password);
  await redis.ping();

  return redis;
}

export async function connectEx(pool: Pool, codec: Codec): Promise<Redis> {
  const redis = createEx(pool, codec);
  await redis.ping();
  return redis;
}

export function create(addr: string, password?: string): Redis {
  const dialer = getDialer(addr);
  const pool = password
    ? createPool(dialer, auth(password, codec))
    : createPool(dialer);
  return createEx(pool, codec);
}

export function createEx(pool: Pool, codec: Codec): Redis {
  return new Redis(pool, codec);
}
