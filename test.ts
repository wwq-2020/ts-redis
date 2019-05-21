import { runTests, test } from "https://deno.land/std/testing/mod.ts";
import {
  assertEquals,
  assertNotEquals
} from "https://deno.land/std/testing/asserts.ts";
import "./redis_test.ts";
import "./pool_test.ts";
import "./codec_test.ts";
import { Nil } from "codec.ts";
import { Buffer } from "util.ts";
import { connect } from "./redis.ts";
import { str2ab } from "./util.ts";

let redis = undefined;
const password: string = "foobared";

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

test(async function testConnect() {
  const ip = Deno.env().DOCKERMODE ? "redis" : "127.0.0.1";

  redis = await connect(
    `${ip}:6379`,
    password
  );
});

const OKResp = "OK";

test(async function testSetAppendDel() {
  const key: string = "hello";
  const val: string = "val";
  const setResp = await redis.set(key, val);
  assertEquals(OKResp, setResp);
  const appendResp = await redis.append(key, "test");
  assertEquals(7, appendResp);
  let gotVal: string = await redis.get(key);
  assertEquals("valtest", gotVal);
  const delResp = await redis.del(key);
  assertEquals(1, delResp);
  gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testSetbitBitCountDel() {
  const key: string = "hello";
  let setbitResp = await redis.setbit(key, 2, 1);
  assertEquals(0, setbitResp);

  setbitResp = await redis.setbit(key, 1, 1);
  assertEquals(0, setbitResp);

  const count = await redis.bitcount(key);

  assertEquals(2, count);
  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotResp = await redis.get(key);
  assertEquals(Nil, gotResp);
});

test(async function testSetbitGetbitDel() {
  const key: string = "hello";
  const setbitResp = await redis.setbit(key, 2, 1);
  assertEquals(0, setbitResp);
  const val = await redis.getbit(key, 2);

  assertEquals(1, val);
  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotResp = await redis.get(key);
  assertEquals(Nil, gotResp);
});

test(async function testGetSetDel() {
  const key: string = "hello";
  const expectedVal: string = "val";
  const setResp = await redis.set(key, expectedVal);
  assertEquals(OKResp, setResp);
  let gotVal: string = await redis.get(key);
  assertEquals(expectedVal, gotVal);
  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testPubSub() {
  const channel="testpub"
  const message1 = "hello"
  const message2 = "hello2"

  setTimeout(async ()=>{
   await  redis.publish(channel,message1)
  },1000)
  const suber= await redis.subscribe(channel)
  
  
  let msg  = await suber.recv()
  
  assertEquals(message1,msg)
  
   

});


test(async function testPipeline() {
 const  key = "pipelinetest"
  const pipeline = await redis.pipeline()
  pipeline.set(key,2)
  pipeline.incr(key)
  const pipelineResp = await pipeline.exec()
  assertEquals(OKResp,pipelineResp[0])
  assertEquals(3,pipelineResp[1])

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key)

  assertEquals(Nil, gotVal);
  
});




test(async function testPsetex() {
  const key = "helloex";
  const val = "world";

  let psetexResp =await redis.psetex(key,1000,val)
  assertEquals(OKResp, psetexResp)
    
  let gotVal = await redis.get(key);
  assertEquals(val, gotVal);

  await sleep(1000)
   gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);

});

test(async function testSetex() {
  const key = "helloex1";
  const val = "world";

  let setexResp =await redis.setex(key,1,val)
  assertEquals(OKResp, setexResp)
    
  let gotVal = await redis.get(key);
  assertEquals(val, gotVal);

  await sleep(1000)
  gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
  

 

});

test(async function testSetnx() {
  const key1 = "hello11";
  const val1 = "world11";
  const key2 = "hello22";
  const val2 = "world22";
  let setnxResp =await redis.setnx(key1,val1)
  
  assertEquals(1,setnxResp)
  setnxResp =await redis.setnx(key1,val1)
  
  assertEquals(0,setnxResp)
  
  setnxResp =await redis.setnx(key2,val2)
  
  assertEquals(1,setnxResp)
  setnxResp =await redis.setnx(key2,val2)
  
  assertEquals(0,setnxResp)

  
  let delResp = await redis.del(key1);
  assertEquals(1, delResp);
  
  let gotVal = await redis.get(key1);
  assertEquals(Nil, gotVal);
  
   delResp = await redis.del(key2);
  assertEquals(1, delResp);
  
   gotVal = await redis.get(key2);
  assertEquals(Nil, gotVal);

const  msetnxResp =  await redis.msetnx(key1,val1,key2,val2)

assertEquals(1,msetnxResp)

 delResp = await redis.del(key1);
assertEquals(1, delResp);

 gotVal = await redis.get(key1);
assertEquals(Nil, gotVal);

 delResp = await redis.del(key2);
assertEquals(1, delResp);

 gotVal = await redis.get(key2);
assertEquals(Nil, gotVal);
});



test(async function testRange() {
  const key = "hello";
  const val = "world";
 const setResp =  await redis.set(key,val);
 assertEquals(OKResp,setResp)

const setrangeResp =await redis.setrange(key,1,"w")
assertEquals(val.length,setrangeResp)
const getrange = await redis.getrange(key,1,val.length)

assertEquals("wrld", getrange)

const delResp = await redis.del(key);
assertEquals(1, delResp);

const gotVal = await redis.get(key);
assertEquals(Nil, gotVal);
});



test(async function testIncr() {
  const key = "hello";
  const incrResp = await redis.incr(key);
  assertEquals(1, incrResp);

  const decrResp = await redis.decr(key);
  assertEquals(0, decrResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testIncrby() {
  const key = "hello";
  const incrResp = await redis.incrby(key, 2);
  assertEquals(2, incrResp);

  const decrbyResp = await redis.decrby(key, 2);
  assertEquals(0, decrbyResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testIncrbyfloat() {
  const key = "hello";
  const incrbyfloatResp = await redis.incrbyfloat(key, 1.1);
  assertEquals(
    true,
    Math.abs(incrbyfloatResp - 1.1) < Number.EPSILON * Math.pow(2, 2)
  );
  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testBrpoplpush() {
  const key = "hello";
  const dest = "dest";
  const val = "world";
   const resp = await redis.lpush(key, val);
  

  const rpoplpushResp = await redis.brpoplpush(key, dest, 1);
  assertEquals(val, rpoplpushResp);
  
  let gotVal = await redis.get(key);
  
  assertEquals(Nil, gotVal);

  let delResp = await redis.del(dest);
  
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  
  assertEquals(Nil, gotVal);
  
});

test(async function testRpoplpush() {
  const key = "hello";
  const dest = "dest";
  const val = "world";
  const lpushResp = await redis.lpush(key, val);
  assertEquals(1, lpushResp);
  const lindexResp = await redis.lindex(key, 0);

  assertEquals(val, lindexResp);

  const rpoplpushResp = await redis.rpoplpush(key, dest);

  assertEquals(val, rpoplpushResp);

  let gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);

  let delResp = await redis.del(dest);
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);
});

test(async function testLinsert() {
  const key = "hello";
  const val1 = "world1";
  const val2 = "world2";
  const val3 = "world3";

  const lpushResp = await redis.lpush(key, val1);

  assertEquals(1, lpushResp);

  let linsertResp = await redis.linsert(key, "before", val1, val2);
  assertEquals(2, linsertResp);
  linsertResp = await redis.linsert(key, "after", val1, val3);
  assertEquals(3, linsertResp);
  let lindexResp = await redis.lindex(key, 0);
  assertEquals(val2, lindexResp);
  lindexResp = await redis.lindex(key, 1);
  assertEquals(val1, lindexResp);
  lindexResp = await redis.lindex(key, 2);
  assertEquals(val3, lindexResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testLset() {
  const key = "hello";
  const val1 = "world1";
  const val2 = "world2";
  const val3 = "world3";

  const lpushResp = await redis.lpush(key, val1, val2, val3);
  assertEquals(3, lpushResp);
  let ltrimResp = await redis.ltrim(key, 0, 1);
  assertEquals(OKResp, ltrimResp);

  const llenResp = await redis.llen(key);
  assertEquals(2, llenResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testLset() {
  const key = "hello";
  const val1 = "world1";
  const val2 = "world2";

  const lpushResp = await redis.lpush(key, val1, val2);
  assertEquals(2, lpushResp);
  let lsetResp = await redis.lset(key, 0, "world3");
  assertEquals(OKResp, lsetResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testLrem() {
  const key = "hello";
  const val = "world";

  const lpushResp = await redis.lpush(key, val, val);
  assertEquals(2, lpushResp);
  let lremResp = await redis.lrem(key, 2, val);
  assertEquals(2, lremResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testLrange() {
  const key = "hello";
  const val1 = "world1";
  const val2 = "world2";

  const lpushResp = await redis.lpush(key, val1, val2);
  assertEquals(2, lpushResp);
  let lrangeResp = await redis.lrange(key, 0, 2);
  assertEquals(2, lrangeResp.length);
  assertEquals("world2", lrangeResp[0]);
  assertEquals("world1", lrangeResp[1]);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testLpushx() {
  const key = "hello";
  const val = "world";
  let lpushxResp = await redis.lpushx(key, val);
  assertEquals(0, lpushxResp);
  let rpushxResp = await redis.rpushx(key, val);
  assertEquals(0, rpushxResp);

  const lpushResp = await redis.lpush(key, val);
  assertEquals(1, lpushResp);

  lpushxResp = await redis.lpushx(key, val);
  assertEquals(2, lpushxResp);

  rpushxResp = await redis.rpushx(key, val);
  assertEquals(3, rpushxResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testBZpop() {
  const key = "hello";
  const item1 = { score: 1.1, member: "a" };

await redis.zadd(key, [item1], {
      exist: "nx",
      ch: true
    });
    
  
  const zpopmaxResp = await redis.bzpopmax(key, 1);
  
  assertEquals(3, zpopmaxResp.length);
  
  assertEquals(key, zpopmaxResp[0]);
  
  assertEquals("a", zpopmaxResp[1]);
  
  assertEquals(
    true,
    Math.abs(zpopmaxResp[2] - 1.1) < Number.EPSILON * Math.pow(2, 2)
  );


    await redis.zadd(key, [item1], {
      exist: "nx",
      ch: true
    });
 

  const zpopminResp = await redis.bzpopmin(key, 1);
  assertEquals(3, zpopmaxResp.length);
  assertEquals(key, zpopmaxResp[0]);
  assertEquals("a", zpopmaxResp[1]);

  assertEquals(
    true,
    Math.abs(zpopmaxResp[2] - 1.1) < Number.EPSILON * Math.pow(2, 2)
  );

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZpop() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };

  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  
  assertEquals(3, zaddResp);
  const zpopmaxResp = await redis.zpopmax(key);
  assertEquals(2, zpopmaxResp.length);
  assertEquals("c", zpopmaxResp[0]);
  assertEquals(
    true,
    Math.abs(zpopmaxResp[1] - 3.1) < Number.EPSILON * Math.pow(2, 2)
  );

  const zpopminResp = await redis.zpopmin(key);

  assertEquals(2, zpopminResp.length);
  assertEquals("a", zpopminResp[0]);
  assertEquals(
    true,
    Math.abs(zpopminResp[1] - 1.1) < Number.EPSILON * Math.pow(2, 2)
  );

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZInterUnion() {
  const key1 = "hellozunion";
  const key2 = "hello1zuion";

  const dest = "destzuion";

  const key1Item1 = { score: 1.1, member: "a1" };
  const key1Item2 = { score: 2.1, member: "b" };

  const key2Item1 = { score: 3.1, member: "b" };

  const key2Item2 = { score: 4.1, member: "d" };

  let zaddResp = await redis.zadd(key1, [key1Item1, key1Item2], {
    exist: "nx",
    ch: true
  });
  assertEquals(2, zaddResp);
  zaddResp = await redis.zadd(key2, [key2Item1, key2Item2], {
    exist: "nx",
    ch: true
  });
  assertEquals(2, zaddResp);

  const zinterstoreResp = await redis.zinterstore(dest, 2, [key1, key2]);
  assertEquals(1, zinterstoreResp);
  let delResp = await redis.del(dest);
  assertEquals(1, delResp);

  let gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);

  const zunionstoreResp = await redis.zunionstore(dest, 2, [key1, key2]);
  assertEquals(3, zunionstoreResp);

  delResp = await redis.del(key1);
  assertEquals(1, delResp);

  gotVal = await redis.get(key1);
  assertEquals(Nil, gotVal);

  delResp = await redis.del(key2);
  assertEquals(1, delResp);

  gotVal = await redis.get(key2);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(dest);
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);
});

test(async function testZscan() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a1" };
  const item2 = { score: 1.1, member: "a2" };

  const item3 = { score: 2.1, member: "b" };

  const item4 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3, item4], {
    exist: "nx",
    ch: true
  });
  assertEquals(4, zaddResp);

  const zscanResp = await redis.zscan(key, 0, { pattern: "a*" });
  assertEquals(2, zscanResp.length);

  assertEquals(4, zscanResp[1].length);
  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZscore() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);
  const zscoreResp = await redis.zscore(key, "a");
  assertEquals(
    true,
    Math.abs(zscoreResp - 1.1) < Number.EPSILON * Math.pow(2, 2)
  );

  const zincrbyResp = await await redis.zincrby(key, 1, "a");

  assertEquals(
    true,
    Math.abs(zincrbyResp - 2.1) < Number.EPSILON * Math.pow(2, 2)
  );

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZrank() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);

  let rankResp = await redis.zrank(key, "a");

  assertEquals(0, rankResp);
  rankResp = await redis.zrevrank(key, "a");
  assertEquals(2, rankResp);

  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZRange() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 4.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);

  let zrangeResp = await redis.zrange(key, 0, 2);
  assertEquals(3, zrangeResp.length);

  assertEquals("a", zrangeResp[0]);
  assertEquals("c", zrangeResp[1]);
  assertEquals("b", zrangeResp[2]);

  const delResp = await redis.del(key);

  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZRangeByRank() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);

  let zrangebyscoreResp = await redis.zremrangebyrank(key, 0, 1);
  assertEquals(2, zrangebyscoreResp);

  const delResp = await redis.del(key);

  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZRangeByScore() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);

  let zrangebyscoreResp = await redis.zrangebyscore(key, 0, 3);
  assertEquals(2, zrangebyscoreResp.length);

  let zrevrangebyscoreResp = await redis.zrevrangebyscore(key, 3, 0);
  assertEquals(2, zrevrangebyscoreResp.length);

  let zremrangebyscoreResp = await redis.zremrangebyscore(key, 0, 3);
  assertEquals(2, zremrangebyscoreResp);

  const delResp = await redis.del(key);

  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZRangeByLex() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  let zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);

  let zrangebylexResp = await redis.zrangebylex(key, "-", "(c");
  assertEquals(2, zrangebylexResp.length);

  let zrevrangebylex = await redis.zrevrangebylex(key, "(c", "-");

  assertEquals(2, zrevrangebylex.length);

  let zremrangebylexResp = await redis.zremrangebylex(key, "-", "(c");
  assertEquals(2, zremrangebylexResp);

  const delResp = await redis.del(key);

  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testZadd() {
  const key = "hello";

  const item1 = { score: 1.1, member: "a" };
  const item2 = { score: 2.1, member: "b" };

  const item3 = { score: 3.1, member: "c" };

  const zaddResp = await redis.zadd(key, [item1, item2, item3], {
    exist: "nx",
    ch: true
  });
  assertEquals(3, zaddResp);

  let zcountResp = await redis.zcount(key, 0, 4);
  assertEquals(3, zcountResp);
  let zlexcountResp = await redis.zlexcount(key, "-", "[c");
  assertEquals(3, zlexcountResp);

  const zremReps = await redis.zrem(key, "a", "b");
  assertEquals(2, zremReps);

  zcountResp = await redis.zcount(key, 0, 4);
  assertEquals(1, zcountResp);

  const zcardResp = await redis.zcard(key);

  assertEquals(1, zcardResp);

  const delResp = await redis.del(key);

  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testSunion() {
  const key1 = "hellosunion";
  const key1member1 = "deno1";
  const key1member2 = "deno2";

  const key2 = "hello1suion";
  const key2member1 = "deno3";
  const key2member2 = "deno2";

  const dest = "dest";

  let saddResp = await redis.sadd(key1, key1member1, key1member2);
  assertEquals(2, saddResp);

  saddResp = await redis.sadd(key2, key2member1, key2member2);
  assertEquals(2, saddResp);

  const sunionResp = await redis.sunion(key1, key2);
  assertEquals(3, sunionResp.length);

  const unionstoreResp = await redis.sunionstore(dest, key1, key2);
  assertEquals(3, unionstoreResp);

  let delResp = await redis.del(key1);
  assertEquals(1, delResp);

  let gotVal = await redis.get(key1);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(key2);
  assertEquals(1, delResp);

  gotVal = await redis.get(key2);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(dest);
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);
});

test(async function testSinter() {
  const key1 = "hellosinter";
  const key1member1 = "deno1";
  const key1member2 = "deno2";

  const key2 = "hello1sinter";
  const key2member1 = "deno3";
  const key2member2 = "deno2";

  const dest = "destsinter";

  let saddResp = await redis.sadd(key1, key1member1, key1member2);
  assertEquals(2, saddResp);

  saddResp = await redis.sadd(key2, key2member1, key2member2);
  assertEquals(2, saddResp);

  const sinterResp = await redis.sinter(key1, key2);
  assertEquals(1, sinterResp.length);
  assertEquals("deno2", sinterResp[0]);

  const sinterstoreResp = await redis.sinterstore(dest, key1, key2);
  assertEquals(1, sinterstoreResp);

  let delResp = await redis.del(key1);
  assertEquals(1, delResp);

  let gotVal = await redis.get(key1);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(key2);
  assertEquals(1, delResp);

  gotVal = await redis.get(key2);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(dest);
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);
});

test(async function testSdiff() {
  const key1 = "hellosdiff";
  const key1member1 = "deno1";
  const key1member2 = "deno2";

  const key2 = "hello1sdiff";
  const key2member1 = "deno3";
  const key2member2 = "deno2";

  const dest = "dest";

  let saddResp = await redis.sadd(key1, key1member1, key1member2);
  assertEquals(2, saddResp);

  saddResp = await redis.sadd(key2, key2member1, key2member2);
  assertEquals(2, saddResp);

  const sdiffResp = await redis.sdiff(key1, key2);
  assertEquals(1, sdiffResp.length);
  assertEquals("deno1", sdiffResp[0]);

  const sdiffstoreResp = await redis.sdiffstore(dest, key1, key2);
  assertEquals(1, sdiffstoreResp);

  let delResp = await redis.del(key1);
  assertEquals(1, delResp);

  let gotVal = await redis.get(key1);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(key2);
  assertEquals(1, delResp);

  gotVal = await redis.get(key2);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(dest);
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);
});

test(async function testSpopSrandmember() {
  const key = "hello";
  const member1 = "deno1";
  const member2 = "deno2";
  const saddResp = await redis.sadd(key, member1, member2);
  assertEquals(2, saddResp);
  const spopResp = await redis.spop(key);
  assertNotEquals(0, spopResp.length);

  const srandmemberResp = await redis.srandmember(key);
  assertNotEquals(0, srandmemberResp.length);

  let delResp = await redis.del(key);
  assertEquals(1, delResp);

  let gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testSadd() {
  const key = "hello";
  const member1 = "deno1";
  const member2 = "deno2";
  const dest = "dest";
  const saddResp = await redis.sadd(key, member1, member2);
  assertEquals(2, saddResp);

  let ismemberResp = await redis.sismember(key, member1);
  assertEquals(1, ismemberResp);

  const smembersResp = await redis.smembers(key, member1);
  assertEquals(2, smembersResp.length);

  const smoveResp = await redis.smove(key, dest, member1);
  assertEquals(1, smoveResp);
  ismemberResp = await redis.sismember(key, member1);
  assertEquals(0, ismemberResp);
  ismemberResp = await redis.sismember(dest, member1);
  assertEquals(1, ismemberResp);

  const scardResp = await redis.scard(key);
  assertEquals(1, scardResp);
  let delResp = await redis.del(key);
  assertEquals(1, delResp);

  let gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
  delResp = await redis.del(dest);
  assertEquals(1, delResp);

  gotVal = await redis.get(dest);
  assertEquals(Nil, gotVal);
});

test(async function testBlpop() {
  const key = "hello";
  const val = "world";

    await redis.lpush(key, val);

  const blpopResp = await redis.blpop(key, 2);
  assertEquals(2, blpopResp.length);
  assertEquals(key, blpopResp[0]);
  assertEquals(val, blpopResp[1]);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testlpop() {
  const key = "hello";
  const val = "world";

  const rpushResp = await redis.lpush(key, val);
  assertEquals(1, rpushResp);

  const blpopResp = await redis.lpop(key);
  assertEquals(val, blpopResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testrpop() {
  const key = "hello";
  const val = "world";

  const rpushResp = await redis.rpush(key, val);
  assertEquals(1, rpushResp);

  const blpopResp = await redis.rpop(key);
  assertEquals(val, blpopResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testBrpop() {
  const key = "hello";
  const val = "world";

    await redis.rpush(key, val);

  const blpopResp = await redis.brpop(key, 2);
  assertEquals(2, blpopResp.length);
  assertEquals(key, blpopResp[0]);
  assertEquals(val, blpopResp[1]);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHsetnxHdel() {
  const key: string = "hello";
  const field1 = "deno1";
  const val1 = "world1";

  let hsetnxResp = await redis.hsetnx(key, field1, val1);

  assertEquals(1, hsetnxResp);

  hsetnxResp = await redis.hsetnx(key, field1, val1);

  assertEquals(0, hsetnxResp);

  const delResp = await redis.hdel(key, field1);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testHstrlenHdel() {
  const key: string = "hello";
  const field1 = "deno1";
  const val1 = "world1";

  let hsetResp = await redis.hset(key, field1, val1);

  assertEquals(1, hsetResp);

  const strlenResp = await redis.hstrlen(key, field1);
  assertEquals(val1.length, strlenResp);
  const delResp = await redis.hdel(key, field1);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testPfaddPfcountPfmergeDel() {
  const key1 = "pf1";
  const key2 = "pf2";
  const key1member1 = "pf1member1";
  const key1member2 = "pf1member2";
  const key2member1 = "pf2member1";
  const key2member2 = "pf2member2";
  const destKey = "pfdest";
  let addResp = await redis.pfadd(key1, key1member1, key1member2);
  assertEquals(1, addResp);
  addResp = await redis.pfadd(key2, key2member1, key2member2);

  assertEquals(1, addResp);
  let countResp = await redis.pfcount(key1);

  assertEquals(2, countResp);
  countResp = await redis.pfcount(key2);

  assertEquals(2, countResp);
  const mergeResp = await redis.pfmerge(destKey, key1, key2);
  countResp = await redis.pfcount(destKey);
  assertEquals(4, countResp);

  let delResp = await redis.del(key1);
  assertEquals(1, delResp);
  delResp = await redis.del(key2);
  assertEquals(1, delResp);
  delResp = await redis.del(destKey);
  assertEquals(1, delResp);
});

test(async function testGeoaddDel() {
  const key = "hello";

  const item1 = { longitude: 1.1, latitude: 1.1, member: "deno1" };
  const item2 = { longitude: 2.1, latitude: 2.1, member: "deno2" };

  let resp = await redis.geoadd(key, item1, item2);
  assertEquals(2, resp);
  const delResp = await redis.del(key);
  assertEquals(1, delResp);

  const gotVal = await redis.get(key);
  assertEquals(Nil, gotVal);
});

test(async function testHscanHdel() {
  const key: string = "hello";
  const field1 = "deno1";
  const val1 = "world1";
  const field2 = "deno2";
  const val2 = "world2";
  const field3 = "otherfield";
  const val3 = "otherval";

  let hmsetResp = await redis.hmset(
    key,
    field1,
    val1,
    field2,
    val2,
    field3,
    val3
  );

  assertEquals(OKResp, hmsetResp);

  let hscanResp = await redis.hscan(key, 0);

  assertEquals(2, hscanResp.length);
  assertEquals("0", hscanResp[0]);

  assertEquals(6, hscanResp[1].length);

  hscanResp = await redis.hscan(key, 0, { pattern: "deno*" });

  assertEquals(2, hscanResp.length);
  assertEquals("0", hscanResp[0]);

  assertEquals(4, hscanResp[1].length);

  const hdelResp = await redis.hdel(key, field1, field2, field3);
  assertEquals(3, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHlenHdel() {
  const key: string = "hello";
  const field1 = "deno1";
  const val1 = "world1";
  const field2 = "deno2";
  const val2 = "world2";
  let hsetResp = await redis.hset(key, field1, val1);

  assertEquals(1, hsetResp);

  let hlenResp = await redis.hlen(key);

  assertEquals(1, hlenResp);

  hsetResp = await redis.hset(key, field2, val2);
  assertEquals(1, hlenResp);

  hlenResp = await redis.hlen(key);

  assertEquals(2, hlenResp);
  const hdelResp = await redis.hdel(key, field1, field2);
  assertEquals(2, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHvalsHdel() {
  const key: string = "hello";
  const field1 = "deno1";
  const val1 = "world1";
  const field2 = "deno2";
  const val2 = "world2";
  let hsetResp = await redis.hset(key, field1, val1);

  assertEquals(1, hsetResp);

  let hgetResp = await redis.hvals(key);
  assertEquals(val1, hgetResp[0]);

  hsetResp = await redis.hset(key, field2, val2);

  assertEquals(1, hsetResp);

  hgetResp = await redis.hvals(key);
  assertEquals(val1, hgetResp[0]);
  assertEquals(val2, hgetResp[1]);

  const hdelResp = await redis.hdel(key, field1, field2);
  assertEquals(2, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHkeysHdel() {
  const key: string = "hello";
  const field1 = "deno1";
  const val1 = "world1";
  const field2 = "deno2";
  const val2 = "world2";
  let hsetResp = await redis.hset(key, field1, val1);

  assertEquals(1, hsetResp);

  let hgetResp = await redis.hkeys(key);
  assertEquals(field1, hgetResp[0]);

  hsetResp = await redis.hset(key, field2, val2);

  assertEquals(1, hsetResp);

  hgetResp = await redis.hkeys(key);
  assertEquals(field1, hgetResp[0]);
  assertEquals(field2, hgetResp[1]);

  const hdelResp = await redis.hdel(key, field1, field2);
  assertEquals(2, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHsetHexistsHdel() {
  const key: string = "hello";
  const field = "deno";
  const val = "world";
  const hsetResp = await redis.hset(key, field, val);

  assertEquals(1, hsetResp);

  let hgetResp = await redis.hexists(key, field);
  assertEquals(1, hgetResp);

  hgetResp = await redis.hexists(key, "noexist");
  assertEquals(0, hgetResp);

  const hdelResp = await redis.hdel(key, field);
  assertEquals(1, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHincrbyfloatHdel() {
  const key: string = "hello";
  const field = "deno";
  const expectedVal = 1.1;
  const hsetResp = await redis.hincrbyfloat(key, field, expectedVal);

  assertEquals("1.1", hsetResp);

  const hdelResp = await redis.hdel(key, field);
  assertEquals(1, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHincrbyHdel() {
  const key: string = "hello";
  const field = "deno";
  const expectedVal = 1;
  const hsetResp = await redis.hincrby(key, field, expectedVal);

  assertEquals(expectedVal, hsetResp);

  const hdelResp = await redis.hdel(key, field);
  assertEquals(1, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHmSetHmgetHgetallHDel() {
  const key: string = "hello";
  const field1 = "deno1";
  const expectedVal1 = "world1";
  const field2 = "deno2";
  const expectedVal2 = "world2";

  const hsetResp = await redis.hmset(
    key,
    field1,
    expectedVal1,
    field2,
    expectedVal2
  );

  assertEquals(OKResp, hsetResp);

  let hgetResp = await redis.hget(key, field1);
  assertEquals(expectedVal1, hgetResp);
  hgetResp = await redis.hget(key, field2);
  assertEquals(expectedVal2, hgetResp);

  const hmgetResp = await redis.hmget(key, field1, field2);

  assertEquals(expectedVal1, hmgetResp[0]);
  assertEquals(expectedVal2, hmgetResp[1]);

  const getallResp = await redis.hgetall(key);

  assertEquals(field1, getallResp[0]);
  assertEquals(expectedVal1, getallResp[1]);

  assertEquals(field2, getallResp[2]);
  assertEquals(expectedVal2, getallResp[3]);

  const hdelResp = await redis.hdel(key, field1, field2);

  assertEquals(2, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testHSetHGetHdelHDel() {
  const key: string = "hello";
  const field = "deno";
  const expectedVal = "world";
  const hsetResp = await redis.hset(key, field, expectedVal);

  assertEquals(1, hsetResp);

  let hgetResp = await redis.hget(key, field);
  assertEquals(expectedVal, hgetResp);

  const hdelResp = await redis.hdel(key, field);
  assertEquals(1, hdelResp);

  const getResp = await redis.get(key);
  assertEquals(Nil, getResp);
});

test(async function testMGetMSetMDel() {
  let key: string = "hello";
  let expectedVal: string = "val";
  const expectedKey1: string = "hello";
  const expectedKey2: string = "deno";
  const expectedVal1: string = "world";
  const expectedVal2: string = "good";
  await redis.mset(expectedKey1, expectedVal1, expectedKey2, expectedVal2);
  let gotResp = await redis.mget(expectedKey1, expectedKey2);
  assertEquals(2, gotResp.length);
  assertEquals(expectedVal1, gotResp[0]);
  assertEquals(expectedVal2, gotResp[1]);
  await redis.del(expectedKey1, expectedKey2);
  gotResp = await redis.mget(expectedKey1, expectedKey2);
  assertEquals(2, gotResp.length);
  assertEquals(Nil, gotResp[0]);
  assertEquals(Nil, gotResp[1]);
});

test(async function testSetDumpDel() {
  const key: string = "hello";
  const expectedVal: string = "val";
  await redis.set(key, expectedVal);
  let gotResp = await redis.dump(key);
  assertNotEquals(0, gotResp.length);
  await redis.del(key);
  gotResp = await redis.dump(key);
  assertEquals(Nil, gotResp);
});

test(async function testMSetExistsMDel() {
  const key1: string = "hello";
  const key2: string = "world";
  const expectedVal: string = "val";
  await redis.mset(key1, expectedVal, key2, expectedVal);
  let gotResp = await redis.exists(key1, key2);
  assertEquals(2, gotResp);
  await redis.del(key1, key2);
  gotResp = await redis.exists(key1, key2);
  assertEquals(0, gotResp);
});


test(async function testAuth() {
  if (password != "") {
    const expectedResp = "OK";
    const gotResp = await redis.auth(password);
    assertEquals(expectedResp, gotResp);
  }
});

test(async function testEcho() {
  const expectedResp = "OK";
  const gotResp = await redis.echo(expectedResp);
  assertEquals(expectedResp, gotResp);
});

test(async function testPing() {
  const expectedResp = "PONG";
  const gotResp = await redis.ping("PING");
  assertEquals(expectedResp, gotResp);
});

test(async function testPersistDel() {
  const key: string = "hello1";
  const expectedVal: string = "val";
  await redis.set(key, expectedVal);
  await redis.expire(key, 1);
  await redis.persist(key);


  await sleep(1000)
  const gotVal = await redis.get(key);
  await redis.del(key);

  assertNotEquals(Nil, gotVal);
  
});

test(async function testSetExpireDel() {
  const key: string = "hello2";
  const expectedVal: string = "val";
  await redis.set(key, expectedVal);
  let gotResp = await redis.expire(key, 1);

  await sleep(1000)
  const gotVal = await redis.get(key);
  await redis.del(key);
  assertEquals(Nil, gotVal);
  
});

test(async function testSetExpireAtDel() {
  const key: string = "hello3";
  const expectedVal: string = "val";
  await redis.set(key, expectedVal);
  const timstamp = Date.parse(new Date().toString()) / 1000 + 1;
  let gotResp = await redis.expireAt(key, timstamp);

  await sleep(1000)
  const gotVal = await redis.get(key);
  await redis.del(key);
  assertEquals(Nil, gotVal);
  
});

runTests({ exitOnFail: true });
