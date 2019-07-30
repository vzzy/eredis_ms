eredis_ms
=====

Redis Muti Master-Slave Client. Need ems config.

Build
-----

    $ rebar3 compile
    $ erl -pa _build/default/lib/*/ebin -pa _build/default/lib/*/priv -config sys.config -s eredis_ms
	> eredis_ms:set(pool,<<"a">>,<<"1">>).
	{ok,<<"OK">>}
	> eredis_ms:setnx(pool,<<"a">>,<<"1">>).
	{ok,<<"0">>}
	> eredis_ms:setex(pool,<<"a">>,5,<<"1">>).
	{ok,<<"OK">>}
	> eredis_ms:get(pool,<<"a">>).
	{ok,undefined}
	> eredis_ms:exists(pool,<<"a">>).
	{ok,<<"0">>}
	> eredis_ms:ttl(pool,<<"a">>).
	{ok,<<"-1">>}
	> eredis_ms:expire(pool,<<"a">>,5).
	{ok,<<"1">>}
	> eredis_ms:del(pool,<<"a">>,[<<"a">>,<<"b">>]).
	{ok,<<"0">>}
	> eredis_ms:mset(pool,<<"as">>,[<<"a">>,1,<<"b">>,2,<<"c">>,3,<<"d">>,4]).
	{ok,<<"OK">>}
	> eredis_ms:mget(pool,<<"as">>,[<<"a">>,<<"b">>,<<"c">>,<<"d">>]).
	{ok,[<<"1">>,<<"2">>,<<"3">>,<<"4">>]}
	
	> eredis_ms:hset(pool,<<"ah">>,<<"a">>,2).
	{ok,<<"0">>}
	> eredis_ms:hget(pool,<<"ah">>,<<"a">>).
	{ok,<<"2">>}
	> eredis_ms:hexists(pool,<<"ah">>,<<"a">>).
	{ok,<<"1">>}
	> eredis_ms:hlen(pool,<<"ah">>).
	{ok,<<"4">>}
	> eredis_ms:hmset(pool,<<"ah">>,[<<"a">>,1,<<"b">>,2,<<"c">>,3,<<"d">>,4]).
	{ok,<<"OK">>}
	> eredis_ms:hmget(pool,<<"ah">>,[<<"a">>,<<"b">>,<<"c">>,<<"d">>]).
	{ok,[<<"2">>,<<"2">>,<<"3">>,<<"4">>]}
	> eredis_ms:hdel(pool,<<"ah">>,[<<"a">>,<<"b">>]).
	{ok,<<"2">>}
	> eredis_ms:hgetall(pool,<<"ah">>).
	{ok,[<<"c">>,<<"3">>,<<"d">>,<<"4">>]}
	
	> eredis_ms:lpush(pool,<<"al">>,[<<"a">>,1,<<"b">>,2,<<"c">>,3,<<"d">>,4]).
	{ok,<<"8">>}
	> eredis_ms:lpushx(pool,<<"al">>,<<"a">>).
	{ok,<<"9">>}
	> eredis_ms:rpush(pool,<<"al">>,[<<"a">>,1,<<"b">>,2,<<"c">>,3,<<"d">>,4]).
	{ok,<<"17">>}
	> eredis_ms:rpushx(pool,<<"al">>,<<"a">>).
	{ok,<<"18">>}
	> eredis_ms:lpop(pool,<<"al">>).
	{ok,<<"a">>}
	> eredis_ms:rpop(pool,<<"al">>).
	{ok,<<"a">>}
	> eredis_ms:llen(pool,<<"al">>).
	{ok,<<"16">>}	
	
	> eredis_ms:sadd(pool,<<"ass">>,[<<"a">>,<<"b">>,<<"c">>,<<"d">>,1]).
	{ok,<<"5">>}
	> eredis_ms:srem(pool,<<"ass">>,[<<"a">>,1]).
	{ok,<<"2">>}
	> eredis_ms:spop(pool,<<"ass">>).
	{ok,<<"d">>}
	> eredis_ms:smembers(pool,<<"ass">>).
	{ok,[<<"b">>,<<"c">>]}
	
	> eredis_ms:qp(pool,<<"qqq">>,[["SET",<<"hello">>,<<"world">>],["GET",<<"hello">>]]).
	[{ok,<<"OK">>},{ok,<<"world">>}]
	
	
	
	
	
