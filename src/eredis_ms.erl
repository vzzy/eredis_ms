%% @author bai
%% @doc @todo Add description to eredis_ms.


-module(eredis_ms).

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	start/0,	 
	
	q/3,
	q_noreply/3, 
	qp/3,
	
	set/3,
	set_ttl/4,
	setnx/3,
	setex/4,
	get/2,
	exists/2,
	ttl/2,
	expire/3,
	incr/2,
	incrby/3,
	del/3,
	mset/3,
	mget/3,
	
	hset/4,
	hset_ttl/5,
	hsetnx/4,
	hget/3,
	hexists/3,
	hdel/3,
	hlen/2,
	hmset/3,
	hmset_ttl/4,
	hmget/3,
	hgetall/2,
	
	lpush/3,
	lpush_ttl/4,
	lpushx/3,
	rpush/3,
	rpush_ttl/4,
	rpushx/3,
	lpop/2,
	rpop/2,
	llen/2,
	lrange/4,
	
	sadd/3,
	sadd_ttl/4,
	sismember/3,
	spop/2,
	srem/3,
	smembers/2,
	
	zadd/3,
	zadd_ttl/4,
	zrange/2,
	zrange/4,
	zrem/3,

	keys/2,
	scan/2,
	sscan/2,
	zscan/2,
	hscan/2,
	
	test/0,
	init/1
]).
%% Redis正常返回时间应该是10ms内，超过100ms都算久。
-define(TIMEOUT, 3*1000).
%% 游标返回建议数据量
-define(DEFAULT_SCAN_COUNT, 500).

%% ====================================================================
%% Internal functions
%% ====================================================================

q(Poolname,Key,Commands)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, Commands, ?TIMEOUT);
		R->
			R
	end.
q_noreply(Poolname,Key,Commands)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q_noreply(Client, Commands);
		R->
			R
	end.
qp(Poolname,Key,Commands)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:qp(Client, Commands, ?TIMEOUT);
		R->
			R
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 具体命令
set(Poolname,Key,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SET",Key,Value], ?TIMEOUT);
		R->
			R
	end.
set_ttl(Poolname,Key,Value,Seconds)->
	setex(Poolname,Key,Seconds,Value).
setnx(Poolname,Key,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SETNX",Key,Value], ?TIMEOUT);
		R->
			R
	end.
setex(Poolname,Key,Seconds,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SETEX",Key,Seconds,Value], ?TIMEOUT);
		R->
			R
	end.
get(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["GET",Key], ?TIMEOUT);
		R->
			R
	end.
exists(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["EXISTS",Key], ?TIMEOUT);
		R->
			R
	end.
ttl(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["TTL",Key], ?TIMEOUT);
		R->
			R
	end.
incr(Poolname,Key)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["INCR",Key], ?TIMEOUT);
		R->
			R
	end.
incrby(Poolname,Key,Increment)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["INCRBY",Key,Increment], ?TIMEOUT);
		R->
			R
	end.
expire(Poolname,Key,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["EXPIRE",Key,Seconds], ?TIMEOUT);
		R->
			R
	end.
del(Poolname,Key,KeyPairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["DEL" | KeyPairs], ?TIMEOUT);
		R->
			R
	end.
mset(Poolname,Key,KeyValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["MSET" | KeyValuePairs], ?TIMEOUT);
		R->
			R
	end.
mget(Poolname,Key,KeyPairs)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["MGET" | KeyPairs], ?TIMEOUT);
		R->
			R
	end.

hset(Poolname,Key,Field,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HSET",Key,Field,Value], ?TIMEOUT);
		R->
			R
	end.
hset_ttl(Poolname,Key,Field,Value,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			case eredis:qp(Client, [["HSET",Key,Field,Value],["EXPIRE",Key,Seconds]], ?TIMEOUT) of
				{error,R1}->
					{error,R1};
				[Response | _] ->
					Response
			end;
		R->
			R
	end.
hsetnx(Poolname,Key,Field,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HSETNX",Key,Field,Value], ?TIMEOUT);
		R->
			R
	end.
hget(Poolname,Key,Field)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HGET",Key,Field], ?TIMEOUT);
		R->
			R
	end.
hexists(Poolname,Key,Field)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HEXISTS",Key,Field], ?TIMEOUT);
		R->
			R
	end.
hlen(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HLEN",Key], ?TIMEOUT);
		R->
			R
	end.
hmset(Poolname,Key,FieldValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HMSET",Key | FieldValuePairs], ?TIMEOUT);
		R->
			R
	end.
hmset_ttl(Poolname,Key,FieldValuePairs,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			case eredis:qp(Client, [["HMSET",Key | FieldValuePairs],["EXPIRE",Key,Seconds]], ?TIMEOUT) of
				{error,R1}->
					{error,R1};
				[Response | _] ->
					Response
			end;
		R->
			R
	end.
hmget(Poolname,Key,FieldPairs)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HMGET",Key | FieldPairs], ?TIMEOUT);
		R->
			R
	end.
hdel(Poolname,Key,FieldPairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["HDEL",Key | FieldPairs], ?TIMEOUT);
		R->
			R
	end.
hgetall(Poolname,Key)->
	hscan(Poolname,Key).
	%% 改用游标，根本解决方式，还是尽量避免出现大key
%% 	case ems:get_client_read(Poolname,Key) of
%% 		{ok,Client}->
%% 			eredis:q(Client, ["HGETALL",Key], ?TIMEOUT);
%% 		R->
%% 			R
%% 	end.


lpush(Poolname,Key,ValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["LPUSH",Key | ValuePairs], ?TIMEOUT);
		R->
			R
	end.
lpush_ttl(Poolname,Key,ValuePairs,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			case eredis:qp(Client, [["LPUSH",Key | ValuePairs],["EXPIRE",Key,Seconds]], ?TIMEOUT) of
				{error,R1}->
					{error,R1};
				[Response | _] ->
					Response
			end;
		R->
			R
	end.
lpushx(Poolname,Key,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["LPUSHX",Key,Value], ?TIMEOUT);
		R->
			R
	end.
rpush(Poolname,Key,ValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["RPUSH",Key | ValuePairs], ?TIMEOUT);
		R->
			R
	end.
rpush_ttl(Poolname,Key,ValuePairs,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			case eredis:qp(Client, [["RPUSH",Key | ValuePairs],["EXPIRE",Key,Seconds]], ?TIMEOUT) of
				{error,R1}->
					{error,R1};
				[Response | _] ->
					Response
			end;
		R->
			R
	end.
rpushx(Poolname,Key,Value)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["RPUSHX",Key,Value], ?TIMEOUT);
		R->
			R
	end.
lpop(Poolname,Key)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["LPOP",Key], ?TIMEOUT);
		R->
			R
	end.
rpop(Poolname,Key)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["RPOP",Key], ?TIMEOUT);
		R->
			R
	end.
llen(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["LLEN",Key], ?TIMEOUT);
		R->
			R
	end.
lrange(Poolname,Key,Start,Stop)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["LRANGE",Key,Start,Stop], ?TIMEOUT);
		R->
			R
	end.
sadd(Poolname,Key,ValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SADD",Key | ValuePairs], ?TIMEOUT);
		R->
			R
	end.
sadd_ttl(Poolname,Key,ValuePairs,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			case eredis:qp(Client, [["SADD",Key | ValuePairs],["EXPIRE",Key,Seconds]], ?TIMEOUT) of
				{error,R1}->
					{error,R1};
				[Response | _] ->
					Response
			end;
		R->
			R
	end.
sismember(Poolname,Key,Member)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SISMEMBER",Key,Member], ?TIMEOUT);
		R->
			R
	end.
srem(Poolname,Key,ValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SREM",Key | ValuePairs], ?TIMEOUT);
		R->
			R
	end.
spop(Poolname,Key)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["SPOP",Key], ?TIMEOUT);
		R->
			R
	end.
smembers(Poolname,Key)->
	sscan(Poolname,Key).
	%% 改用游标，根本解决方式，还是尽量避免出现大key
%% 	case ems:get_client_read(Poolname,Key) of
%% 		{ok,Client}->
%% 			eredis:q(Client, ["SMEMBERS",Key], ?TIMEOUT);
%% 		R->
%% 			R
%% 	end.

zadd(Poolname,Key,ScoreMemberPairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["ZADD",Key | ScoreMemberPairs], ?TIMEOUT);
		R->
			R
	end.
zadd_ttl(Poolname,Key,ScoreMemberPairs,Seconds)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			case eredis:qp(Client, [["ZADD",Key | ScoreMemberPairs],["EXPIRE",Key,Seconds]], ?TIMEOUT) of
				{error,R1}->
					{error,R1};
				[Response | _] ->
					Response
			end;
		R->
			R
	end.
zrange(Poolname,Key)->
	zscan(Poolname,Key).
zrange(Poolname,Key,Start,Stop)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["ZRANGE",Key,Start,Stop], ?TIMEOUT);
		R->
			R
	end.
zrem(Poolname,Key,MemberPairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["ZREM",Key | MemberPairs], ?TIMEOUT);
		R->
			R
	end.

%% 所有keys
keys(Poolname,Key)->
	scan(Poolname,Key).

%% 代替 keys *
scan(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			scan(Client,0,sets:new());
		R->
			R
	end.
scan(_Client,<<"0">>,Sets)->{ok,sets:to_list(Sets)};
scan(Client,Cursor,Sets)->
	case eredis:q(Client, ["SCAN",Cursor,"COUNT",?DEFAULT_SCAN_COUNT], ?TIMEOUT) of
		{ok,[New_Cursor,New_List]}->
			%% [K,....]
			New_Sets = lists:foldl(fun(E,Temp_Sets)-> 
				sets:add_element(E, Temp_Sets) 					
			end,Sets,New_List),
			scan(Client,New_Cursor,New_Sets);
		R->
			{error,R}
	end.

sscan(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			sscan(Client,Key,0,sets:new());
		R->
			R
	end.
sscan(_Client,_Key,<<"0">>,Sets)->{ok,sets:to_list(Sets)};
sscan(Client,Key,Cursor,Sets)->
	case eredis:q(Client, ["SSCAN",Key,Cursor,"COUNT",?DEFAULT_SCAN_COUNT], ?TIMEOUT) of
		{ok,[New_Cursor,New_List]}->
			%% [V,....]
			New_Sets = lists:foldl(fun(E,Temp_Sets)-> 
				sets:add_element(E, Temp_Sets) 					
			end,Sets,New_List),
			sscan(Client,Key,New_Cursor,New_Sets);
		R->
			{error,R}
	end.

zscan(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			zscan(Client,Key,0,[],dict:new());
		R->
			R
	end.
zscan(_Client,_Key,<<"0">>,List,_Dict)->{ok,List};
zscan(Client,Key,Cursor,List,Dict)->
	case eredis:q(Client, ["ZSCAN",Key,Cursor,"COUNT",?DEFAULT_SCAN_COUNT], ?TIMEOUT) of
		{ok,[New_Cursor,List1]}->
			%% [V,Score,....]
			{New_List,New_Dict} = list_to_pair(List1,List,Dict),
			zscan(Client,Key,New_Cursor,New_List,New_Dict);
		R->
			{error,R}
	end.
list_to_pair([],List,Dict)->{List,Dict};
list_to_pair([_],List,Dict)->{List,Dict};
list_to_pair([V,S | T],List,Dict)->
	case dict:is_key(V, Dict) of
		false->
			list_to_pair(T,List ++ [{V,S}],dict:store(V,S,Dict));
		true->
			list_to_pair(T,List,Dict)
	end.

hscan(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			hscan(Client,Key,0,dict:new());
		R->
			R
	end.
hscan(_Client,_Key,<<"0">>,Dict)->{ok,dict:to_list(Dict)};
hscan(Client,Key,Cursor,Dict)->
	case eredis:q(Client, ["HSCAN",Key,Cursor,"COUNT",?DEFAULT_SCAN_COUNT], ?TIMEOUT) of
		{ok,[New_Cursor,New_List]}->
			%% [K,V,...]
			New_Dict = list_to_dict(New_List,Dict),
			hscan(Client,Key,New_Cursor,New_Dict);
		R->
			{error,R}
	end.
list_to_dict([],Dict)->Dict;
list_to_dict([_],Dict)->Dict;
list_to_dict([K,V|T],Dict)->
	list_to_dict(T,dict:store(K, V, Dict)).
	

%% eredis_ms:init(20).
%% eredis_ms:test().
%% 
%% Redis v2.8+
%% SCAN 命令用于迭代当前数据库中的数据库键。 --- 取代keys命令    --- sets
%% SSCAN 命令用于迭代集合键中的元素。                         --- sets
%% HSCAN 命令用于迭代哈希键中的键值对。                       --- dict
%% ZSCAN 命令用于迭代有序集合中的元素（包括元素成员和元素分值）   --- lists + dict
%% 多线程下使用redis的scan操作需要使用一个连接遍历完Cursor，而不能复用连接，否则导致报错Unknown reply.
%% 
%% 迭代过程，只能用同一个连接。
%% 同一个元素可能会被返回多次，程序需去重。
%% 迭代过程中一直在的元素，一定会返回。
%% 迭代过程中加入、删除的元素，可能不会返回。
%% 只要游标不是0(返回元素可能为空)，就需要一致迭代下去。
test()->
	{T1,{ok,L1}} = timer:tc(eredis_ms,scan,[redis_pool,<<"a">>]),
	io:format("scan-------->~pms -- ~p~n",[T1 div 1000,length(L1)]),
	{T2,{ok,L2}} = timer:tc(eredis_ms,sscan,[redis_pool,<<"test:set:test">>]),
	io:format("sscan-------->~pms -- ~p~n",[T2 div 1000,length(L2)]),
	{T3,{ok,L3}} = timer:tc(eredis_ms,hscan,[redis_pool,<<"test:hash:test">>]),
	io:format("hscan-------->~pms -- ~p~n",[T3 div 1000,length(L3)]),
	{T4,{ok,L4}} = timer:tc(eredis_ms,zscan,[redis_pool,<<"test:zset:test">>]),
	io:format("zscan-------->~pms -- ~p~n",[T4 div 1000,length(L4)]),
	ok.
init(Num)->
	K1 = "test:string:test:",
	lists:foreach(fun(I)-> 
		K = list_to_binary(lists:concat([K1,I])),	
		Value = list_to_binary(lists:concat(["v_",I])),	
		set(redis_pool,K,Value)					  
	end,lists:seq(1,Num)),
	
	K2 = <<"test:hash:test">>,
	lists:foreach(fun(I)-> 
		Field = list_to_binary(lists:concat(["k_",I])),	
		Value = list_to_binary(lists:concat(["v_",I])),	
		hset(redis_pool,K2,Field,Value)					  
	end,lists:seq(1,Num)),
	
	%% SSCAN 命令用于迭代集合键中的元素。
	K3 = <<"test:set:test">>,
	lists:foreach(fun(I)-> 
		Value = list_to_binary(lists:concat(["v_",I])),	
		sadd(redis_pool,K3,[Value])	
	end,lists:seq(1,Num)),
	
	%% SSCAN 命令用于迭代集合键中的元素。
	K4 = <<"test:zset:test">>,
	lists:foreach(fun(I)-> 
		Value = list_to_binary(lists:concat(["v_",I])),	
		zadd(redis_pool,K4,[I,Value])		  				  
	end,lists:seq(1,Num)),
	
	ok.


%% 启动方法
start()->
	%% 含连接从节点过程。
	ok = start(?MODULE),
	ok.
%% 启动App
start(App) ->
    start_ok(App, application:start(App, permanent)).
start_ok(_App, ok) -> ok;
start_ok(_App, {error, {already_started, _App}}) -> ok;
start_ok(App, {error, {not_started, Dep}}) ->
    ok = start(Dep),
    start(App);
start_ok(App, {error, Reason}) ->
    erlang:error({aps_start_failed, App, Reason}).