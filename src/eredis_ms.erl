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
	hget/3,
	hexists/3,
	hlen/2,
	hmset/3,
	hmget/3,
	hdel/3,
	hgetall/2,
	
	lpush/3,
	lpushx/3,
	rpush/3,
	rpushx/3,
	lpop/2,
	rpop/2,
	llen/2,
	lrange/4,
	
	sadd/3,
	srem/3,
	spop/2,
	smembers/2
]).
-define(TIMEOUT, 5000).


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
			%% 判断大小，改用游标

			eredis:q(Client, ["HDEL",Key | FieldPairs], ?TIMEOUT);
		R->
			R
	end.
hgetall(Poolname,Key)->
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			%% 改用游标
			
			eredis:q(Client, ["HGETALL",Key], ?TIMEOUT);
		R->
			R
	end.
lpush(Poolname,Key,ValuePairs)->
	case ems:get_client_write(Poolname,Key) of
		{ok,Client}->
			eredis:q(Client, ["LPUSH",Key | ValuePairs], ?TIMEOUT);
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
	case ems:get_client_read(Poolname,Key) of
		{ok,Client}->
			%% 改成游标
			
			eredis:q(Client, ["SMEMBERS",Key], ?TIMEOUT);
		R->
			R
	end.


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