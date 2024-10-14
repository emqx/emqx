-module(alinkdata).
-export([connect/1]).

-export([start/0, query_from_mysql/2, query_from_redis/2]).

-export([query_mysql_format_map/2, format_result_to_map/1]).

-export([ensure_start_resource/2]).

-define(POOL(Type, Name), list_to_atom(lists:concat([Type, Name]))).

start() ->
    ensure_start_resource(mysql, application:get_env(alinkdata, mysql, [])).
%%    start(redis, application:get_env(alinkdata, redis, [])).


query_from_mysql(Pool, SQL) ->
    poolman:transaction(?POOL(mysql, Pool),
        fun(Pid) ->
            logger:debug("query ~s", [SQL]),
            mysql:query(Pid, SQL, 30000)
        end).

query_mysql_format_map(Pool, SQL) ->
    format_result_to_map(query_from_mysql(Pool, SQL)).

query_from_redis(Pool, Command) ->
    poolman:transaction(?POOL(redis, Pool),
        fun(Pid) ->
            eredis:q(Pid, Command)
        end).

ensure_start_resource(_, []) -> ok;
ensure_start_resource(Type, [Opt | Opts]) ->
    Name = proplists:get_value(pool, Opt, default),
    PoolSize = proplists:get_value(pool_size, Opt, 1),
    PoolArgs = [
        {size, PoolSize},
        {auto_size, true},
        {pool_type, random},
        {worker_module, {con, ?MODULE}}
    ],
    WorkerArgs = [Type, Opt],
    Spec = poolman:pool_spec(?POOL(Type, Name), PoolArgs, WorkerArgs),
   {ok, _} = supervisor:start_child(alinkdata_sup, Spec),
    ensure_start_resource(Type, Opts).

connect([mysql, Opts]) ->
    mysql:start_link(Opts);
connect([redis, Opts]) ->
    eredis:start_link([{reconnect_sleep, no_reconnect} | Opts]).




format_result_to_map(ok) ->
    ok;
format_result_to_map({ok, ColNames, Datas}) ->
    NDatas =
        lists:map(
            fun(Data) ->
                maps:from_list(lists:zip(ColNames, Data))
            end, Datas),
    {ok, NDatas};
format_result_to_map({ok, QueryDatas}) ->
    NQueryDatas =
        lists:map(
            fun({ColNames, Datas}) ->
                lists:map(
                    fun(Data) ->
                        maps:from_list(lists:zip(ColNames, Data))
                    end, Datas)
            end, QueryDatas),
    {ok, NQueryDatas};
format_result_to_map(Other) ->
    Other.
