-module(emqx_http_client_sup).

-behaviour(supervisor).

-export([ start_link/2
        , init/1
        , stop_pool/1
        ]).

start_link(Pool, Opts) ->
    supervisor:start_link(?MODULE, [Pool, Opts]).

init([Pool, Opts]) ->
    PoolSize = pool_size(Opts),
    ok = ensure_pool(Pool, random, [{size, PoolSize}]),
    {ok, {{one_for_one, 10, 100}, [
        begin
            ensure_pool_worker(Pool, {Pool, I}, I),
            #{id => {Pool, I},
              start => {emqx_http_client, start_link, [Pool, I, Opts]},
              restart => transient,
              shutdown => 5000,
              type => worker,
              modules => [emqx_http_client]}
        end || I <- lists:seq(1, PoolSize)]}}.


ensure_pool(Pool, Type, Opts) ->
    try gproc_pool:new(Pool, Type, Opts)
    catch
        error:exists -> ok
    end.

ensure_pool_worker(Pool, Name, Slot) ->
    try gproc_pool:add_worker(Pool, Name, Slot)
    catch
        error:exists -> ok
    end.

pool_size(Opts) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Opts, Schedulers).

stop_pool(Name) ->
    Workers = gproc_pool:defined_workers(Name),
    _ = [gproc_pool:remove_worker(Name, WokerName) || {WokerName, _, _} <- Workers],
    gproc_pool:delete(Name),
    ok.
