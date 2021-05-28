%%%-------------------------------------------------------------------
%% @doc emqx_resource top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_resource_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(RESOURCE_INST_MOD, emqx_resource_instance).
-define(POOL_SIZE, 64). %% set a very large pool size in case all the workers busy

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    TabOpts = [named_table, set, public, {read_concurrency, true}],
    _ = ets:new(emqx_resource_instance, TabOpts),

    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Pool = ?RESOURCE_INST_MOD,
    Mod = ?RESOURCE_INST_MOD,
    ensure_pool(Pool, hash, [{size, ?POOL_SIZE}]),
    {ok, {SupFlags, [
        begin
            ensure_pool_worker(Pool, {Pool, Idx}, Idx),
            #{id => {Mod, Idx},
              start => {Mod, start_link, [Pool, Idx]},
              restart => transient,
              shutdown => 5000, type => worker, modules => [Mod]}
        end || Idx <- lists:seq(1, ?POOL_SIZE)]}}.

%% internal functions
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