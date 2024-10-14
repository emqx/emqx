-module(alinkdata_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 10},
    DaoCacheSpec = child_spec(alinkdata_dao_cache, worker),
    WechatSpec = child_spec(alinkdata_wechat, worker, []),
    DataCache = child_spec(alinkdata_cache, worker),
    HookSpec = child_spec(alinkdata_hooks, worker),
    BatchSpec = child_spec(alinkdata_batch_log, worker),
    AsyncWorkerSpec = child_spec(alinkdata_async_worker, worker),
    MetricsSpec = child_spec(alinkdata_metrics, worker),
    SceneSpec = child_spec(alinkdata_scene_data, worker),
    SceneSupSpec = child_spec(alinkdata_scene_worker_sup, supervisor),
    ChildSpecs = lists:flatten([DaoCacheSpec, WechatSpec, DataCache, HookSpec, BatchSpec, AsyncWorkerSpec, MetricsSpec, SceneSupSpec, SceneSpec]),
    {ok, {SupFlags, ChildSpecs}}.



child_spec(Mod, WorkerType) ->
    child_spec(Mod, permanent, WorkerType, []).


child_spec(Mod, WorkerType, Args) ->
    child_spec(Mod, permanent, WorkerType, Args).


child_spec(Mod, Strategy, WorkerType, Args) ->
    IsLegal0 = lists:member(WorkerType, [worker, supervisor]),
    IsLegal1 = lists:member(Strategy, [permanent, transient, temporary]),
    case IsLegal0 andalso IsLegal1 of
        true ->
            #{id => Mod,
                start => {Mod, start_link, Args},
                restart => Strategy,
                shutdown => infinity,
                type => WorkerType,
                modules => [Mod]
            };
        false ->
            erlang:error(bad_arg)
    end.
