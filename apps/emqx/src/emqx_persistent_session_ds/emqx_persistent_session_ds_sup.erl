%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0
]).

%% `supervisor' API
-export([
    init/1
]).

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------------------
%% `supervisor' API
%%--------------------------------------------------------------------------------

init(Opts) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            do_init(Opts);
        false ->
            ignore
    end.

do_init(_Opts) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 2,
        auto_shutdown => never
    },
    %% CoreNodeChildren = [
    %%     worker(session_gc_worker, emqx_persistent_session_ds_gc_worker, []),
    %%     worker(message_gc_worker, emqx_persistent_message_ds_gc_worker, [])
    %% ],
    AnyNodeChildren = [
        worker(node_heartbeat, emqx_persistent_session_ds_node_heartbeat_worker, [])
    ],
    CoreNodeChildren = [],
    Children =
        case mria_rlog:role() of
            core -> CoreNodeChildren ++ AnyNodeChildren;
            replicant -> AnyNodeChildren
        end,
    {ok, {SupFlags, Children}}.

%%--------------------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------------------

worker(Id, Mod, Args) ->
    #{
        id => Id,
        start => {Mod, start_link, Args},
        type => worker,
        restart => permanent,
        shutdown => 10_000,
        significant => false
    }.
