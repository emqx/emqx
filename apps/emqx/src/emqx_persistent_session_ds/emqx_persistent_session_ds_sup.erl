%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    on_dbs_up/0,
    start_link_system/0
]).

%% `supervisor' API
-export([
    init/1
]).

-include_lib("snabbkaffe/include/trace.hrl").

-define(top, emqx_persistent_session_ds_top_sup).

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?top}, ?MODULE, top).

start_link_system() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, system).

-doc """
This function is called by emqx_persistent_message:handle_continue
when all DS DBs are up.

It starts the processes related to durable sessions.
""".
on_dbs_up() ->
    {ok, _} = supervisor:start_child(?top, #{
        id => system,
        type => supervisor,
        start => {?MODULE, start_link_system, []},
        restart => permanent,
        shutdown => infinity
    }),
    ?tp(notice, durable_sessions_are_ready, #{}).

%%--------------------------------------------------------------------------------
%% `supervisor' API
%%--------------------------------------------------------------------------------

init(top) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 100
    },
    Children = [
        #{
            id => bootstrapper,
            type => worker,
            start => {emqx_persistent_message, start_link, []},
            restart => transient,
            shutdown => 100
        }
    ],
    {ok, {SupFlags, Children}};
init(system) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 2,
        auto_shutdown => never
    },
    CoreNodeChildren = [
        worker(message_gc_worker, emqx_persistent_message_ds_gc_worker, [])
    ],
    AnyNodeChildren = [
        worker(session_bookkeeper, emqx_persistent_session_bookkeeper, [])
    ],
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
