%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This supervisor manages the global worker processes needed for
%% the functioning of builtin local databases, and all builtin local
%% databases that attach to it.
-module(emqx_ds_builtin_local_sup).

-behaviour(supervisor).

%% API:
-export([start_db/4, stop_db/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_top/0, start_databases_sup/0]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(top, ?MODULE).
-define(databases, emqx_ds_builtin_local_db_sup).

%%================================================================================
%% API functions
%%================================================================================

-spec start_top() -> {ok, pid()}.
start_top() ->
    supervisor:start_link({local, ?top}, ?MODULE, ?top).

-spec start_db(
    emqx_ds:db(),
    boolean(),
    emqx_ds_builtin_local:db_schema(),
    emqx_ds_builtin_local:db_runtime_config()
) ->
    supervisor:startchild_ret().
start_db(DB, Create, Schema, RTOpts) ->
    supervisor:start_child(?databases, [DB, Create, Schema, RTOpts]).

-spec stop_db(emqx_ds:db()) -> ok.
stop_db(DB) ->
    maybe
        Sup = whereis(?databases),
        true ?= is_pid(Sup),
        DBPid = emqx_ds_builtin_local_db_sup:whereis_db(DB),
        true ?= is_pid(DBPid),
        supervisor:terminate_child(Sup, DBPid),
        ok
    else
        _ -> ok
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% There are two layers of supervision:
%%
%% 1. top supervisor for the builtin backend. It contains the global
%% worker processes (like the metadata server), and `?databases'
%% supervisior.
%%
%% 2. `?databases': a `one_for_one' supervisor where each child is a
%% `db' supervisor that contains processes that represent the DB.
%% Chidren are attached dynamically to this one.
init(?top) ->
    %% Children:
    MetadataServer = #{
        id => metadata_server,
        start => {emqx_ds_builtin_local_meta, start_link, []},
        restart => permanent,
        type => worker,
        shutdown => 5000
    },
    DBsSup = #{
        id => ?databases,
        start => {?MODULE, start_databases_sup, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity
    },
    %%
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 1,
        auto_shutdown => never
    },
    {ok, {SupFlags, [MetadataServer, DBsSup]}};
init(?databases) ->
    %% Children are added dynamically:
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 1
    },
    Children = [
        #{
            id => db,
            start => {?databases, start_link_db, []},
            type => supervisor,
            shutdown => infinity
        }
    ],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal exports
%%================================================================================

start_databases_sup() ->
    supervisor:start_link({local, ?databases}, ?MODULE, ?databases).

%%================================================================================
%% Internal functions
%%================================================================================
