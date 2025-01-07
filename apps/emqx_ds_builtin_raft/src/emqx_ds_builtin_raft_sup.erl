%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This supervisor manages the global worker processes needed for
%% the functioning of builtin databases, and all builtin database
%% attach to it.
-module(emqx_ds_builtin_raft_sup).

-behaviour(supervisor).

%% API:
-export([start_top/0, start_db/2, stop_db/1, which_dbs/0]).
-export([set_gvar/3, get_gvar/3, clean_gvars/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_databases_sup/0]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(top, ?MODULE).
-define(databases, emqx_ds_builtin_databases_sup).
-define(gvar_tab, emqx_ds_builtin_gvar).

-record(gvar, {
    k :: {emqx_ds:db(), _Key},
    v :: _Value
}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_top() -> {ok, pid()}.
start_top() ->
    supervisor:start_link({local, ?top}, ?MODULE, ?top).

-spec start_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    supervisor:startchild_ret().
start_db(DB, Opts) ->
    ChildSpec = #{
        id => DB,
        start => {emqx_ds_builtin_raft_db_sup, start_db, [DB, Opts]},
        type => supervisor,
        shutdown => infinity
    },
    supervisor:start_child(?databases, ChildSpec).

-spec stop_db(emqx_ds:db()) -> ok.
stop_db(DB) ->
    case whereis(?databases) of
        Pid when is_pid(Pid) ->
            _ = supervisor:terminate_child(?databases, DB),
            _ = supervisor:delete_child(?databases, DB),
            clean_gvars(DB);
        undefined ->
            ok
    end.

-spec which_dbs() -> {ok, [emqx_ds:db()]} | {error, inactive}.
which_dbs() ->
    case whereis(?databases) of
        Pid when is_pid(Pid) ->
            [DB || {DB, _Child, _, _} <- supervisor:which_children(Pid)];
        undefined ->
            {error, inactive}
    end.

%% @doc Set a DB-global variable. Please don't abuse this API.
-spec set_gvar(emqx_ds:db(), _Key, _Val) -> ok.
set_gvar(DB, Key, Val) ->
    ets:insert(?gvar_tab, #gvar{k = {DB, Key}, v = Val}),
    ok.

-spec get_gvar(emqx_ds:db(), _Key, Val) -> Val.
get_gvar(DB, Key, Default) ->
    case ets:lookup(?gvar_tab, {DB, Key}) of
        [#gvar{v = Val}] ->
            Val;
        [] ->
            Default
    end.

-spec clean_gvars(emqx_ds:db()) -> ok.
clean_gvars(DB) ->
    ets:match_delete(?gvar_tab, #gvar{k = {DB, '_'}, _ = '_'}),
    ok.

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
        start => {emqx_ds_replication_layer_meta, start_link, []},
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
    _ = ets:new(?gvar_tab, [named_table, set, public, {keypos, #gvar.k}, {read_concurrency, true}]),
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
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, []}}.

%%================================================================================
%% Internal exports
%%================================================================================

start_databases_sup() ->
    supervisor:start_link({local, ?databases}, ?MODULE, ?databases).

%%================================================================================
%% Internal functions
%%================================================================================
