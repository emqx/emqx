%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_db_group_mgr).
-moduledoc """
This server is responsible for tracking "DB groups":
entities that are mostly used for setting rate limits and qoutas for DBs.
""".

-behavior(gen_server).

%% API:
-export([
    start_link/0,
    setup_group/2,
    destroy/1,
    on_backend_stop/1,
    autoclean/1,
    attach/3,
    detach/1,
    lookup/1
]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(gtab, emqx_ds_db_group_mgr_gtab).

-record(call_setup, {
    id :: emqx_ds:db_group(),
    opts :: emqx_ds:db_group_opts()
}).

-record(call_destroy, {
    id :: emqx_ds:db_group()
}).

-record(call_on_backend_stop, {
    backend :: emqx_ds:backend()
}).

-record(call_attach, {
    db :: emqx_ds:db(),
    backend :: emqx_ds:backend(),
    group :: emqx_ds:db_group()
}).

-record(call_detach, {
    db :: emqx_ds:db()
}).

-record(grp, {
    id :: emqx_ds:db_group(),
    backend :: emqx_ds:backend(),
    inner :: term()
}).

-record(s, {
    dbs = #{} :: #{emqx_ds:db() => emqx_ds:db_group()}
}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec setup_group(emqx_ds:db_group(), emqx_ds:db_group_opts()) -> ok | {error, _}.
setup_group(Group, #{backend := _} = Opts) ->
    gen_server:call(?SERVER, #call_setup{id = Group, opts = Opts}, infinity);
setup_group(_, _) ->
    {error, badarg}.

-spec destroy(emqx_ds:db_group()) -> ok | {error, _}.
destroy(Group) ->
    gen_server:call(?SERVER, #call_destroy{id = Group}, infinity).

-doc """
This API is intended for the backends:
they should call it during shutdown to release all DB groups owned.
""".
-spec on_backend_stop(emqx_ds:backend()) -> ok.
on_backend_stop(Backend) ->
    gen_server:call(?SERVER, #call_on_backend_stop{backend = Backend}, infinity).

-doc """
Helper function that creates a supervisor child spec for a process
that will call `on_backend_stop` function. Backend can attack this
child spec into its supervisor tree in a way that it will fire
automatically when all DBs are closed.
""".
autoclean(Backend) ->
    emqx_ds_lib:autoclean(
        db_group_autoclean,
        15_000,
        fun() ->
            ok
        end,
        fun() ->
            on_backend_stop(Backend)
        end
    ).

-doc """
Backend API: attach a DB to the group.

Preconditions:

1. Group must exist
2. The group's backend must be equal to the backend of the DB
""".
-spec attach(emqx_ds:db(), emqx_ds:backend(), emqx_ds:db_group()) -> ok | {error, _}.
attach(DB, Backend, GroupId) ->
    gen_server:call(?SERVER, #call_attach{db = DB, backend = Backend, group = GroupId}).

-doc """
Backend API: detach a DB from the group.
This function is called when the DB is being closed.
""".
-spec detach(emqx_ds:db()) -> ok | {error, _}.
detach(DB) ->
    gen_server:call(?SERVER, #call_detach{db = DB}).

-doc """
Backend API: get backend-specific data of the group.
""".
-spec lookup(emqx_ds:db_group()) -> {ok, _Inner} | undefined.
lookup(Group) ->
    case ets:lookup(?gtab, Group) of
        [#grp{inner = Inner}] ->
            {ok, Inner};
        [] ->
            undefined
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    _ = ets:new(?gtab, [set, protected, named_table, {keypos, #grp.id}]),
    {ok, #s{}}.

handle_call(#call_setup{id = Id, opts = Opts}, _From, S0) ->
    {Result, S} = handle_setup(Id, Opts, S0),
    {reply, Result, S};
handle_call(#call_destroy{id = Group}, _From, S0) ->
    {Result, S} = handle_destroy(Group, S0),
    {reply, Result, S};
handle_call(#call_on_backend_stop{backend = Backend}, _From, S0) ->
    S = handle_backend_stop(Backend, S0),
    {reply, ok, S};
handle_call(
    #call_attach{db = DB, backend = Backend, group = Group}, _From, S = #s{dbs = D0}
) ->
    case ets:lookup(?gtab, Group) of
        [#grp{backend = Backend}] ->
            D = D0#{DB => Group},
            {reply, ok, S#s{dbs = D}};
        [#grp{backend = Other}] ->
            Err = {error, {backend_mismatch, Other}},
            {reply, Err, S};
        [] ->
            Err = {error, no_such_group},
            {reply, Err, S}
    end;
handle_call(#call_detach{db = DB}, _From, S = #s{dbs = D0}) ->
    D = maps:remove(DB, D0),
    {reply, ok, S#s{dbs = D}};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
    {stop, shutdown, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _) ->
    %% Note: we cannot delete the groups automatically here, because
    %% this function is called when `emqx_durable_storage' app is
    %% shutting down, but the groups are owned by the backends. At
    %% this point we don't know anything about the state of the
    %% backends (they are likely stopped). So we just complain:
    lists:foreach(
        fun(#grp{id = Id, backend = Backend}) ->
            ?tp(warning, "leftover_db_group", #{id => Id, backend => Backend})
        end,
        ets:tab2list(?gtab)
    ),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

handle_setup(Id, Opts, S0) ->
    #{backend := Backend} = Opts,
    case ets:lookup(?gtab, Id) of
        [Grp0 = #grp{backend = Backend, inner = Inner0}] ->
            %% Group already exists and backend matches.
            maybe
                {ok, Mod} ?= emqx_dsch:get_backend_cbm(Backend),
                {ok, Inner} ?= safe_call(Mod, update_db_group, [Id, Opts, Inner0]),
                Grp = Grp0#grp{inner = Inner},
                ets:insert(?gtab, Grp),
                {ok, S0}
            else
                Err ->
                    {Err, S0}
            end;
        [#grp{backend = Old}] ->
            case handle_destroy(Id, S0) of
                {ok, S} ->
                    handle_setup(Id, Opts, S);
                _ ->
                    {{error, {backend_mismatch, Old}}, S0}
            end;
        [] ->
            %% Entirely new
            maybe
                {ok, Mod} ?= emqx_dsch:get_backend_cbm(Backend),
                {ok, Inner} ?= safe_call(Mod, create_db_group, [Id, Opts]),
                Grp = #grp{id = Id, backend = Backend, inner = Inner},
                ets:insert(?gtab, Grp),
                {ok, S0}
            else
                Err ->
                    {Err, S0}
            end
    end.

handle_backend_stop(Backend, S0) ->
    MS = {#grp{backend = Backend, _ = '_'}, [], ['$_']},
    lists:foldl(
        fun(#grp{id = Group}, S1) ->
            {_, S} = handle_destroy(Group, S1),
            S
        end,
        S0,
        ets:select(?gtab, [MS])
    ).

handle_destroy(Id, S) ->
    maybe
        [#grp{backend = Backend, inner = Inner}] ?= ets:take(?gtab, Id),
        true ?= group_is_empty(Id, S),
        {ok, Mod} ?= emqx_dsch:get_backend_cbm(Backend),
        ok ?= do_destroy(Mod, Id, Inner),
        {ok, S}
    else
        [] ->
            {{error, no_such_group}, S};
        Err ->
            {Err, S}
    end.

group_is_empty(Group, S) ->
    case dbs_of_group(Group, S) of
        [] ->
            true;
        [_ | _] ->
            {error, group_is_not_empty}
    end.

dbs_of_group(Group, #s{dbs = DBs}) ->
    maps:fold(
        fun(DB, G, Acc) ->
            case G of
                Group -> [DB | Acc];
                _ -> Acc
            end
        end,
        [],
        DBs
    ).

do_destroy(Mod, Id, Inner) ->
    safe_call(Mod, destroy_db_group, [Id, Inner]).

safe_call(Mod, Fun, Args) ->
    try
        %% Note: this callback is defined in `emqx_ds' behavior
        apply(Mod, Fun, Args)
    catch
        EC:Err:Stack ->
            ?tp(error, "ds_db_group_error", #{
                EC => Err, stack => Stack, call => {Mod, Fun, Args}
            }),
            {error, internal_error}
    end.
