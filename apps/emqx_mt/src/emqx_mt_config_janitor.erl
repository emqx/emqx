%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_config_janitor).

-moduledoc """
This process is responsible for cleaning up a namespace's resources asynchronously after
it is deleted.
""".

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    clean_namespace/1
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include("emqx_mt_internal.hrl").
-include_lib("emqx/include/logger.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(CHECK_INTERVAL_MS, 15_000).

-define(check_timer, check_timer).

%% Calls/casts/infos/continues
-record(clean_tombstones, {}).
-record(clean_namespace, {namespace}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _Opts = #{}, []).

clean_namespace(Namespace) ->
    Core = do_pick_core_for(Namespace),
    gen_server:cast({?MODULE, Core}, #clean_namespace{namespace = Namespace}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    case mria_rlog:role() of
        core ->
            State = #{
                ?check_timer => undefined
            },
            {ok, State, {continue, #clean_tombstones{}}};
        replicant ->
            ignore
    end.

terminate(_Reason, _State) ->
    ok.

handle_continue(#clean_tombstones{}, State0) ->
    ok = handle_cleanup_tombstones(),
    State = ensure_check_timer(State0),
    {noreply, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#clean_namespace{namespace = Namespace}, State) ->
    ok = handle_clean_namespace(Namespace),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, TRef, #clean_tombstones{}}, #{?check_timer := TRef} = State0) ->
    ok = handle_cleanup_tombstones(),
    State = ensure_check_timer(State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_cleanup_tombstones() ->
    Namespaces = enumerate_deleted_namespaces(),
    lists:foreach(fun do_clean_up/1, Namespaces).

handle_clean_namespace(Namespace) ->
    maybe
        true ?= emqx_mt_state:is_tombstoned(Namespace),
        do_clean_up(Namespace)
    else
        false -> ok
    end.

enumerate_deleted_namespaces() ->
    ThisNode = node(),
    Cores = mk_core_index(),
    lists:filter(
        fun(Namespace) ->
            ThisNode == do_pick_core_for(Namespace, Cores)
        end,
        emqx_mt_state:tombstoned_namespaces()
    ).

do_pick_core_for(Namespace) ->
    Cores = mk_core_index(),
    do_pick_core_for(Namespace, Cores).

do_pick_core_for(Namespace, Cores) ->
    N = map_size(Cores),
    I = erlang:phash2(Namespace, N),
    maps:get(I, Cores).

mk_core_index() ->
    Cores0 = lists:sort(mria:cluster_nodes(cores)),
    maps:from_list(lists:enumerate(0, Cores0)).

do_clean_up(Namespace) ->
    RootConfigs = emqx_config:get_all_roots_from_namespace(Namespace),
    AllRootKeys = maps:keys(RootConfigs),
    %% We destroy terminal root keys before initial ones.
    SortedRootKeys = lists:reverse(emqx_config_dep_registry:sorted_root_keys()),
    %% These don't have a specified order in which to be destroyed
    OtherRootKeys = AllRootKeys -- SortedRootKeys,
    lists:foreach(
        fun(RootKey) ->
            ok = do_clean_up(Namespace, RootKey)
        end,
        SortedRootKeys
    ),
    lists:foreach(
        fun(RootKey) ->
            ok = do_clean_up(Namespace, RootKey)
        end,
        OtherRootKeys
    ),
    ok = emqx_hooks:run('namespace.delete', [Namespace]),
    ok = emqx_config:erase_namespaced_configs(Namespace),
    ok = emqx_mt_state:clear_tombstone(Namespace),
    ok.

do_clean_up(Namespace, RootKey) ->
    %% Set to empty, so that any config cleanup callbacks are triggered.
    UpdateRes =
        emqx_conf:update(
            [RootKey],
            #{},
            #{namespace => Namespace}
        ),
    case UpdateRes of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "mt_failed_to_delete_namespaced_config",
                namespace => Namespace,
                root_key => RootKey,
                reason => Reason
            }),
            ok
    end.

ensure_check_timer(#{?check_timer := TRef0} = State0) ->
    emqx_utils:cancel_timer(TRef0),
    TRef = emqx_utils:start_timer(?CHECK_INTERVAL_MS, self(), #clean_tombstones{}),
    State0#{?check_timer := TRef}.
