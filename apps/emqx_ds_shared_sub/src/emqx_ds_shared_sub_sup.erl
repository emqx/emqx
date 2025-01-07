%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    on_enable/0,
    on_disable/0
]).

%% supervisor behaviour callbacks
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

on_enable() ->
    ok = emqx_ds_shared_sub_store:open(),
    ensure_started(emqx_ds_shared_sub_registry:child_spec()).

on_disable() ->
    ok = ensure_stopped(emqx_ds_shared_sub_registry:child_spec()),
    emqx_ds_shared_sub_store:close().

%%------------------------------------------------------------------------------

ensure_started(ChildSpec) ->
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, _} = Error ->
            Error
    end.

ensure_stopped(#{id := ChildId}) ->
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        {error, not_found} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% supervisor behaviour callbacks
%%------------------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, []}}.
