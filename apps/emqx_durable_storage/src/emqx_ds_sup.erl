%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0, attach_backend/2]).

%% behaviour callbacks:
-export([init/1]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP, ?MODULE).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, top).

%% @doc Attach a child backend-specific supervisor to the top
%% application supervisor, if not yet present
-spec attach_backend(_BackendId, {module(), atom(), list()}) ->
    {ok, pid()} | {error, _}.
attach_backend(Backend, Start) ->
    Spec = #{
        id => Backend,
        start => Start,
        significant => false,
        shutdown => infinity,
        type => supervisor
    },
    case supervisor:start_child(?SUP, Spec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Err} ->
            {error, Err}
    end.

%%================================================================================
%% behaviour callbacks
%%================================================================================

init(top) ->
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================
