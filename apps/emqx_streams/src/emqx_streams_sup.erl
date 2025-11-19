%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_sup).

-export([
    start_link/0,
    start_post_starter/1,
    start_metrics/0
]).

-behaviour(supervisor).
-export([init/1]).

-define(ROOT_SUP, ?MODULE).

%%

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

start_post_starter(MFA) ->
    supervisor:start_child(?ROOT_SUP, post_start_child_spec(MFA)).

start_metrics() ->
    ensure_child(?ROOT_SUP, emqx_streams_metrics:child_spec()).

%%

init(?ROOT_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

post_start_child_spec(MFA) ->
    #{
        id => post_start,
        start => MFA,
        restart => transient,
        type => worker,
        shutdown => brutal_kill
    }.

ensure_child(SupRef, ChildSpec) ->
    case supervisor:start_child(SupRef, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
