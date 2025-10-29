%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_app).

-behaviour(application).

-export([start/2, stop/1, do_start/0]).
-export([is_ready/0, wait_readiness/1]).

-export([start_link_post_start/0]).
-export([post_start/0]).

-define(OPTVAR_READY, emqx_streams_app_ready).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_streams_sup:start_link(),
    emqx_conf:add_handler(emqx_streams_schema:roots(), emqx_streams_config),
    emqx_streams_config:is_enabled() andalso do_start(),
    {ok, Sup}.

do_start() ->
    {ok, _} = emqx_streams_sup:start_post_starter({?MODULE, start_link_post_start, []}),
    ok.

stop(_State) ->
    ok = optvar:unset(?OPTVAR_READY),
    ok = emqx_conf:remove_handler(emqx_streams_schema:roots()),
    ok = emqx_streams:unregister_hooks(),
    ok.

%% Readiness

-spec is_ready() -> boolean().
is_ready() ->
    case optvar:peek(?OPTVAR_READY) of
        {ok, _} ->
            true;
        undefined ->
            false
    end.

-spec wait_readiness(timeout()) -> ok | timeout.
wait_readiness(Timeout) ->
    case optvar:read(?OPTVAR_READY, Timeout) of
        {ok, _} ->
            ok;
        timeout ->
            timeout
    end.

%% Post-start phase

start_link_post_start() ->
    {ok, proc_lib:spawn_link(?MODULE, post_start, [])}.

post_start() ->
    complete_start(),
    optvar:set(?OPTVAR_READY, true).

complete_start() ->
    ok = emqx_streams:register_hooks().
