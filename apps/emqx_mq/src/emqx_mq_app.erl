%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_app).
-moduledoc """
Root application supervisor.

Application startup is separated in 2 phases, both of them are orchestrated
by this module:
1. Initialization: setting up supervision tree, creating DBs.
2. Post-start: awaiting DBs readiness, starting rest of the supervision tree
   that relies on it, registering hooks, making API accessible.

Second phase runs asynchronously in relation to the application startup, and
does not block node boot sequence. Use `is_ready/0` and/or `wait_readiness/1`
to find out if full application functionality is available.

This is a temporary solution that is meant to work around a problem related to
the EMQX startup sequence: before cluster discovery begins, EMQX waits for the
full startup of the applications. It cannot occur if the DS DBs are configured
to wait for a certain number of replicas.
""".

-behaviour(application).

-export([start/2, stop/1]).
-export([is_ready/0, wait_readiness/1]).

-export([start_link_post_start/0]).
-export([post_start/0]).

-define(OPTVAR_READY, emqx_mq_sup_ready).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    ok = mria:wait_for_tables(emqx_mq_registry:create_tables()),
    ok = emqx_mq_message_db:open(),
    ok = emqx_mq_state_storage:open_db(),
    {ok, Sup} = emqx_mq_sup:start_link(),
    {ok, _} = emqx_mq_sup:start_post_starter({?MODULE, start_link_post_start, []}),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_mq:unregister_hooks(),
    ok = emqx_mq_message_db:close(),
    ok = emqx_mq_state_storage:close_db(),
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
    ok = emqx_mq_message_db:wait_readiness(infinity),
    ok = emqx_mq_state_storage:wait_readiness(infinity),
    complete_start(),
    optvar:set(?OPTVAR_READY, true).

complete_start() ->
    ok = emqx_mq_sup:start_gc_scheduler(),
    ok = emqx_mq:register_hooks().
