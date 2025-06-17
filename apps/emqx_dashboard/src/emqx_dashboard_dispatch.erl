%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_dispatch).

-moduledoc """
This module is used to regenerate dispatch (cowboy routes and swagger specs)
for the dashboard HTTP servers.

Its main responsibilities are:
* Serve dispatch regeneration requests from pre/post_config_update hooks, when
    the regeneration should be delayed until the configuration is fully applied.

* Serialize dispatch regeneration to avoid a situation when a not up-to-date dispatch
    was applied due to a race condition.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    regenerate_dispatch/1,
    regenerate_dispatch_after_config_update/1
]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(regenerate_dispatch, {
    listeners :: [emqx_dashboard:listener_name()]
}).

-record(regenerate_dispatch_after_config_update, {
    listeners :: [emqx_dashboard:listener_name()]
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Regenerate the dispatch for the given listeners immediately and synchronously.
""".
-spec regenerate_dispatch([emqx_dashboard:listener_name()]) -> ok.
regenerate_dispatch(Listeners) ->
    gen_server:call(?SERVER, #regenerate_dispatch{listeners = Listeners}, infinity).

-doc """
Regenerate the dispatch for the given listeners asynchronously after the current
config update is completed.
""".
-spec regenerate_dispatch_after_config_update([emqx_dashboard:listener_name()]) -> ok.
regenerate_dispatch_after_config_update(Listeners) ->
    gen_server:cast(?SERVER, #regenerate_dispatch_after_config_update{listeners = Listeners}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call(#regenerate_dispatch{listeners = Listeners}, _From, State) ->
    ok = do_regenerate_dispatch(Listeners),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(#regenerate_dispatch_after_config_update{listeners = Listeners}, State) ->
    ok = wait_for_config_update(),
    ok = do_regenerate_dispatch(Listeners),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_regenerate_dispatch(ListenerNames) ->
    {Time, ok} = timer:tc(fun() ->
        lists:foreach(
            fun(Name) ->
                ok = minirest:update_dispatch(Name)
            end,
            ListenerNames
        )
    end),
    ?tp(info, regenerate_dispatch, #{
        elapsed_ms => erlang:convert_time_unit(Time, microsecond, millisecond),
        i18n_lang => emqx:get_config([dashboard, i18n_lang]),
        listeners => ListenerNames
    }),
    ok.

%% We make sure that a config update that could cause
%% the `regenerate_dispatch_after_config_update` request is completed.
wait_for_config_update() ->
    _ = emqx_config_handler:info(),
    ok.
