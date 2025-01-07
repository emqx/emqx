%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("stdlib/include/qlc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_statem).

-export([
    start_link/0,
    enable/1,
    enable/2,
    enable/3,
    disable/1,
    disable/2,
    status/0
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    code_change/4
]).

-define(ENABLE_KIND, emqx_node_rebalance).
-define(SERVER_REFERENCE, undefined).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-type status() :: {enabled, pid()} | disabled.

-spec start_link() -> startlink_ret().
start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec enable(pid()) -> ok_or_error(already_enabled | eviction_agent_busy).
enable(CoordinatorPid) ->
    enable(CoordinatorPid, ?ENABLE_KIND).

-spec enable(pid(), emqx_eviction_agent:kind()) ->
    ok_or_error(invalid_coordinator | eviction_agent_busy).
enable(CoordinatorPid, Kind) ->
    enable(CoordinatorPid, Kind, emqx_eviction_agent:default_options()).

-spec enable(pid(), emqx_eviction_agent:kind(), emqx_eviction_agent:options()) ->
    ok_or_error(invalid_coordinator | eviction_agent_busy).
enable(CoordinatorPid, Kind, Options) ->
    gen_statem:call(?MODULE, {enable, CoordinatorPid, Kind, Options}).

-spec disable(pid()) -> ok_or_error(already_disabled | invalid_coordinator).
disable(CoordinatorPid) ->
    disable(CoordinatorPid, ?ENABLE_KIND).

-spec disable(pid(), emqx_eviction_agent:kind()) ->
    ok_or_error(already_disabled | invalid_coordinator).
disable(CoordinatorPid, Kind) ->
    gen_statem:call(?MODULE, {disable, CoordinatorPid, Kind}).

-spec status() -> status().
status() ->
    gen_statem:call(?MODULE, status).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

-define(disabled, disabled).
-define(enabled(ST), {enabled, ST}).

callback_mode() ->
    handle_event_function.

init([]) ->
    {ok, ?disabled, #{}}.

%% disabled status

%% disabled status, enable command
handle_event({call, From}, {enable, CoordinatorPid, Kind, Options}, ?disabled, Data) ->
    true = link(CoordinatorPid),
    EvictionAgentPid = whereis(emqx_eviction_agent),
    true = link(EvictionAgentPid),
    case emqx_eviction_agent:enable(Kind, ?SERVER_REFERENCE, Options) of
        ok ->
            {next_state,
                ?enabled(#{
                    coordinator_pid => CoordinatorPid,
                    eviction_agent_pid => EvictionAgentPid,
                    kind => Kind
                }), Data, {reply, From, ok}};
        {error, eviction_agent_busy} ->
            true = unlink(EvictionAgentPid),
            true = unlink(CoordinatorPid),
            {keep_state_and_data, {reply, From, {error, eviction_agent_busy}}}
    end;
%% disabled status, disable command
handle_event({call, From}, {disable, _CoordinatorPid, _Kind}, ?disabled, _Data) ->
    {keep_state_and_data, {reply, From, {error, already_disabled}}};
%% disabled status, status command
handle_event({call, From}, status, ?disabled, _Data) ->
    {keep_state_and_data, {reply, From, disabled}};
%% enabled status

%% enabled status, enable command
handle_event(
    {call, From},
    {enable, CoordinatorPid, Kind, Options},
    ?enabled(#{
        coordinator_pid := CoordinatorPid,
        kind := Kind
    }),
    _Data
) ->
    %% just updating options
    ok = emqx_eviction_agent:enable(Kind, ?SERVER_REFERENCE, Options),
    {keep_state_and_data, {reply, From, ok}};
handle_event({call, From}, {enable, _CoordinatorPid, _Kind, _Options}, ?enabled(_St), _Data) ->
    {keep_state_and_data, {reply, From, {error, invalid_coordinator}}};
%% enabled status, disable command
handle_event(
    {call, From},
    {disable, CoordinatorPid, Kind},
    ?enabled(#{
        coordinator_pid := CoordinatorPid,
        eviction_agent_pid := EvictionAgentPid
    }),
    Data
) ->
    _ = emqx_eviction_agent:disable(Kind),
    true = unlink(EvictionAgentPid),
    true = unlink(CoordinatorPid),
    {next_state, ?disabled, Data, {reply, From, ok}};
handle_event({call, From}, {disable, _CoordinatorPid, _Kind}, ?enabled(_St), _Data) ->
    {keep_state_and_data, {reply, From, {error, invalid_coordinator}}};
%% enabled status, status command
handle_event({call, From}, status, ?enabled(#{coordinator_pid := CoordinatorPid}), _Data) ->
    {keep_state_and_data, {reply, From, {enabled, CoordinatorPid}}};
%% fallbacks

handle_event({call, From}, Msg, State, Data) ->
    ?SLOG(warning, #{
        msg => "unknown_call",
        call => Msg,
        state => State,
        data => Data
    }),
    {keep_state_and_data, {reply, From, ignored}};
handle_event(cast, Msg, State, Data) ->
    ?SLOG(warning, #{
        msg => "unknown_cast",
        cast => Msg,
        state => State,
        data => Data
    }),
    keep_state_and_data;
handle_event(info, Msg, State, Data) ->
    ?SLOG(warning, #{
        msg => "unknown_info",
        info => Msg,
        state => State,
        data => Data
    }),
    keep_state_and_data.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.
