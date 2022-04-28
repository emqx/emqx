%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_evacuation).

-include("emqx_node_rebalance.hrl").

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start/1,
         status/0,
         stop/0
        ]).

-export([start_link/0]).

-export([init/1,
         callback_mode/0,
         handle_event/4,
         code_change/4
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-define(EVICT_INTERVAL, 1000).

-type start_opts() :: #{server_reference => emqx_eviction_agent:server_reference(),
                        conn_evict_rate => integer()}.
-type start_error() :: already_started | eviction_agent_busy.
-type stats() :: #{
                   initial_conns := non_neg_integer(),
                   current_conns := non_neg_integer(),
                   conn_evict_rate := pos_integer(),
                   server_reference := emqx_eviction_agent:server_reference()
                  }.
-type status() :: {started, stats()} | stopped.

-spec start(start_opts()) -> ok_or_error(start_error()).
start(StartOpts) ->
    Opts = #{
             server_reference => maps:get(server_reference, StartOpts, undefined),
             conn_evict_rate => maps:get(conn_evict_rate, StartOpts, ?DEFAULT_CONN_EVICT_RATE)
            },
    gen_statem:call(?MODULE, {start, Opts}).

-spec stop() -> ok_or_error(not_started).
stop() ->
    gen_statem:call(?MODULE, stop).

-spec status() -> status().
status() ->
    gen_statem:call(?MODULE, status).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

%% states: disabled, waiting_for_agent, enabled

init([]) ->
    case emqx_node_rebalance_evacuation_persist:read() of
        {ok, #{server_reference := ServerReference} = Opts} ->
            case emqx_eviction_agent:enable(?MODULE, ServerReference) of
                ok ->
                    Data = init_data(#{}, Opts),
                    ok = warn_enabled(),
                    {ok, enabled, Data, [{state_timeout, 0, evict}]};
                {error, eviction_agent_busy} ->
                    emqx_node_rebalance_evacuation_persist:clear(),
                    {ok, disabled, #{}}
            end;
        none ->
            {ok, disabled, #{}}
    end.

%% start
handle_event({call, From},
             {start, #{server_reference := ServerReference} = Opts},
             disabled,
             #{} = Data) ->
    case emqx_eviction_agent:enable(?MODULE, ServerReference) of
        ok ->
            NewData = init_data(Data, Opts),
            ok = emqx_node_rebalance_evacuation_persist:save(Opts),
            {next_state,
             enabled,
             NewData,
             [{state_timeout, 0, evict},
              {reply, From, ok}]};
        {error, eviction_agent_busy} ->
            {keep_state_and_data,
              [{reply, From, {error, eviction_agent_busy}}]}
    end;
handle_event({call, From}, {start, _Opts}, enabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, {error, already_started}}]};

%% stop
handle_event({call, From}, stop, enabled, Data) ->
    ok = emqx_node_rebalance_evacuation_persist:clear(),
    _ = emqx_eviction_agent:disable(?MODULE),
    {next_state,
     disabled,
     deinit(Data),
     [{reply, From, ok}]};
handle_event({call, From}, stop, disabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, {error, not_started}}]};

%% status
handle_event({call, From}, status, enabled, Data) ->
    Stats = maps:with(
              [initial_conns, current_conns, server_reference, conn_evict_rate],
              Data),
    {keep_state_and_data,
     [{reply, From, {enabled, Stats}}]};
handle_event({call, From}, status, disabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, disabled}]};

%% eviction
handle_event(state_timeout,
             evict,
             enabled,
             #{conn_evict_rate := ConnEvictRate} = Data) ->
    CurrentConns = case emqx_eviction_agent:status() of
        {enabled, #{connections := Conns}} when Conns > 0 ->
            ok = emqx_eviction_agent:evict_connections(ConnEvictRate),
            ?tp(debug, node_evacuation_evict, #{conn_evict_rate => ConnEvictRate}),
            Conns;
        {enabled, #{connections := Conns}} ->
            Conns
    end,
    NewData = Data#{current_conns => CurrentConns},
    {keep_state,
     NewData,
     [{state_timeout, ?EVICT_INTERVAL, evict}]};

handle_event(info, Msg, State, Data) ->
    ?LOG(warning, "Unknown Msg: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    keep_state_and_data;

handle_event(cast, Msg, State, Data) ->
    ?LOG(warning, "Unknown cast Msg: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    keep_state_and_data.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% internal helpers
init_data(Data, Opts) ->
    {enabled, #{connections := Conns}} = emqx_eviction_agent:status(),
    Data#{
      initial_conns => Conns,
      current_conns => Conns,
      server_reference => maps:get(server_reference, Opts),
      conn_evict_rate => maps:get(conn_evict_rate, Opts)
     }.

deinit(Data) ->
    maps:without(
        [eviction_agent_pid, initial_conns, current_conns, server_reference, conn_evict_rate],
        Data).

warn_enabled() ->
    Msg = "Node evacuation is enabled. The node will not receive connections.",
    ?LOG(warning, Msg),
    io:format(standard_error, "~s~n", [Msg]).
