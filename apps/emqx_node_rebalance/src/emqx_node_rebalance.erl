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

-module(emqx_node_rebalance).

-include("emqx_node_rebalance.hrl").

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start/1,
         status/0,
         status/1,
         stop/0
        ]).

-export([start_link/0]).

-behavior(gen_statem).

-export([init/1,
         callback_mode/0,
         handle_event/4,
         code_change/4
        ]).

-export([is_node_available/0,
         available_nodes/1,
         connection_count/0,
         session_count/0,
         disconnected_session_count/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-type start_opts() :: #{conn_evict_rate => pos_integer(),
                        sess_evict_rate => pos_integer(),
                        wait_health_check => pos_integer(),
                        wait_takeover => pos_integer(),
                        abs_conn_threshold => pos_integer(),
                        rel_conn_threshold => number(),
                        abs_sess_threshold => pos_integer(),
                        rel_sess_threshold => number(),
                        nodes => [node()]
                       }.
-type start_error() :: already_started | [{node(), term()}].


-spec start(start_opts()) -> ok_or_error(start_error()).
start(StartOpts) ->
    Opts = maps:merge(default_opts(), StartOpts),
    gen_statem:call(?MODULE, {start, Opts}).

-spec stop() -> ok_or_error(not_started).
stop() ->
    gen_statem:call(?MODULE, stop).

-spec status() -> disabled | {enabled, map()}.
status() ->
    gen_statem:call(?MODULE, status).

-spec status(pid()) -> disabled | {enabled, map()}.
status(Pid) ->
    gen_statem:call(Pid, status).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec available_nodes(list(node())) -> list(node()).
available_nodes(Nodes) when is_list(Nodes) ->
    {Available, _} = rpc:multicall(Nodes, ?MODULE, is_node_available, []),
    lists:filter(fun is_atom/1, Available).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

%% states: disabled, wait_health_check, evicting_conns, wait_takeover, evicting_sessions

init([]) ->
    ?tp(debug, emqx_node_rebalance_started, #{}),
    {ok, disabled, #{}}.

%% start
handle_event({call, From},
             {start, #{wait_health_check := WaitHealthCheck} = Opts},
             disabled,
             #{} = Data) ->
    case enable_rebalance(Data#{opts => Opts}) of
        {ok, NewData} ->
            ?LOG(warning, "Node rebalance enabled: ~p", [Opts]),
            {next_state,
             wait_health_check,
             NewData,
             [{state_timeout, seconds(WaitHealthCheck), evict_conns},
              {reply, From, ok}]};
        {error, Reason} ->
            ?LOG(warning, "Node rebalance enabling failed: ~p", [Reason]),
            {keep_state_and_data,
             [{reply, From, {error, Reason}}]}
    end;
handle_event({call, From}, {start, _Opts}, _State, #{}) ->
    {keep_state_and_data,
     [{reply, From, {error, already_started}}]};

%% stop
handle_event({call, From}, stop, disabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, {error, not_started}}]};
handle_event({call, From}, stop, _State, Data) ->
    ok = disable_rebalance(Data),
    ?LOG(warning, "Node rebalance stopped"),
    {next_state,
     disabled,
     deinit(Data),
     [{reply, From, ok}]};

%% status
handle_event({call, From}, status, disabled, #{}) ->
    {keep_state_and_data,
     [{reply, From, disabled}]};
handle_event({call, From}, status, State, Data) ->
    Stats = get_stats(State, Data),
    {keep_state_and_data,
     [{reply, From, {enabled, Stats#{state => State,
                                     coordinator_node => node()}}}]};

%% conn eviction
handle_event(state_timeout,
             evict_conns,
             wait_health_check,
             Data) ->
    ?LOG(warning, "Node rebalance wait_health_check over"),
    {next_state,
     evicting_conns,
     Data,
     [{state_timeout, 0, evict_conns}]};

handle_event(state_timeout,
             evict_conns,
             evicting_conns,
             #{opts := #{wait_takeover := WaitTakeover,
                         evict_interval := EvictInterval}} = Data) ->
    case evict_conns(Data) of
        ok ->
            ?LOG(warning, "Node rebalance evict_conns over"),
            {next_state,
             wait_takeover,
             Data,
             [{state_timeout, seconds(WaitTakeover), evict_sessions}]};
        {continue, NewData} ->
            {keep_state,
             NewData,
             [{state_timeout, EvictInterval, evict_conns}]}
    end;

handle_event(state_timeout,
             evict_sessions,
             wait_takeover,
             Data) ->
    ?LOG(warning, "Node rebalance wait_takeover over"),
    {next_state,
     evicting_sessions,
     Data,
     [{state_timeout, 0, evict_sessions}]};

handle_event(state_timeout,
             evict_sessions,
             evicting_sessions,
             #{opts := #{evict_interval := EvictInterval}} = Data) ->
    case evict_sessions(Data) of
        ok ->
            ?tp(debug, emqx_node_rebalance_evict_sess_over, #{}),
            ?LOG(warning, "Node rebalance evict_sess over"),
            ok = disable_rebalance(Data),
            ?LOG(warning, "Rebalance finished successfully"),
            {next_state,
             disabled,
             deinit(Data)};
        {continue, NewData} ->
            {keep_state,
             NewData,
             [{state_timeout, EvictInterval, evict_sessions}]}
    end;

handle_event({call, From}, Msg, State, Data) ->
    ?LOG(warning, "Unknown call: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    {keep_state_and_data,
     [{reply, From, ignored}]};

handle_event(info, Msg, State, Data) ->
    ?LOG(warning, "Unknown Msg: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    keep_state_and_data;

handle_event(cast, Msg, State, Data) ->
    ?LOG(warning, "Unknown cast Msg: ~p, State: ~p, Data: ~p", [Msg, State, Data]),
    keep_state_and_data.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%--------------------------------------------------------------------
%% internal funs
%%--------------------------------------------------------------------

enable_rebalance(#{opts := Opts} = Data) ->
    Nodes = maps:get(nodes, Opts),
    ConnCounts = multicall(Nodes, {?MODULE, connection_count, []}),
    SessCounts = multicall(Nodes, {?MODULE, session_count, []}),
    {_, Counts} = lists:unzip(ConnCounts),
    Avg = avg(Counts),
    {DonorCounts,
     RecipientCounts} = lists:partition(
                          fun({_Node, Count}) ->
                                  Count >= Avg
                          end,
                          ConnCounts),
    ?LOG(warning, "Enabling rebalance: ConnCounts=~p, DonorCounts=~p, RecipientCounts=~p",
         [ConnCounts, DonorCounts, RecipientCounts]),
    {DonorNodes, _} = lists:unzip(DonorCounts),
    {RecipientNodes, _} = lists:unzip(RecipientCounts),
    case need_rebalance(DonorNodes, RecipientNodes, ConnCounts, SessCounts, Opts) of
        false -> {error, nothing_to_balance};
        true ->
            _ = multicall(DonorNodes, {emqx_node_rebalance_agent, enable, [self()]}),
            {ok, Data#{donors => DonorNodes,
                       recipients => RecipientNodes,
                       initial_conn_counts => maps:from_list(ConnCounts),
                       initial_sess_counts => maps:from_list(SessCounts)}}
    end.

disable_rebalance(#{donors := DonorNodes}) ->
    _ = multicall(DonorNodes, {emqx_node_rebalance_agent, disable, [self()]}),
    ok.

evict_conns(#{donors := DonorNodes, recipients := RecipientNodes, opts := Opts} = Data) ->
    DonorNodeCounts = multicall(DonorNodes, {?MODULE, connection_count, []}),
    {_, DonorCounts} = lists:unzip(DonorNodeCounts),
    RecipientNodeCounts = multicall(RecipientNodes, {?MODULE, connection_count, []}),
    {_, RecipientCounts} = lists:unzip(RecipientNodeCounts),

    DonorAvg = avg(DonorCounts),
    RecipientAvg = avg(RecipientCounts),
    Thresholds = thresholds(conn, Opts),
    NewData = Data#{donor_conn_avg => DonorAvg,
                    recipient_conn_avg => RecipientAvg,
                    donor_conn_counts => maps:from_list(DonorNodeCounts),
                    recipient_conn_counts => maps:from_list(RecipientNodeCounts)},
    case within_thresholds(DonorAvg, RecipientAvg, Thresholds) of
        true -> ok;
        false ->
            ConnEvictRate = maps:get(conn_evict_rate, Opts),
            NodesToEvict = nodes_to_evict(RecipientAvg, DonorNodeCounts),
            ?LOG(warning, "Node rebalance, evict_conns, nodes=~p, counts=~p",
                 [NodesToEvict, ConnEvictRate]),
            _ = multicall(NodesToEvict, {emqx_eviction_agent, evict_connections, [ConnEvictRate]}),
            {continue, NewData}
    end.

evict_sessions(#{donors := DonorNodes, recipients := RecipientNodes, opts := Opts} = Data) ->
    DonorNodeCounts = multicall(DonorNodes, {?MODULE, disconnected_session_count, []}),
    {_, DonorCounts} = lists:unzip(DonorNodeCounts),
    RecipientNodeCounts = multicall(RecipientNodes, {?MODULE, disconnected_session_count, []}),
    {_, RecipientCounts} = lists:unzip(RecipientNodeCounts),

    DonorAvg = avg(DonorCounts),
    RecipientAvg = avg(RecipientCounts),
    Thresholds = thresholds(sess, Opts),
    NewData = Data#{donor_sess_avg => DonorAvg,
                    recipient_sess_avg => RecipientAvg,
                    donor_sess_counts => maps:from_list(DonorNodeCounts),
                    recipient_sess_counts => maps:from_list(RecipientNodeCounts)},
    case within_thresholds(DonorAvg, RecipientAvg, Thresholds) of
        true -> ok;
        false ->
            SessEvictRate = maps:get(sess_evict_rate, Opts),
            NodesToEvict = nodes_to_evict(RecipientAvg, DonorNodeCounts),
            ?LOG(warning, "Node rebalance, evict_sessions, nodes=~p, counts=~p",
                 [NodesToEvict, SessEvictRate]),
            _ = multicall(NodesToEvict,
                          {emqx_eviction_agent,
                           evict_sessions,
                           [SessEvictRate, RecipientNodes, disconnected]}),
            {continue, NewData}
    end.

need_rebalance([] = _DonorNodes, _RecipientNodes, _ConnCounts, _SessCounts, _Opts) -> false;
need_rebalance(_DonorNodes, [] = _RecipientNodes, _ConnCounts, _SessCounts, _Opts) -> false;
need_rebalance(DonorNodes, RecipientNodes, ConnCounts, SessCounts, Opts) ->
    DonorConnAvg = avg_for_nodes(DonorNodes, ConnCounts),
    RecipientConnAvg = avg_for_nodes(RecipientNodes, ConnCounts),
    DonorSessAvg = avg_for_nodes(DonorNodes, SessCounts),
    RecipientSessAvg = avg_for_nodes(RecipientNodes, SessCounts),
    Result = (not within_thresholds(DonorConnAvg, RecipientConnAvg, thresholds(conn, Opts)))
        orelse (not within_thresholds(DonorSessAvg, RecipientSessAvg, thresholds(sess, Opts))),
    ?tp(debug, emqx_node_rebalance_need_rebalance,
        #{donors => DonorNodes,
          recipients => RecipientNodes,
          conn_counts => ConnCounts,
          sess_counts => SessCounts,
          opts => Opts,
          result => Result
         }),
    Result.

avg_for_nodes(Nodes, Counts) ->
    avg(maps:values(maps:with(Nodes, maps:from_list(Counts)))).

within_thresholds(Value, GoalValue, {AbsThres, RelThres}) ->
    (Value =< GoalValue + AbsThres) orelse (Value =< GoalValue * RelThres).

thresholds(conn, #{abs_conn_threshold := Abs, rel_conn_threshold := Rel}) ->
    {Abs, Rel};
thresholds(sess, #{abs_sess_threshold := Abs, rel_sess_threshold := Rel}) ->
    {Abs, Rel}.

nodes_to_evict(Goal, NodeCounts) ->
    {Nodes, _} = lists:unzip(
                   lists:filter(
                     fun({_Node, Count}) ->
                             Count > Goal
                     end,
                     NodeCounts)),
    Nodes.

get_stats(disabled, _Data) -> #{};
get_stats(_State, Data) -> Data.

avg(List) when length(List) >= 1 ->
    lists:sum(List) / length(List).

multicall(Nodes, {M, F, A}) ->
    case rpc:multicall(Nodes, M, F, A) of
        {Results, []} ->
            case lists:partition(fun is_ok/1, lists:zip(Nodes, Results)) of
                {OkResults, []} ->
                    [{Node, ok_result(Result)} || {Node, Result} <- OkResults];
                {_, BadResults} ->
                    error({bad_nodes, BadResults})
            end;
        {_, [_BadNode | _] = BadNodes} ->
            error({bad_nodes, BadNodes})
    end.

is_ok({_Node, {ok, _}}) -> true;
is_ok({_Node, ok}) -> true;
is_ok(_) -> false.

ok_result({ok, Result}) -> Result;
ok_result(ok) -> ok.

connection_count() ->
    {ok, emqx_eviction_agent:connection_count()}.

session_count() ->
    {ok, emqx_eviction_agent:session_count()}.

disconnected_session_count() ->
    {ok, emqx_eviction_agent:session_count(disconnected)}.

default_opts() ->
    #{
      conn_evict_rate => ?DEFAULT_CONN_EVICT_RATE,
      abs_conn_threshold => ?DEFAULT_ABS_CONN_THRESHOLD,
      rel_conn_threshold => ?DEFAULT_REL_CONN_THRESHOLD,

      sess_evict_rate => ?DEFAULT_SESS_EVICT_RATE,
      abs_sess_threshold => ?DEFAULT_ABS_SESS_THRESHOLD,
      rel_sess_threshold => ?DEFAULT_REL_SESS_THRESHOLD,

      wait_health_check => ?DEFAULT_WAIT_HEALTH_CHECK,
      wait_takeover => ?DEFAULT_WAIT_TAKEOVER,

      evict_interval => ?EVICT_INTERVAL,

      nodes => all_nodes()
     }.


deinit(Data) ->
    Keys = [recipient_conn_avg, recipient_sess_avg, donor_conn_avg, donor_sess_avg,
            recipient_conn_counts, recipient_sess_counts, donor_conn_counts, donor_sess_counts,
            initial_conn_counts, initial_sess_counts,
            opts],
    maps:without(Keys, Data).

is_node_available() ->
    true = is_pid(whereis(emqx_node_rebalance_agent)),
    disabled = emqx_eviction_agent:status(),
    node().

all_nodes() ->
    ekka_mnesia:cluster_nodes(all).

seconds(Sec) ->
    round(timer:seconds(Sec)).
