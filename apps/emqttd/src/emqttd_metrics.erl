%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd metrics. responsible for collecting broker metrics.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_metrics).

-author('feng@emqtt.io').

-include_lib("emqtt/include/emqtt.hrl").

-include("emqttd_systop.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(METRIC_TAB, mqtt_broker_metric).

%% API Function Exports
-export([start_link/1]).

-export([all/0, value/1,
         inc/1, inc/2, inc/3,
         dec/2, dec/3,
         set/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pub_interval, tick_timer}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start emqttd metrics.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link([tuple()]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Options) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Options], []).

%%------------------------------------------------------------------------------
%% @doc
%% Get all metrics.
%%
%% @end
%%------------------------------------------------------------------------------
-spec all() -> [{atom(), non_neg_integer()}].
all() ->
    maps:to_list(
        ets:foldl(
            fun({{Metric, _N}, Val}, Map) ->
                    case maps:find(Metric, Map) of
                        {ok, Count} -> maps:put(Metric, Count+Val, Map);
                        error -> maps:put(Metric, Val, Map)
                    end
            end, #{}, ?METRIC_TAB)).

%%------------------------------------------------------------------------------
%% @doc
%% Get metric value.
%%
%% @end
%%------------------------------------------------------------------------------
-spec value(atom()) -> non_neg_integer().
value(Metric) ->
    lists:sum(ets:select(?METRIC_TAB, [{{{Metric, '_'}, '$1'}, [], ['$1']}])).

%%------------------------------------------------------------------------------
%% @doc
%% Increase counter.
%%
%% @end
%%------------------------------------------------------------------------------
-spec inc(atom()) -> non_neg_integer().
inc(Metric) ->
    inc(counter, Metric, 1).

%%------------------------------------------------------------------------------
%% @doc
%% Increase metric value.
%%
%% @end
%%------------------------------------------------------------------------------
-spec inc(counter | gauge, atom()) -> non_neg_integer().
inc(gauge, Metric) ->
    inc(gauge, Metric, 1);
inc(counter, Metric) ->
    inc(counter, Metric, 1);
inc(Metric, Val) when is_atom(Metric) and is_integer(Val) ->
    inc(counter, Metric, Val).

%%------------------------------------------------------------------------------
%% @doc
%% Increase metric value.
%%
%% @end
%%------------------------------------------------------------------------------
-spec inc(counter | gauge, atom(), pos_integer()) -> pos_integer().
inc(gauge, Metric, Val) ->
    ets:update_counter(?METRIC_TAB, key(gauge, Metric), {2, Val});
inc(counter, Metric, Val) ->
    ets:update_counter(?METRIC_TAB, key(counter, Metric), {2, Val}).

%%------------------------------------------------------------------------------
%% @doc
%% Decrease metric value.
%%
%% @end
%%------------------------------------------------------------------------------
-spec dec(gauge, atom()) -> integer().
dec(gauge, Metric) ->
    dec(gauge, Metric, 1).

%%------------------------------------------------------------------------------
%% @doc
%% Decrease metric value
%%
%% @end
%%------------------------------------------------------------------------------
-spec dec(gauge, atom(), pos_integer()) -> integer().
dec(gauge, Metric, Val) ->
    ets:update_counter(?METRIC_TAB, key(gauge, Metric), {2, -Val}).

%%------------------------------------------------------------------------------
%% @doc
%% Set metric value.
%%
%% @end
%%------------------------------------------------------------------------------
set(Metric, Val) when is_atom(Metric) ->
    set(gauge, Metric, Val).
set(gauge, Metric, Val) ->
    ets:insert(?METRIC_TAB, {key(gauge, Metric), Val}).

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Metric Key
%%
%% @end
%%------------------------------------------------------------------------------
key(gauge, Metric) ->
    {Metric, 0};
key(counter, Metric) ->
    {Metric, erlang:system_info(scheduler_id)}.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Options]) ->
    random:seed(now()),
    Metrics = ?SYSTOP_BYTES ++ ?SYSTOP_PACKETS ++ ?SYSTOP_MESSAGES,
    % Create metrics table
    ets:new(?METRIC_TAB, [set, public, named_table, {write_concurrency, true}]),
    % Init metrics
    [new_metric(Metric) ||  Metric <- Metrics],
    % $SYS Topics for metrics
    [ok = emqttd_pubsub:create(systop(Topic)) || {_, Topic} <- Metrics],
    PubInterval = proplists:get_value(pub_interval, Options, 60),
    Delay = if 
                PubInterval == 0 -> 0;
                true -> random:uniform(PubInterval)
            end,
    {ok, tick(Delay, #state{pub_interval = PubInterval}), hibernate}.

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info(tick, State) ->
    % publish metric message
    [publish(systop(Metric), i2b(Val))|| {Metric, Val} <- all()],
    {noreply, tick(State), hibernate};

handle_info(Info, State) ->
    {stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

systop(Name) when is_atom(Name) ->
    list_to_binary(lists:concat(["$SYS/brokers/", node(), "/", Name])).

publish(Topic, Payload) ->
    emqttd_pubsub:publish(metrics, #mqtt_message{topic = Topic,
                                                 payload = Payload}).

new_metric({gauge, Name}) ->
    ets:insert(?METRIC_TAB, {{Name, 0}, 0});

new_metric({counter, Name}) ->
    Schedulers = lists:seq(1, erlang:system_info(schedulers)),
    [ets:insert(?METRIC_TAB, {{Name, I}, 0}) || I <- Schedulers].

tick(State = #state{pub_interval = PubInterval}) ->
    tick(PubInterval, State).

tick(0, State) ->
    State;
tick(Delay, State) ->
    State#state{tick_timer = erlang:send_after(Delay * 1000, self(), tick)}.

i2b(I) ->
    list_to_binary(integer_to_list(I)).


