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
%%% emqtt metrics module. responsible for collecting broker metrics.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqtt_metrics).

-include("emqtt_packet.hrl").

-include("emqtt_systop.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TABLE, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

-export([all/0, value/1,
         inc/1, inc/2,
         dec/1, dec/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pub_interval, tick_timer}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
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
            end, #{}, ?TABLE)).

%%------------------------------------------------------------------------------
%% @doc
%% Get metric value
%%
%% @end
%%------------------------------------------------------------------------------
-spec value(atom()) -> non_neg_integer().
value(Metric) ->
    lists:sum(ets:select(?TABLE, [{{{Metric, '_'}, '$1'}, [], ['$1']}])).

%%------------------------------------------------------------------------------
%% @doc
%% Increase metric value
%%
%% @end
%%------------------------------------------------------------------------------
-spec inc(atom()) -> non_neg_integer().
inc(Metric) ->
    inc(Metric, 1).

%%------------------------------------------------------------------------------
%% @doc
%% Increase metric value
%%
%% @end
%%------------------------------------------------------------------------------
-spec inc(atom(), pos_integer()) -> pos_integer().
inc(Metric, Val) ->
    ets:update_counter(?TABLE, key(Metric), {2, Val}).

%%------------------------------------------------------------------------------
%% @doc
%% Decrease metric value
%%
%% @end
%%------------------------------------------------------------------------------
-spec dec(atom()) -> integer().
dec(Metric) ->
    dec(Metric, 1).

%%------------------------------------------------------------------------------
%% @doc
%% Decrease metric value
%%
%% @end
%%------------------------------------------------------------------------------
-spec dec(atom(), pos_integer()) -> integer().
dec(Metric, Val) ->
    %TODO: ok?
    ets:update_counter(?TABLE, key(Metric), {2, -Val}).

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Metric Key
%%
%% @end
%%------------------------------------------------------------------------------
key(Metric) ->
    {Metric, erlang:system_info(scheduler_id)}.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Options) ->
    random:seed(now()),
    Topics = ?SYSTOP_BYTES ++ ?SYSTOP_PACKETS ++ ?SYSTOP_MESSAGES,
    % $SYS Topics for metrics
    [{atomic, _} = emqtt_pubsub:create(systop(Topic)) || Topic <- Topics],
    % Create metrics table
    ets:new(?TABLE, [set, public, named_table, {write_concurrency, true}]),
    % Init metrics
    [new_metric(Topic) || Topic <- Topics],
    PubInterval = proplists:get_value(pub_interval, Options, 60),
    {ok, tick(random:uniform(PubInterval), #state{pub_interval = PubInterval}), hibernate}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    % publish metric message
    [publish(systop(Metric), i2b(Val))|| {Metric, Val} <- all()],
    {noreply, tick(State), hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

systop(Name) when is_atom(Name) ->
    list_to_binary(lists:concat(["$SYS/brokers/", node(), "/", Name])).

publish(Topic, Payload) ->
    emqtt_router:route(#mqtt_message{topic = Topic, payload = Payload}).

new_metric(Name) ->
    Schedulers = lists:seq(1, erlang:system_info(schedulers)),
    [ets:insert(?TABLE, {{Name, I}, 0}) || I <- Schedulers].

tick(State = #state{pub_interval = PubInterval}) ->
    tick(PubInterval, State).

tick(Delay, State) ->
    State#state{tick_timer = erlang:send_after(Delay * 1000, self(), tick)}.

i2b(I) ->
    list_to_binary(integer_to_list(I)).

