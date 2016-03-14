%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc emqttd metrics. responsible for collecting broker metrics.
-module(emqttd_metrics).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0]).

%% Received/Sent Metrics
-export([received/1, sent/1]).

-export([all/0, value/1, inc/1, inc/2, inc/3, dec/2, dec/3, set/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tick_tref}).

-define(METRIC_TAB, mqtt_metric).

%% Bytes sent and received of Broker
-define(SYSTOP_BYTES, [
    {counter, 'bytes/received'},   % Total bytes received
    {counter, 'bytes/sent'}        % Total bytes sent
]).

%% Packets sent and received of Broker
-define(SYSTOP_PACKETS, [
    {counter, 'packets/received'},         % All Packets received
    {counter, 'packets/sent'},             % All Packets sent
    {counter, 'packets/connect'},          % CONNECT Packets received
    {counter, 'packets/connack'},          % CONNACK Packets sent
    {counter, 'packets/publish/received'}, % PUBLISH packets received
    {counter, 'packets/publish/sent'},     % PUBLISH packets sent
    {counter, 'packets/puback/received'},  % PUBACK packets received
    {counter, 'packets/puback/sent'},      % PUBACK packets sent
    {counter, 'packets/pubrec/received'},  % PUBREC packets received
    {counter, 'packets/pubrec/sent'},      % PUBREC packets sent
    {counter, 'packets/pubrel/received'},  % PUBREL packets received
    {counter, 'packets/pubrel/sent'},      % PUBREL packets sent
    {counter, 'packets/pubcomp/received'}, % PUBCOMP packets received
    {counter, 'packets/pubcomp/sent'},     % PUBCOMP packets sent
    {counter, 'packets/subscribe'},        % SUBSCRIBE Packets received 
    {counter, 'packets/suback'},           % SUBACK packets sent 
    {counter, 'packets/unsubscribe'},      % UNSUBSCRIBE Packets received
    {counter, 'packets/unsuback'},         % UNSUBACK Packets sent
    {counter, 'packets/pingreq'},          % PINGREQ packets received
    {counter, 'packets/pingresp'},         % PINGRESP Packets sent
    {counter, 'packets/disconnect'}        % DISCONNECT Packets received 
]).

%% Messages sent and received of broker
-define(SYSTOP_MESSAGES, [
    {counter, 'messages/received'},      % Messages received
    {counter, 'messages/sent'},          % Messages sent
    {counter, 'messages/qos0/received'}, % Messages received
    {counter, 'messages/qos0/sent'},     % Messages sent
    {counter, 'messages/qos1/received'}, % Messages received
    {counter, 'messages/qos1/sent'},     % Messages sent
    {counter, 'messages/qos2/received'}, % Messages received
    {counter, 'messages/qos2/sent'},     % Messages sent
    {gauge,   'messages/retained'},      % Messagea retained
    {counter, 'messages/dropped'}        % Messages dropped
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start metrics server
-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Count packets received.
-spec(received(mqtt_packet()) -> ok).
received(Packet) ->
    inc('packets/received'),
    received1(Packet).
received1(?PUBLISH_PACKET(Qos, _PktId)) ->
    inc('packets/publish/received'),
    inc('messages/received'),
    qos_received(Qos);
received1(?PACKET(Type)) ->
    received2(Type).
received2(?CONNECT) ->
    inc('packets/connect');
received2(?PUBACK) ->
    inc('packets/puback/received');
received2(?PUBREC) ->
    inc('packets/pubrec/received');
received2(?PUBREL) ->
    inc('packets/pubrel/received');
received2(?PUBCOMP) ->
    inc('packets/pubcomp/received');
received2(?SUBSCRIBE) ->
    inc('packets/subscribe');
received2(?UNSUBSCRIBE) ->
    inc('packets/unsubscribe');
received2(?PINGREQ) ->
    inc('packets/pingreq');
received2(?DISCONNECT) ->
    inc('packets/disconnect');
received2(_) ->
    ignore.
qos_received(?QOS_0) ->
    inc('messages/qos0/received');
qos_received(?QOS_1) ->
    inc('messages/qos1/received');
qos_received(?QOS_2) ->
    inc('messages/qos2/received').

%% @doc Count packets received. Will not count $SYS PUBLISH.
-spec(sent(mqtt_packet()) -> ok).
sent(?PUBLISH_PACKET(_Qos, <<"$SYS/", _/binary>>, _, _)) ->
    ignore;
sent(Packet) ->
    emqttd_metrics:inc('packets/sent'),
    sent1(Packet).
sent1(?PUBLISH_PACKET(Qos, _PktId)) ->
    inc('packets/publish/sent'),
    inc('messages/sent'),
    qos_sent(Qos);
sent1(?PACKET(Type)) ->
    sent2(Type).
sent2(?CONNACK) ->
    inc('packets/connack');
sent2(?PUBACK) ->
    inc('packets/puback/sent');
sent2(?PUBREC) ->
    inc('packets/pubrec/sent');
sent2(?PUBREL) ->
    inc('packets/pubrel/sent');
sent2(?PUBCOMP) ->
    inc('packets/pubcomp/sent');
sent2(?SUBACK) ->
    inc('packets/suback');
sent2(?UNSUBACK) ->
    inc('packets/unsuback');
sent2(?PINGRESP) ->
    inc('packets/pingresp');
sent2(_Type) ->
    ingore.
qos_sent(?QOS_0) ->
    inc('messages/qos0/sent');
qos_sent(?QOS_1) ->
    inc('messages/qos1/sent');
qos_sent(?QOS_2) ->
    inc('messages/qos2/sent').

%% @doc Get all metrics
-spec(all() -> [{atom(), non_neg_integer()}]).
all() ->
    maps:to_list(
        ets:foldl(
            fun({{Metric, _N}, Val}, Map) ->
                    case maps:find(Metric, Map) of
                        {ok, Count} -> maps:put(Metric, Count+Val, Map);
                        error -> maps:put(Metric, Val, Map)
                    end
            end, #{}, ?METRIC_TAB)).

%% @doc Get metric value
-spec(value(atom()) -> non_neg_integer()).
value(Metric) ->
    lists:sum(ets:select(?METRIC_TAB, [{{{Metric, '_'}, '$1'}, [], ['$1']}])).

%% @doc Increase counter
-spec(inc(atom()) -> non_neg_integer()).
inc(Metric) ->
    inc(counter, Metric, 1).

%% @doc Increase metric value
-spec(inc({counter | gauge, atom()} | atom(), pos_integer()) -> non_neg_integer()).
inc({gauge, Metric}, Val) ->
    inc(gauge, Metric, Val);
inc({counter, Metric}, Val) ->
    inc(counter, Metric, Val);
inc(Metric, Val) when is_atom(Metric) ->
    inc(counter, Metric, Val).

%% @doc Increase metric value
-spec(inc(counter | gauge, atom(), pos_integer()) -> pos_integer()).
inc(gauge, Metric, Val) ->
    ets:update_counter(?METRIC_TAB, key(gauge, Metric), {2, Val});
inc(counter, Metric, Val) ->
    ets:update_counter(?METRIC_TAB, key(counter, Metric), {2, Val}).

%% @doc Decrease metric value
-spec(dec(gauge, atom()) -> integer()).
dec(gauge, Metric) ->
    dec(gauge, Metric, 1).

%% @doc Decrease metric value
-spec(dec(gauge, atom(), pos_integer()) -> integer()).
dec(gauge, Metric, Val) ->
    ets:update_counter(?METRIC_TAB, key(gauge, Metric), {2, -Val}).

%% @doc Set metric value
set(Metric, Val) when is_atom(Metric) ->
    set(gauge, Metric, Val).
set(gauge, Metric, Val) ->
    ets:insert(?METRIC_TAB, {key(gauge, Metric), Val}).

%% @doc Metric Key
key(gauge, Metric) ->
    {Metric, 0};
key(counter, Metric) ->
    {Metric, erlang:system_info(scheduler_id)}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqttd_time:seed(),
    Metrics = ?SYSTOP_BYTES ++ ?SYSTOP_PACKETS ++ ?SYSTOP_MESSAGES,
    % Create metrics table
    ets:new(?METRIC_TAB, [set, public, named_table, {write_concurrency, true}]),
    % Init metrics
    [create_metric(Metric) ||  Metric <- Metrics],
    % $SYS Topics for metrics
    [ok = emqttd:create(topic, metric_topic(Topic)) || {_, Topic} <- Metrics],
    % Tick to publish metrics
    {ok, #state{tick_tref = emqttd_broker:start_tick(tick)}, hibernate}.

handle_call(_Req, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    % publish metric message
    [publish(Metric, Val) || {Metric, Val} <- all()],
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tick_tref = TRef}) ->
    emqttd_broker:stop_tick(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

publish(Metric, Val) ->
    Msg = emqttd_message:make(metrics, metric_topic(Metric), bin(Val)),
    emqttd:publish(emqttd_message:set_flag(sys, Msg)).

create_metric({gauge, Name}) ->
    ets:insert(?METRIC_TAB, {{Name, 0}, 0});

create_metric({counter, Name}) ->
    Schedulers = lists:seq(1, erlang:system_info(schedulers)),
    ets:insert(?METRIC_TAB, [{{Name, I}, 0} || I <- Schedulers]).

metric_topic(Metric) ->
    emqttd_topic:systop(list_to_binary(lists:concat(['metrics/', Metric]))).

bin(I) when is_integer(I) -> list_to_binary(integer_to_list(I)).

