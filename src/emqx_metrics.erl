%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_metrics).

-include("logger.hrl").
-include("emqx_mqtt.hrl").

-export([start_link/0]).
-export([new/1, all/0]).
-export([val/1, inc/1, inc/2, inc/3, dec/2, dec/3, set/2]).
-export([trans/2, trans/3, trans/4, commit/0]).
%% Received/sent metrics
-export([received/1, sent/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Bytes sent and received of Broker
-define(BYTES_METRICS, [
    {counter, 'bytes/received'}, % Total bytes received
    {counter, 'bytes/sent'}      % Total bytes sent
]).

%% Packets sent and received of broker
-define(PACKET_METRICS, [
    {counter, 'packets/received'},              % All Packets received
    {counter, 'packets/sent'},                  % All Packets sent
    {counter, 'packets/connect'},               % CONNECT Packets received
    {counter, 'packets/connack'},               % CONNACK Packets sent
    {counter, 'packets/publish/received'},      % PUBLISH packets received
    {counter, 'packets/publish/sent'},          % PUBLISH packets sent
    {counter, 'packets/puback/received'},       % PUBACK packets received
    {counter, 'packets/puback/sent'},           % PUBACK packets sent
    {counter, 'packets/puback/missed'},         % PUBACK packets missed
    {counter, 'packets/pubrec/received'},       % PUBREC packets received
    {counter, 'packets/pubrec/sent'},           % PUBREC packets sent
    {counter, 'packets/pubrec/missed'},         % PUBREC packets missed
    {counter, 'packets/pubrel/received'},       % PUBREL packets received
    {counter, 'packets/pubrel/sent'},           % PUBREL packets sent
    {counter, 'packets/pubrel/missed'},         % PUBREL packets missed
    {counter, 'packets/pubcomp/received'},      % PUBCOMP packets received
    {counter, 'packets/pubcomp/sent'},          % PUBCOMP packets sent
    {counter, 'packets/pubcomp/missed'},        % PUBCOMP packets missed
    {counter, 'packets/subscribe'},             % SUBSCRIBE Packets received
    {counter, 'packets/suback'},                % SUBACK packets sent
    {counter, 'packets/unsubscribe'},           % UNSUBSCRIBE Packets received
    {counter, 'packets/unsuback'},              % UNSUBACK Packets sent
    {counter, 'packets/pingreq'},               % PINGREQ packets received
    {counter, 'packets/pingresp'},              % PINGRESP Packets sent
    {counter, 'packets/disconnect/received'},   % DISCONNECT Packets received
    {counter, 'packets/disconnect/sent'},       % DISCONNECT Packets sent
    {counter, 'packets/auth'}                   % Auth Packets received
]).

%% Messages sent and received of broker
-define(MESSAGE_METRICS, [
    {counter, 'messages/received'},      % All Messages received
    {counter, 'messages/sent'},          % All Messages sent
    {counter, 'messages/qos0/received'}, % QoS0 Messages received
    {counter, 'messages/qos0/sent'},     % QoS0 Messages sent
    {counter, 'messages/qos1/received'}, % QoS1 Messages received
    {counter, 'messages/qos1/sent'},     % QoS1 Messages sent
    {counter, 'messages/qos2/received'}, % QoS2 Messages received
    {counter, 'messages/qos2/expired'},  % QoS2 Messages expired
    {counter, 'messages/qos2/sent'},     % QoS2 Messages sent
    {counter, 'messages/qos2/dropped'},  % QoS2 Messages dropped
    {gauge,   'messages/retained'},      % Messagea retained
    {counter, 'messages/dropped'},       % Messages dropped
    {counter, 'messages/expired'},       % Messages expired
    {counter, 'messages/forward'}        % Messages forward
]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

%% @doc Start the metrics server.
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% Metrics API
%%------------------------------------------------------------------------------

new({gauge, Name}) ->
    ets:insert(?TAB, {{Name, 0}, 0});

new({counter, Name}) ->
    Schedulers = lists:seq(1, emqx_vm:schedulers()),
    ets:insert(?TAB, [{{Name, I}, 0} || I <- Schedulers]).

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
            end, #{}, ?TAB)).

%% @doc Get metric value
-spec(val(atom()) -> non_neg_integer()).
val(Metric) ->
    lists:sum(ets:select(?TAB, [{{{Metric, '_'}, '$1'}, [], ['$1']}])).

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
inc(Type, Metric, Val) ->
    update_counter(key(Type, Metric), {2, Val}).

%% @doc Decrease metric value
-spec(dec(gauge, atom()) -> integer()).
dec(gauge, Metric) ->
    dec(gauge, Metric, 1).

%% @doc Decrease metric value
-spec(dec(gauge, atom(), pos_integer()) -> integer()).
dec(gauge, Metric, Val) ->
    update_counter(key(gauge, Metric), {2, -Val}).

%% @doc Set metric value
set(Metric, Val) when is_atom(Metric) ->
    set(gauge, Metric, Val).
set(gauge, Metric, Val) ->
    ets:insert(?TAB, {key(gauge, Metric), Val}).

trans(inc, Metric) ->
    trans(inc, {counter, Metric}, 1).

trans(Opt, {gauge, Metric}, Val) ->
    trans(Opt, gauge, Metric, Val);
trans(inc, {counter, Metric}, Val) ->
    trans(inc, counter, Metric, Val);
trans(inc, Metric, Val) when is_atom(Metric) ->
    trans(inc, counter, Metric, Val);
trans(dec, gauge, Metric) ->
    trans(dec, gauge, Metric, 1).

trans(inc, Type, Metric, Val) ->
    hold(Type, Metric, Val);
trans(dec, gauge, Metric, Val) ->
    hold(gauge, Metric, -Val).

hold(Type, Metric, Val) when Type =:= counter orelse Type =:= gauge ->
    put('$metrics', case get('$metrics') of
                        undefined ->
                            #{{Type, Metric} => Val};
                        Metrics ->
                            maps:update_with({Type, Metric}, fun(Cnt) -> Cnt + Val end, Val, Metrics)
                    end).

commit() ->
    case get('$metrics') of
        undefined -> ok;
        Metrics ->
            maps:fold(fun({Type, Metric}, Val, _Acc) ->
                          update_counter(key(Type, Metric), {2, Val})
                      end, 0, Metrics),
            erase('$metrics')
    end.

%% @doc Metric key
key(gauge, Metric) ->
    {Metric, 0};
key(counter, Metric) ->
    {Metric, erlang:system_info(scheduler_id)}.

update_counter(Key, UpOp) ->
    ets:update_counter(?TAB, Key, UpOp).

%%-----------------------------------------------------------------------------
%% Received/Sent metrics
%%-----------------------------------------------------------------------------

%% @doc Count packets received.
-spec(received(emqx_mqtt_types:packet()) -> ok).
received(Packet) ->
    inc('packets/received'),
    received1(Packet).
received1(?PUBLISH_PACKET(QoS, _PktId)) ->
    inc('packets/publish/received'),
    inc('messages/received'),
    qos_received(QoS);
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
    inc('packets/disconnect/received');
received2(_) ->
    ignore.
qos_received(?QOS_0) ->
    inc('messages/qos0/received');
qos_received(?QOS_1) ->
    inc('messages/qos1/received');
qos_received(?QOS_2) ->
    inc('messages/qos2/received').

%% @doc Count packets received. Will not count $SYS PUBLISH.
-spec(sent(emqx_mqtt_types:packet()) -> ignore | non_neg_integer()).
sent(?PUBLISH_PACKET(_QoS, <<"$SYS/", _/binary>>, _, _)) ->
    ignore;
sent(Packet) ->
    inc('packets/sent'),
    sent1(Packet).
sent1(?PUBLISH_PACKET(QoS, _PktId)) ->
    inc('packets/publish/sent'),
    inc('messages/sent'),
    qos_sent(QoS);
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
sent2(?DISCONNECT) ->
    inc('packets/disconnect/sent');
sent2(_Type) ->
    ignore.
qos_sent(?QOS_0) ->
    inc('messages/qos0/sent');
qos_sent(?QOS_1) ->
    inc('messages/qos1/sent');
qos_sent(?QOS_2) ->
    inc('messages/qos2/sent').

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    % Create metrics table
    ok = emqx_tables:new(?TAB, [public, set, {write_concurrency, true}]),
    lists:foreach(fun new/1, ?BYTES_METRICS ++ ?PACKET_METRICS ++ ?MESSAGE_METRICS),
    {ok, #{}, hibernate}.

handle_call(Req, _From, State) ->
    ?ERROR("[Metrics] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?ERROR("[Metrics] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?ERROR("[Metrics] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

