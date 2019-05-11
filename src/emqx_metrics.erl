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

-behavior(gen_server).

-include("logger.hrl").
-include("types.hrl").
-include("emqx_mqtt.hrl").

-export([start_link/0]).

-export([ new/1
        , all/0
        , all/1
        , all/2
        ]).

-export([ add_metrics/2
        , del_metrics/2
        ]).

-export([ val/1
        , inc/1
        , inc/2
        , inc/3
        , dec/2
        , dec/3
        , set/2
        ]).

-export([ trans/2
        , trans/3
        , trans/4
        , commit/0
        ]).

%% Received/sent metrics
-export([ received/1
        , sent/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

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

-define(TOPIC_METRICS, [
    {counter, 'messages/received'},      % All Messages received
    {counter, 'messages/sent'},          % All Messages sent
    {counter, 'messages/dropped'},       % Messages dropped
    {counter, 'messages/expired'},       % Messages expired
    {counter, 'messages/forward'}        % Messages forward
]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

%% @doc Start the metrics server.
-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% Metrics API
%%------------------------------------------------------------------------------

new({gauge, Name}) ->
    case ets:member(?TAB, {Name, 0}) of
        true -> false;
        false -> ets:insert(?TAB, {{Name, 0}, 0})
    end;
new({counter, Name}) ->
    case ets:member(?TAB, {Name, 1}) of
        true -> false;
        false ->
            Schedulers = lists:seq(1, emqx_vm:schedulers()),
            ets:insert(?TAB, [{{Name, I}, 0} || I <- Schedulers])
    end.

del({gauge, Name}) ->
    ets:delete(?TAB, {Name, 0});
del({counter, Name}) ->
    ets:match_delete(?TAB, {{Name, '_'}, '_'}).

%% @doc Get all metrics
-spec(all() -> [{atom(), non_neg_integer()}]).
all() ->
    maps:to_list(
        ets:foldl(
            fun({{Name, _N}, Val}, Map) when is_atom(Name) ->
                case maps:find(Name, Map) of
                    {ok, Count} -> maps:put(Name, Count+Val, Map);
                    error -> maps:put(Name, Val, Map)
                end;
               (_, Map) -> Map
            end, #{}, ?TAB)).

all(topic_metrics) ->
   {_Type, Event} = lists:last(?TOPIC_METRICS),
    case lists:usort(ets:select(?TAB, [{{{{topic_metrics, {'$1', Event}}, '_'}, '_'}, [], ['$1']}])) of
        [] -> [];
        Topics ->
            lists:foldl(fun(Topic, Acc) ->
                            [{Topic, all(topic_metrics, Topic)} | Acc]
                        end, [], Topics)
    end;
all(_) ->
    []. 

all(topic_metrics, undefined) ->
    [];
all(topic_metrics, Topic) ->
    maps:to_list(
        ets:foldl(
            fun({{{topic_metrics, {Topic0, Event}}, _N}, Val}, Map) when Topic0 =:= Topic ->
                    case maps:find(Event, Map) of
                        {ok, Count} -> maps:put(Event, Count+Val, Map);
                        error -> maps:put(Event, Val, Map)
                    end;
               (_, Map) -> Map
            end, #{}, ?TAB)).

add_metrics(topic_metrics, undefined) ->
    {error, topic_is_undefined};
add_metrics(topic_metrics, Topic) when is_binary(Topic) ->
    lists:foreach(fun({Type, Event}) ->
                      new({Type, {topic_metrics, {Topic, Event}}})
                  end, ?TOPIC_METRICS),
    case ets:lookup(?TAB, dynamic_metrics) of
        [] ->
            ets:insert(?TAB, {dynamic_metrics, [{topic_metrics, Topic}]});
        [{dynamic_metrics, DynamicMetrics}] ->
            ets:insert(?TAB, {dynamic_metrics, [{topic_metrics, Topic} | DynamicMetrics]})
    end,
    ok;
add_metrics(_, _) ->
    ok.

del_metrics(topic_metrics, Topic) when is_binary(Topic) ->
    lists:foreach(fun({Type, Event}) ->
                      del({Type, {topic_metrics, {Topic, Event}}})
                  end, ?TOPIC_METRICS),
    case ets:lookup(?TAB, dynamic_metrics) of
        [] -> ok;
        [{dynamic_metrics, DynamicMetrics}] ->
            ets:insert(?TAB, {dynamic_metrics, lists:delete({topic_metrics, Topic}, DynamicMetrics)})
    end,             
    ok;
del_metrics(_, _) ->
    ok.

%% @doc Get metric value
-spec(val(atom() | tuple()) -> non_neg_integer()).
val(Name) when is_atom(Name) ->
    lists:sum(ets:select(?TAB, [{{{Name, '_'}, '$1'}, [], ['$1']}])).

%% @doc Increase counter
-spec(inc(atom() | tuple()) -> non_neg_integer()).
inc(Name) ->
    inc(counter, Name, 1).

%% @doc Increase metric value
-spec(inc({counter | gauge, atom() | tuple()} | atom() | tuple(), pos_integer()) -> non_neg_integer()).
inc({gauge, Name}, Val) ->
    inc(gauge, Name, Val);
inc({counter, Name}, Val) ->
    inc(counter, Name, Val);
inc(Name, Val) ->
    inc(counter, Name, Val).

%% @doc Increase metric value
-spec(inc(counter | gauge, atom() | tuple(), pos_integer()) -> pos_integer()).
inc(Type, Name, Val) when is_atom(Name) ->
    update_counter(key(Type, Name), {2, Val});
inc(Type, Name = {topic_metrics, {Topic, _Event}}, Val) when is_binary(Topic) ->
    case is_being_monitored({topic_metrics, Topic}) of
        false -> ok;
        true -> update_counter(key(Type, Name), {2, Val})
    end.

%% @doc Decrease metric value
-spec(dec(gauge, atom()) -> integer()).
dec(gauge, Name) ->
    dec(gauge, Name, 1).

%% @doc Decrease metric value
-spec(dec(gauge, atom() | binary(), pos_integer()) -> integer()).
dec(gauge, Name, Val) when is_atom(Name) ->
    update_counter(key(gauge, Name), {2, -Val});
dec(gauge, Name = {topic_metrics, {Topic, _Event}}, Val) when is_binary(Topic) ->
    case is_being_monitored({topic_metrics, Topic}) of
        false -> ok;
        true -> update_counter(key(gauge, Name), {2, -Val})
    end.

%% @doc Set metric value
set(Name, Val) ->
    set(gauge, Name, Val).

set(gauge, Name, Val) when is_atom(Name) ->
    ets:insert(?TAB, {key(gauge, Name), Val});
set(gauge, Name = {topic_metrics, {Topic, _Event}}, Val) when is_binary(Topic) ->
    case is_being_monitored({topic_metrics, Topic}) of
        false -> ok;
        true -> ets:insert(?TAB, {key(gauge, Name), Val})
    end.

trans(inc, Name) ->
    trans(inc, {counter, Name}, 1).

trans(Opt, {gauge, Name}, Val) ->
    trans(Opt, gauge, Name, Val);
trans(inc, {counter, Name}, Val) ->
    trans(inc, counter, Name, Val);
trans(inc, Name, Val) ->
    trans(inc, counter, Name, Val);
trans(dec, gauge, Name) ->
    trans(dec, gauge, Name, 1).

trans(Opt, Type, Name, Val) when is_atom(Name) ->
    case Opt of
        inc -> hold(Type, Name, Val);
        dec -> hold(Type, Name, -Val)
    end;
trans(Opt, Type, Name = {topic_metrics, {Topic, _Event}}, Val) when is_binary(Topic) ->
    case is_being_monitored({topic_metrics, Topic}) of
        false -> ok;
        true ->
            case Opt of
                inc -> hold(Type, Name, Val);
                dec -> hold(Type, Name, -Val)
            end
    end.

hold(Type, Name, Val) when Type =:= counter orelse Type =:= gauge ->
    put('$metrics', case get('$metrics') of
                        undefined ->
                            #{{Type, Name} => Val};
                        Metrics ->
                            maps:update_with({Type, Name}, fun(Cnt) -> Cnt + Val end, Val, Metrics)
                    end).

commit() ->
    case get('$metrics') of
        undefined -> ok;
        Metrics ->
            maps:fold(fun({Type, Name}, Val, _Acc) ->
                          update_counter(key(Type, Name), {2, Val})
                      end, 0, Metrics),
            erase('$metrics')
    end.

%% @doc Metric key
key(gauge, Name) ->
    {Name, 0};
key(counter, Name) ->
    {Name, erlang:system_info(scheduler_id)}.

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
received1(?PUBLISH_PACKET(QoS, Topic, _PktId, _Payload)) ->
    inc('packets/publish/received'),
    inc('messages/received'),
    inc({topic_metrics, {Topic, 'messages/received'}}),
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
sent1(?PUBLISH_PACKET(QoS, Topic, _PktId, _Payload)) ->
    inc('packets/publish/sent'),
    inc('messages/sent'),
    inc({topic_metrics, {Topic, 'messages/sent'}}),
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
    ?LOG(error, "[Metrics] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[Metrics] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "[Metrics] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

is_being_monitored(DynamicMetric) ->
    case ets:lookup(?TAB, dynamic_metrics) of
        [] -> false;
        [{dynamic_metrics, DynamicMetrics}] ->
            lists:member(DynamicMetric, DynamicMetrics)
    end.