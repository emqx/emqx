%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_otel_trace).

-behaviour(emqx_external_trace).

-export([
    ensure_traces/1,
    start/1,
    stop/0
]).

-export([toggle_registered/1]).

%% --------------------------------------------------------------------
%% Rich Trace Mode callbacks

-export([
    client_connect/3,
    client_disconnect/3,
    client_subscribe/3,
    client_unsubscribe/3,
    client_authn/3,
    client_authz/3,

    %% Message Processing Spans (From Client)
    %% PUBLISH(form Publisher) -> ROUTE -> FORWARD(optional) -> DELIVER(to Subscribers)
    client_publish/3,
    client_puback/3,
    client_pubrec/3,
    client_pubrel/3,
    client_pubcomp/3,
    msg_route/3,
    msg_forward/3,
    msg_handle_forward/3,
    msg_deliver/2,

    %% Start Span when `emqx_channel:handle_out/3` called.
    %% Stop when `emqx_channel:handle_outgoing/3` returned
    outgoing/3
]).

%% --------------------------------------------------------------------
%% Span enrichments APIs

-export([
    add_span_attrs/1,
    add_span_event/2
]).

-include("emqx_otel_trace.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_external_trace.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(EMQX_OTEL_CTX, otel_ctx).

%%--------------------------------------------------------------------
%% config
%%--------------------------------------------------------------------

-spec toggle_registered(boolean()) -> ok | {error, term()}.
toggle_registered(true = _Enable) ->
    emqx_external_trace:register_provider(?MODULE);
toggle_registered(false = _Enable) ->
    _ = emqx_external_trace:unregister_provider(?MODULE),
    ok.

-spec ensure_traces(map()) -> ok | {error, term()}.
ensure_traces(#{traces := #{enable := true}} = Conf) ->
    start(Conf);
ensure_traces(_Conf) ->
    ok.

-spec start(map()) -> ok | {error, term()}.
start(#{traces := TracesConf, exporter := ExporterConf}) ->
    #{
        max_queue_size := MaxQueueSize,
        exporting_timeout := ExportingTimeout,
        scheduled_delay := ScheduledDelay,
        filter := #{trace_all := TraceAll}
    } = TracesConf,
    OtelEnv = [
        {bsp_scheduled_delay_ms, ScheduledDelay},
        {bsp_exporting_timeout_ms, ExportingTimeout},
        {bsp_max_queue_size, MaxQueueSize},
        {traces_exporter, emqx_otel_config:otel_exporter(ExporterConf)}
    ],
    set_trace_all(TraceAll),
    ok = application:set_env([{opentelemetry, OtelEnv}]),
    Res = assert_started(opentelemetry:start_default_tracer_provider()),
    case Res of
        ok ->
            _ = toggle_registered(true),
            Res;
        Err ->
            Err
    end.

-spec stop() -> ok.
stop() ->
    _ = toggle_registered(false),
    safe_stop_default_tracer().

%%--------------------------------------------------------------------
%% Rich mode trace API
%%--------------------------------------------------------------------

-spec client_connect(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_connect(Packet, Attrs, ProcessFun) ->
    RootCtx = otel_ctx:new(),
    SpanCtx = otel_tracer:start_span(
        RootCtx,
        ?current_tracer,
        ?CLIENT_CONNECT_SPAN_NAME,
        #{attributes => Attrs}
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    _ = otel_ctx:attach(Ctx),
    try
        ProcessFun(Packet)
    after
        _ = ?end_span(),
        clear()
    end.

-spec client_disconnect(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_disconnect(Packet, Attrs, ProcessFun) ->
    RootCtx = otel_ctx:new(),
    SpanCtx = otel_tracer:start_span(
        RootCtx,
        ?current_tracer,
        ?CLIENT_DISCONNECT_SPAN_NAME,
        #{attributes => Attrs}
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    _ = otel_ctx:attach(Ctx),
    try
        ProcessFun(Packet)
    after
        _ = ?end_span(),
        clear()
    end.

-spec client_subscribe(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_subscribe(Packet, Attrs, ProcessFun) ->
    RootCtx = otel_ctx:new(),
    SpanCtx = otel_tracer:start_span(
        RootCtx,
        ?current_tracer,
        ?CLIENT_SUBSCRIBE_SPAN_NAME,
        #{attributes => Attrs}
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    _ = otel_ctx:attach(Ctx),
    try
        ProcessFun(Packet)
    after
        _ = ?end_span(),
        clear()
    end.

-spec client_unsubscribe(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_unsubscribe(Packet, Attrs, ProcessFun) ->
    RootCtx = otel_ctx:new(),
    SpanCtx = otel_tracer:start_span(
        RootCtx,
        ?current_tracer,
        ?CLIENT_UNSUBSCRIBE_SPAN_NAME,
        #{attributes => Attrs}
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    _ = otel_ctx:attach(Ctx),
    try
        ProcessFun(Packet)
    after
        _ = ?end_span(),
        clear()
    end.

-spec client_authn(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_authn(Packet, Attrs, ProcessFun) ->
    ?with_span(
        ?CLIENT_AUTHN_SPAN_NAME,
        #{attributes => Attrs},
        fun(_SpanCtx) ->
            ProcessFun(Packet)
        end
    ).

-spec client_authz(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_authz(Packet, Attrs, ProcessFun) ->
    ?with_span(
        ?CLIENT_AUTHZ_SPAN_NAME,
        #{attributes => Attrs},
        fun(_SpanCtx) ->
            ProcessFun(Packet)
        %% TODO: add more attributes about: which authorizer resulted:
        %% allow|deny|cache_hit|cache_miss
        %% case ProcessFun(Packet) of
        %%     xx -> xx,
        end
    ).

-spec client_publish(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_publish(Packet, Attrs, ProcessFun) ->
    %% XXX: should trace for durable sessions?
    RootCtx = otel_ctx:new(),
    SpanCtx = otel_tracer:start_span(
        RootCtx,
        ?current_tracer,
        ?CLIENT_PUBLISH_SPAN_NAME,
        #{attributes => Attrs}
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    %% Otel attach for next spans (client_authz, msg_route... etc )
    _ = otel_ctx:attach(Ctx),
    %% Attach in process dictionary for outgoing/awaiting packets
    attach_outgoing(Packet, Ctx),
    try
        ProcessFun(Packet)
    after
        _ = ?end_span(),
        erase_outgoing(Packet),
        clear()
    end.

%% TODO: CONNACK, AUTH, SUBACK, UNSUBACK, DISCONNECT
-compile({inline, [attach_outgoing/2, erase_outgoing/1]}).
attach_outgoing(?PUBLISH_PACKET(?QOS_0), _Ctx) ->
    ok;
attach_outgoing(?PUBLISH_PACKET(?QOS_1, PacketId), Ctx) ->
    attach_internal_ctx({?PUBACK, PacketId}, Ctx),
    ok;
attach_outgoing(?PUBLISH_PACKET(?QOS_2, PacketId), Ctx) ->
    attach_internal_ctx({?PUBREC, PacketId}, Ctx),
    ok.

erase_outgoing(?PUBLISH_PACKET(?QOS_0)) ->
    ok;
erase_outgoing(?PUBLISH_PACKET(?QOS_1, PacketId)) ->
    earse_internal_ctx({?PUBACK, PacketId}),
    ok;
erase_outgoing(?PUBLISH_PACKET(?QOS_2, PacketId)) ->
    earse_internal_ctx({?PUBREC, PacketId}),
    ok.

-spec client_puback(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_puback(Packet, Attrs, ProcessFun) ->
    client_incoming(Packet, Attrs, ProcessFun).

-spec client_pubrec(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_pubrec(Packet, Attrs, ProcessFun) ->
    client_incoming(Packet, Attrs, ProcessFun).

-spec client_pubrel(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_pubrel(Packet, Attrs, ProcessFun) ->
    client_incoming(Packet, Attrs, ProcessFun).

-spec client_pubcomp(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
client_pubcomp(Packet, Attrs, ProcessFun) ->
    client_incoming(Packet, Attrs, ProcessFun).

-spec msg_route(
    Delivery,
    Attrs,
    fun((Delivery) -> Res)
) ->
    Res
when
    Delivery :: emqx_types:delivery(),
    Attrs :: attrs(),
    Res :: emqx_types:publish_result().
msg_route(Delivery, Attrs, Fun) ->
    case ignore_delivery(Delivery) of
        true ->
            Fun(Delivery);
        false ->
            ?with_span(
                ?MSG_ROUTE_SPAN_NAME,
                #{attributes => Attrs},
                fun(_SpanCtx) ->
                    Fun(put_ctx(otel_ctx:get_current(), Delivery))
                end
            )
    end.

-spec msg_forward(
    Delivery,
    Attrs,
    fun((_) -> Res)
) ->
    Res
when
    Delivery :: emqx_types:delivery(),
    Attrs :: attrs(),
    Res :: term().
msg_forward(Delivery, Attrs, Fun) ->
    case ignore_delivery(Delivery) of
        true ->
            Fun(Delivery);
        false ->
            ?with_span(
                ?MSG_FORWARD_SPAN_NAME,
                #{attributes => Attrs},
                fun(_SpanCtx) ->
                    Fun(put_ctx(otel_ctx:get_current(), Delivery))
                end
            )
    end.

-spec msg_handle_forward(
    Delivery,
    Attrs,
    fun((_) -> Res)
) ->
    Res
when
    Delivery :: emqx_types:delivery(),
    Attrs :: attrs(),
    Res :: term().
msg_handle_forward(Delivery, Attrs, Fun) ->
    case ignore_delivery(Delivery) of
        true ->
            Fun(Delivery);
        false ->
            _ = otel_ctx:attach(get_ctx(Delivery)),
            ?with_span(
                ?MSG_HANDLE_FORWARD_SPAN_NAME,
                #{attributes => Attrs},
                fun(_SpanCtx) ->
                    Fun(put_ctx(otel_ctx:get_current(), Delivery))
                end
            )
    end.

%% NOTE:
%% Span starts in Delivers(Msg) and stops when outgoing(Packets)
%% Only for `PUBLISH(Qos=0|1|2)`
-spec msg_deliver(
    list(Deliver),
    Attrs
) ->
    %% Delivers with Ctx Attached
    list(Deliver)
when
    Deliver :: emqx_types:deliver(),
    Attrs :: attrs().
msg_deliver(Delivers, Attrs) ->
    lists:map(
        fun({deliver, Topic, Msg} = Deliver) ->
            case get_ctx(Msg) of
                Ctx when is_map(Ctx) ->
                    SpanCtx = otel_tracer:start_span(
                        Ctx,
                        ?current_tracer,
                        ?MSG_DELIVER_SPAN_NAME,
                        #{attributes => maps:merge(Attrs, emqx_external_trace:msg_attrs(Msg))}
                    ),
                    NCtx = otel_tracer:set_current_span(Ctx, SpanCtx),
                    NMsg = put_ctx(NCtx, Msg),
                    {deliver, Topic, NMsg};
                _ ->
                    Deliver
            end
        end,
        Delivers
    ).

-spec outgoing(
    TraceAction,
    Packet,
    Attrs
) ->
    Res
when
    TraceAction :: ?EXT_TRACE_START | ?EXT_TRACE_STOP,
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
outgoing(?EXT_TRACE_START, Packet, Attrs) ->
    %% Note: the function case only for
    %% `PUBACK`, `PUBREC`, `PUBREL`, `PUBCOMP`
    start_outgoing_trace(Packet, Attrs);
outgoing(?EXT_TRACE_STOP, Any, Attrs) ->
    stop_outgoing_trace(Any, Attrs).

%% --------------------------------------------------------------------
%% Internal Helpers
%% --------------------------------------------------------------------

%% ====================
%% Broker -> Client(`Publisher'):
start_outgoing_trace(?PUBACK_PACKET(PacketId) = Packet, Attrs) ->
    start_outgoing_trace(Packet, Attrs, detach_internal_ctx({?PUBACK, PacketId}));
start_outgoing_trace(?PUBREC_PACKET(PacketId) = Packet, Attrs) ->
    start_outgoing_trace(Packet, Attrs, detach_internal_ctx({?PUBREC, PacketId}));
start_outgoing_trace(?PUBCOMP_PACKET(PacketId) = Packet, Attrs) ->
    start_outgoing_trace(Packet, Attrs, detach_internal_ctx({?PUBREL, PacketId}));
%% ====================
%% Broker -> Client(`Subscriber'):
start_outgoing_trace(?PUBREL_PACKET(PacketId) = Packet, Attrs) ->
    start_outgoing_trace(Packet, Attrs, detach_internal_ctx({?PUBREC, PacketId}));
%% The Incoming span is still being recorded and Ctx has not been erased
%% when the following outgoing spans starting.
%% `SUBACK' / `UNSUBACK' / `PUBACK'
start_outgoing_trace(Packet, Attrs) ->
    start_outgoing_trace(Packet, Attrs, otel_ctx:get_current()).

start_outgoing_trace(Packet, Attrs, ParentCtx) ->
    SpanCtx = otel_tracer:start_span(
        ParentCtx,
        ?current_tracer,
        outgoing_span_name(Packet),
        #{attributes => Attrs}
    ),
    NCtx = otel_tracer:set_current_span(ParentCtx, SpanCtx),
    _NPacketWithCtx = put_ctx(NCtx, Packet).

%% Ctx attached in Delivers in `msg_deliver/2` and
%% transformed to Packet when outgoing
stop_outgoing_trace(Anys, Attrs) when is_list(Anys) ->
    lists:foreach(fun(Any) -> stop_outgoing_trace(Any, Attrs) end, Anys);
stop_outgoing_trace(Packet, _Attrs) when is_record(Packet, mqtt_packet) ->
    %% Maybe awaiting for next Packet
    %% The current outgoing Packet SHOULD NOT be modified
    ok = outgoing_maybe_awaiting_next(Packet),
    end_span(get_ctx(Packet));
stop_outgoing_trace(Any, _Attrs) ->
    end_span(get_ctx(Any)).

-compile({inline, [end_span/1]}).
end_span(Ctx) when
    is_map(Ctx)
->
    otel_span:end_span(otel_tracer:current_span_ctx(Ctx));
end_span(_) ->
    ok.

%% Note:
%% When replying PUBACK/PUBREC/PUBCOMP to the `Publisher' or sending PUBLISH/PUBREL to the `Subscriber',
%% the current `PacketId' is stored and awaiting-trace begins.
%% At this time, a new span begins and the span ends after receiving and processing the reply
%% from Client(might be Publisher or Subscriber).

-compile({inline, [outgoing_maybe_awaiting_next/1]}).

%% ====================
%% Broker -> Client(`Publisher'):
outgoing_maybe_awaiting_next(?PACKET(?PUBACK)) ->
    %% PUBACK (QoS=1), Ignore
    ok;
outgoing_maybe_awaiting_next(?PUBREC_PACKET(PacketId) = OutgoingPubrecPacket) ->
    %% PUBREC (QoS=2), Awaiting PUBREL
    start_awaiting_trace(?PUBREL, PacketId, get_ctx(OutgoingPubrecPacket));
outgoing_maybe_awaiting_next(?PACKET(?PUBCOMP)) ->
    %% PUBCOMP (QoS=2), Ignore
    ok;
%% ====================
%% Broker -> Client(`Subscriber'):
outgoing_maybe_awaiting_next(?PUBLISH_PACKET(?QOS_0)) ->
    %% PUBLISH (QoS=0), Ignore
    ok;
outgoing_maybe_awaiting_next(?PUBLISH_PACKET(?QOS_1, PacketId) = Packet) ->
    %% PUBLISH (QoS=1), Awaiting PUBACK
    start_awaiting_trace(?PUBACK, PacketId, get_ctx(Packet));
outgoing_maybe_awaiting_next(?PUBLISH_PACKET(?QOS_2, PacketId) = Packet) ->
    %% PUBLISH (QoS=2), Awaiting PUBREC
    start_awaiting_trace(?PUBREC, PacketId, get_ctx(Packet));
outgoing_maybe_awaiting_next(?PACKET(?PUBREL, PacketId) = Packet) ->
    %% PUBREL (QoS=2), Awaiting PUBCOMP
    start_awaiting_trace(?PUBCOMP, PacketId, get_ctx(Packet));
%% ====================
outgoing_maybe_awaiting_next(_) ->
    %% TODO: Awaiting AUTH
    ok.

start_awaiting_trace(AwaitingType, PacketId, ParentCtx) ->
    AwaitingCtxKey = internal_extra_key(AwaitingType, PacketId),
    AwaitingSpanCtx = otel_tracer:start_span(
        ParentCtx,
        ?current_tracer,
        awaiting_span_name(AwaitingType),
        #{}
    ),
    NCtx = otel_tracer:set_current_span(ParentCtx, AwaitingSpanCtx),
    _ = attach_internal_ctx(AwaitingCtxKey, NCtx),
    ok.

client_incoming(?PACKET(AwaitingType, PktVar) = Packet, Attrs, ProcessFun) ->
    try
        ProcessFun(Packet)
    after
        end_awaiting_client_packet(
            internal_extra_key(AwaitingType, PktVar), Attrs
        )
    end.

end_awaiting_client_packet(AwaitingCtxKey, Attrs) ->
    case detach_internal_ctx(AwaitingCtxKey) of
        Ctx when is_map(Ctx) ->
            ok = add_span_attrs(Attrs, Ctx),
            _ = end_span(Ctx),
            earse_internal_ctx(AwaitingCtxKey),
            ok;
        _ ->
            %% TODO:
            %% erlang:get() for all internal_extra_key and erase all unknown Awaiting Type
            earse_internal_ctx(AwaitingCtxKey),
            ok
    end,
    ok.

%%--------------------------------------------------------------------

attach_internal_ctx({Type, PktVarOrPacketId}, Ctx) ->
    attach_internal_ctx(internal_extra_key(Type, PktVarOrPacketId), Ctx);
attach_internal_ctx(Key, Ctx) ->
    erlang:put(Key, Ctx).

detach_internal_ctx({Type, PktVarOrPacketId}) ->
    detach_internal_ctx(internal_extra_key(Type, PktVarOrPacketId));
detach_internal_ctx(Key) ->
    erlang:get(Key).

earse_internal_ctx({Type, PktVarOrPacketId}) ->
    earse_internal_ctx(internal_extra_key(Type, PktVarOrPacketId));
earse_internal_ctx(Key) ->
    erlang:erase(Key).

-compile(
    {inline, [
        internal_extra_key/2,
        outgoing_span_name/1,
        awaiting_span_name/1
    ]}
).

internal_extra_key(Type, PktVar) when is_tuple(PktVar) ->
    internal_extra_key(
        Type,
        emqx_packet:info(packet_id, PktVar)
    );
internal_extra_key(Type, PacketId) ->
    {
        ?MQTT_INTERNAL_EXTRA,
        emqx_packet:type_name(Type),
        PacketId
    }.

%% Outgoing Packet Span Name
%% Broker -> Client(`Subscriber'):
outgoing_span_name(?PACKET(?PUBREL)) ->
    %% PUBREL (QoS=2)
    ?EMQX_PUBREL_SPAN_NAME;
%% Broker -> Client(`Publisher'):
outgoing_span_name(?PACKET(?PUBACK)) ->
    %% PUBACK (QoS=1)
    ?EMQX_PUBACK_SPAN_NAME;
outgoing_span_name(?PACKET(?PUBREC)) ->
    %% PUBREC (QoS=2)
    ?EMQX_PUBREC_SPAN_NAME;
outgoing_span_name(?PACKET(?PUBCOMP)) ->
    %% PUBCOMP (QoS=2)
    ?EMQX_PUBCOMP_SPAN_NAME.

%% Incoming Packet Span Name
%% Client(`Publisher') -> Broker:
awaiting_span_name(?PUBREL) ->
    %% PUBREL (QoS=2)
    ?CLIENT_PUBREL_SPAN_NAME;
%% Client(`Subscriber') -> Broker:
awaiting_span_name(?PUBACK) ->
    %% PUBACK (QoS=1)
    ?CLIENT_PUBACK_SPAN_NAME;
awaiting_span_name(?PUBREC) ->
    %% PUBREC (QoS=2)
    ?CLIENT_PUBREC_SPAN_NAME;
awaiting_span_name(?PUBCOMP) ->
    %% PUBCOMP (QoS=2)
    ?CLIENT_PUBCOMP_SPAN_NAME.

%%--------------------------------------------------------------------
%% Span Attributes API
%%--------------------------------------------------------------------

-spec add_span_attrs(AttrsOrMeta) -> ok when
    AttrsOrMeta :: attrs() | attrs_meta().
add_span_attrs(?EXT_TRACE_ATTRS_META(_Meta)) ->
    %% TODO
    %% add_span_attrs(meta_to_attrs(Meta));
    ok;
add_span_attrs(Attrs) ->
    true = ?set_attributes(Attrs),
    ok.

add_span_attrs(Attrs, Ctx) ->
    CurrentSpanCtx = otel_tracer:current_span_ctx(Ctx),
    otel_span:set_attributes(CurrentSpanCtx, Attrs),
    ok.

%% TODO: remove me
-spec add_span_event(EventName, Attrs) -> ok when
    EventName :: event_name(),
    Attrs :: attrs() | attrs_meta().
add_span_event(_EventName, ?EXT_TRACE_ATTRS_META(_Meta)) ->
    %% TODO
    %% add_span_event(_EventName, meta_to_attrs(_Meta));
    ok;
add_span_event(EventName, Attrs) ->
    %% TODO
    %% The otel ctx is in Packet or Delivery
    %% not in the current process dictionary
    %% get it by internal_extra_key(Packet)
    true = ?add_event(EventName, Attrs),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ignore_delivery(#delivery{message = #message{topic = Topic}}) ->
    is_sys(Topic).

%% TODO: move to emqx_topic module?
is_sys(<<"$SYS/", _/binary>> = _Topic) -> true;
is_sys(_Topic) -> false.

%% ====================
%% Trace Context in message
put_ctx(
    OtelCtx,
    Msg = #message{extra = Extra}
) when is_map(Extra) ->
    Msg#message{extra = Extra#{?EMQX_OTEL_CTX => OtelCtx}};
%% extra field has not being used previously and defaulted to an empty list, it's safe to overwrite it
put_ctx(
    OtelCtx,
    Msg
) when is_record(Msg, message) ->
    Msg#message{extra = #{?EMQX_OTEL_CTX => OtelCtx}};
%% ====================
%% Trace Context in delivery
put_ctx(OtelCtx, #delivery{message = Msg} = Delivery) ->
    NMsg = put_ctx(OtelCtx, Msg),
    Delivery#delivery{message = NMsg};
%% ====================
%% Trace Context in packet
put_ctx(
    OtelCtx,
    #mqtt_packet{variable = #mqtt_packet_publish{properties = Props} = PubPacket} = Packet
) ->
    NProps = to_properties(OtelCtx, Props),
    Packet#mqtt_packet{variable = PubPacket#mqtt_packet_publish{properties = NProps}};
put_ctx(
    OtelCtx,
    #mqtt_packet{variable = #mqtt_packet_puback{properties = Props} = PubAckPacket} = Packet
) ->
    NProps = to_properties(OtelCtx, Props),
    Packet#mqtt_packet{variable = PubAckPacket#mqtt_packet_puback{properties = NProps}};
put_ctx(
    _OtelCtx,
    Any
) ->
    Any.

get_ctx(#message{extra = Extra}) ->
    from_extra(Extra);
get_ctx(#delivery{message = #message{extra = Extra}}) ->
    from_extra(Extra);
get_ctx(#mqtt_packet{
    variable = PktVar
}) when is_tuple(PktVar) ->
    from_extra(maps:get(?MQTT_INTERNAL_EXTRA, emqx_packet:info(properties, PktVar), #{}));
get_ctx(_) ->
    undefined.

from_extra(#{?EMQX_OTEL_CTX := OtelCtx}) ->
    OtelCtx;
from_extra(_) ->
    undefined.

to_properties(OtelCtx, Props) ->
    Extra = maps:get(?MQTT_INTERNAL_EXTRA, Props, #{}),
    Props#{?MQTT_INTERNAL_EXTRA => Extra#{?EMQX_OTEL_CTX => OtelCtx}}.

clear() ->
    otel_ctx:clear().

safe_stop_default_tracer() ->
    try
        _ = opentelemetry:stop_default_tracer_provider(),
        ok
    catch
        %% noramal scenario, opentelemetry supervisor is not started
        exit:{noproc, _} -> ok
    end,
    ok.

assert_started({ok, _Pid}) -> ok;
assert_started({ok, _Pid, _Info}) -> ok;
assert_started({error, {already_started, _Pid}}) -> ok;
assert_started({error, Reason}) -> {error, Reason}.

set_trace_all(TraceAll) ->
    persistent_term:put({?MODULE, trace_all}, TraceAll).
