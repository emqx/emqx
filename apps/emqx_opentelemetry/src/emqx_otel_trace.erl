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
    msg_route/3,
    msg_dispatch/3,
    msg_forward/3
]).

%% --------------------------------------------------------------------
%% Span enrichments APIs

-export([
    add_span_attrs/1,
    add_span_event/2
]).

%% --------------------------------------------------------------------
%% Legacy Mode callbacks

-export([
    trace_process_publish/3,
    start_trace_send/2,
    end_trace_send/1
]).

-include("emqx_otel_trace.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_external_trace.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

-define(EMQX_OTEL_CTX, otel_ctx).
-define(USER_PROPERTY, 'User-Property').

-define(TRACE_ALL_KEY, {?MODULE, trace_all}).
-define(TRACE_ALL, persistent_term:get(?TRACE_ALL_KEY, false)).

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
        #{
            attributes => gen_attrs(Packet, Attrs)
        }
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    _ = otel_ctx:attach(Ctx),
    CurrentSpanCtx = otel_tracer:current_span_ctx(),
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
        #{
            attributes => gen_attrs(Packet, Attrs)
        }
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
        #{
            attributes => gen_attrs(Packet, Attrs)
        }
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    %% put ctx to packet, so it can be further propagated
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
        #{
            attributes => gen_attrs(Packet, Attrs)
        }
    ),
    Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
    %% put ctx to packet, so it can be further propagated
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
client_authn(Packet, _Attrs, ProcessFun) ->
    ?with_span(
        ?CLIENT_AUTHN_SPAN_NAME,
        #{
            %% attributes => gen_attrs(Packet, Attrs)
        },
        fun(_SpanCtx) ->
            ProcessFun(Packet)
        %% case ProcessFun(Packet) of
        %%     xx -> xx,
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
client_authz(Packet, _Attrs, ProcessFun) ->
    ?with_span(
        ?CLIENT_AUTHZ_SPAN_NAME,
        #{
            %% TODO: add more attributes
            attributes => #{}
        },
        fun(_SpanCtx) ->
            ProcessFun(Packet)
        end
    ).

-spec msg_route(
    Delivery,
    Attrs,
    fun(() -> Res)
) ->
    Res
when
    Delivery :: emqx_types:delivery(),
    Attrs :: attrs(),
    Res :: term().
msg_route(Delivery, Attrs, Fun) ->
    case ignore_delivery(Delivery) of
        true ->
            Fun(Delivery);
        false ->
            ?with_span(
                ?MSG_ROUTE_SPAN_NAME,
                #{attributes => Attrs},
                fun(_SpanCtx) ->
                    Fun(Delivery)
                end
            )
    end.

-spec msg_dispatch(
    Delivery,
    Attrs,
    fun(() -> Res)
) ->
    Res
when
    Delivery :: emqx_types:delivery(),
    Attrs :: attrs(),
    Res :: term().
msg_dispatch(Delivery, Attrs, Fun) ->
    case ignore_delivery(Delivery) of
        true ->
            Fun(Delivery);
        false ->
            ?with_span(
                ?MSG_DISPATCH_SPAN_NAME,
                #{attributes => Attrs},
                fun(_SpanCtx) ->
                    Fun(Delivery)
                end
            )
    end.

-spec msg_forward(
    Delivery,
    Attrs,
    fun(() -> Res)
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
                    Fun(Delivery)
                end
            )
    end.

%% --------------------------------------------------------------------
%% Legacy trace API
%% --------------------------------------------------------------------

-spec trace_process_publish(
    Packet,
    Attrs,
    fun((Packet) -> Res)
) ->
    Res
when
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().
trace_process_publish(Packet, ChannelInfo, ProcessFun) ->
    case maybe_init_ctx(Packet) of
        false ->
            ProcessFun(Packet);
        RootCtx ->
            NAttrs = maps:merge(packet_attributes(Packet), channel_attributes(ChannelInfo)),
            SpanCtx = otel_tracer:start_span(RootCtx, ?current_tracer, process_message, #{
                attributes => NAttrs
            }),
            Ctx = otel_tracer:set_current_span(RootCtx, SpanCtx),
            %% put ctx to packet, so it can be further propagated
            Packet1 = put_ctx_to_packet(Ctx, Packet),
            _ = otel_ctx:attach(Ctx),
            try
                ProcessFun(Packet1)
            after
                _ = ?end_span(),
                clear()
            end
    end.

-spec start_trace_send(
    list(Delivers),
    Attrs
) ->
    list(Delivers)
when
    Delivers :: emqx_types:deliver(),
    Attrs :: attrs().
start_trace_send(Delivers, Attrs) ->
    lists:map(
        fun({deliver, Topic, Msg} = Deliver) ->
            case get_ctx_from_msg(Msg) of
                Ctx when is_map(Ctx) ->
                    NAttrs = maps:merge(
                        msg_attributes(Msg), sub_channel_attributes(Attrs)
                    ),
                    StartOpts = #{attributes => NAttrs},
                    SpanCtx = otel_tracer:start_span(
                        Ctx, ?current_tracer, send_published_message, StartOpts
                    ),
                    Msg1 = put_ctx_to_msg(
                        otel_tracer:set_current_span(Ctx, SpanCtx), Msg
                    ),
                    {deliver, Topic, Msg1};
                _ ->
                    Deliver
            end
        end,
        Delivers
    ).

-spec end_trace_send(
    Packet | [Packet]
) ->
    ok
when
    Packet :: emqx_types:packet().
end_trace_send(Packets) when
    is_list(Packets)
->
    lists:foreach(
        fun(Packet) ->
            case get_ctx_from_packet(Packet) of
                Ctx when is_map(Ctx) ->
                    otel_span:end_span(otel_tracer:current_span_ctx(Ctx));
                _ ->
                    ok
            end
        end,
        Packets
    );
end_trace_send(Packet) ->
    end_trace_send([Packet]).

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

-spec add_span_event(EventName, Attrs) -> ok when
    EventName :: event_name(),
    Attrs :: attrs() | attrs_meta().
add_span_event(_EventName, ?EXT_TRACE_ATTRS_META(_Meta)) ->
    %% TODO
    %% add_span_event(_EventName, meta_to_attrs(_Meta));
    ok;
add_span_event(EventName, Attrs) ->
    true = ?add_event(EventName, Attrs),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
maybe_init_ctx(#mqtt_packet{variable = Packet}) ->
    case should_trace_packet(Packet) of
        true ->
            Ctx = extract_traceparent_from_packet(Packet),
            should_trace_context(Ctx) andalso Ctx;
        false ->
            false
    end.

extract_traceparent_from_packet(Packet) ->
    Ctx = otel_ctx:new(),
    case emqx_packet:info(properties, Packet) of
        #{?USER_PROPERTY := UserProps} ->
            otel_propagator_text_map:extract_to(Ctx, UserProps);
        _ ->
            Ctx
    end.

should_trace_context(RootCtx) ->
    map_size(RootCtx) > 0 orelse ?TRACE_ALL.

should_trace_packet(Packet) ->
    not is_sys(emqx_packet:info(topic_name, Packet)).

ignore_delivery(#delivery{message = #message{topic = Topic}}) ->
    is_sys(Topic).

%% TODO: move to emqx_topic module?
is_sys(<<"$SYS/", _/binary>> = _Topic) -> true;
is_sys(_Topic) -> false.

msg_attributes(Msg) ->
    #{
        'messaging.destination.name' => emqx_message:topic(Msg),
        'messaging.client_id' => emqx_message:from(Msg)
    }.

%% TODO: refactor to use raw `Attrs` or `AttrsMeta`
%% FIXME:
%% function_clause for DISCONNECT packet
%% XXX:
%% emqx_packet:info/2 as utils

%% gen_attrs(Packet, Attrs) ->
gen_attrs(
    ?PACKET(?CONNECT, PktVar),
    #{
        clientid := ClientId,
        username := Username
    } = _InitAttrs
) ->
    #{
        'client.connect.clientid' => ClientId,
        'client.connect.username' => Username,
        'client.connect.proto_name' => emqx_packet:info(proto_name, PktVar),
        'client.connect.proto_ver' => emqx_packet:info(proto_ver, PktVar)
    };
gen_attrs(?PACKET(?DISCONNECT, PktVar), InitAttrs) ->
    #{
        'client.disconnect.clientid' => maps:get(clientid, InitAttrs, undefined),
        'client.disconnect.username' => maps:get(username, InitAttrs, undefined),
        'client.disconnect.peername' => maps:get(peername, InitAttrs, undefined),
        'client.disconnect.sockname' => maps:get(sockname, InitAttrs, undefined),
        'client.disconnect.reason_code' => emqx_packet:info(reason_code, PktVar)
    };
gen_attrs(
    ?PACKET(?SUBSCRIBE) = Packet,
    #{clientid := ClientId} = _Attrs
) ->
    #{
        'client.subscribe.clientid' => ClientId,
        'client.subscribe.topic_filters' => serialize_topic_filters(Packet)
    };
gen_attrs(
    ?PACKET(?UNSUBSCRIBE) = Packet,
    #{clientid := ClientId} = _Attrs
) ->
    #{
        'client.unsubscribe.clientid' => ClientId,
        'client.unsubscribe.topic_filters' => serialize_topic_filters(Packet)
    }.

serialize_topic_filters(?PACKET(?SUBSCRIBE, PktVar)) ->
    TFs = [Name || {Name, _SubOpts} <- emqx_packet:info(topic_filters, PktVar)],
    emqx_utils_json:encode(TFs);
serialize_topic_filters(?PACKET(?UNSUBSCRIBE, PktVar)) ->
    emqx_utils_json:encode(emqx_packet:info(topic_filters, PktVar)).

packet_attributes(#mqtt_packet{
    header = #mqtt_packet_header{type = ?PUBLISH},
    variable = PubPacket
}) ->
    #{'messaging.destination.name' => emqx_packet:info(topic_name, PubPacket)}.

channel_attributes(ChannelInfo) ->
    #{'messaging.client_id' => maps:get(clientid, ChannelInfo, undefined)}.

sub_channel_attributes(ChannelInfo) ->
    channel_attributes(ChannelInfo).

%% ====================
%% Trace Context in message
put_ctx_to_msg(OtelCtx, Msg = #message{extra = Extra}) when is_map(Extra) ->
    Msg#message{extra = Extra#{?EMQX_OTEL_CTX => OtelCtx}};
%% extra field has not being used previously and defaulted to an empty list, it's safe to overwrite it
put_ctx_to_msg(OtelCtx, Msg) when is_record(Msg, message) ->
    Msg#message{extra = #{?EMQX_OTEL_CTX => OtelCtx}}.

get_ctx_from_msg(#message{extra = Extra}) ->
    from_extra(Extra).

%% ====================
%% Trace Context in packet
put_ctx_to_packet(
    OtelCtx, #mqtt_packet{variable = #mqtt_packet_publish{properties = Props} = PubPacket} = Packet
) ->
    Extra = maps:get(internal_extra, Props, #{}),
    Props1 = Props#{internal_extra => Extra#{?EMQX_OTEL_CTX => OtelCtx}},
    Packet#mqtt_packet{variable = PubPacket#mqtt_packet_publish{properties = Props1}}.

get_ctx_from_packet(#mqtt_packet{
    variable = #mqtt_packet_publish{properties = #{internal_extra := Extra}}
}) ->
    from_extra(Extra);
get_ctx_from_packet(_) ->
    undefined.

from_extra(#{?EMQX_OTEL_CTX := OtelCtx}) ->
    OtelCtx;
from_extra(_) ->
    undefined.

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
