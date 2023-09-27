%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([toggle_registered/1]).

-export([
    trace_process_publish/3,
    start_trace_send/2,
    end_trace_send/1,
    event/2
]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

-define(EMQX_OTEL_CTX, otel_ctx).
%% NOTE: it's possible to use trace_flags to set is_sampled flag
-define(IS_ENABLED, emqx_enable).
-define(USER_PROPERTY, 'User-Property').

-define(TRACE_ALL, emqx:get_config([opentelemetry, trace, trace_all], false)).
%%--------------------------------------------------------------------
%% config
%%--------------------------------------------------------------------

-spec toggle_registered(boolean()) -> ok | {error, term()}.
toggle_registered(true = _Enable) ->
    emqx_external_trace:register_provider(?MODULE);
toggle_registered(false = _Enable) ->
    _ = emqx_external_trace:unregister_provider(?MODULE),
    ok.

%%--------------------------------------------------------------------
%% trace API
%%--------------------------------------------------------------------

-spec trace_process_publish(Packet, Channel, fun((Packet, Channel) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    Channel :: emqx_channel:channel(),
    Res :: term().
trace_process_publish(Packet, Channel, ProcessFun) ->
    case maybe_init_ctx(Packet) of
        false ->
            ProcessFun(Packet, Channel);
        RootCtx ->
            RootCtx1 = otel_ctx:set_value(RootCtx, ?IS_ENABLED, true),
            Attrs = maps:merge(packet_attributes(Packet), channel_attributes(Channel)),
            SpanCtx = otel_tracer:start_span(RootCtx1, ?current_tracer, process_message, #{
                attributes => Attrs
            }),
            Ctx = otel_tracer:set_current_span(RootCtx1, SpanCtx),
            %% put ctx to packet, so it can be further propagated
            Packet1 = put_ctx_to_packet(Ctx, Packet),
            %% TODO: consider getting rid of propagating Ctx through process dict as it's anyway seems to have
            %% very limited usage
            otel_ctx:attach(Ctx),
            try
                ProcessFun(Packet1, Channel)
            after
                ?end_span(),
                clear()
            end
    end.

-spec start_trace_send(list(emqx_types:deliver()), emqx_channel:channel()) ->
    list(emqx_types:deliver()).
start_trace_send(Delivers, Channel) ->
    lists:map(
        fun({deliver, Topic, Msg} = Deliver) ->
            case get_ctx_from_msg(Msg) of
                Ctx when is_map(Ctx) ->
                    Attrs = maps:merge(
                        msg_attributes(Msg), sub_channel_attributes(Channel)
                    ),
                    StartOpts = #{attributes => Attrs},
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

-spec end_trace_send(emqx_types:packet() | [emqx_types:packet()]) -> ok.
end_trace_send(Packets) ->
    lists:foreach(
        fun(Packet) ->
            case get_ctx_from_packet(Packet) of
                Ctx when is_map(Ctx) ->
                    otel_span:end_span(otel_tracer:current_span_ctx(Ctx));
                _ ->
                    ok
            end
        end,
        packets_list(Packets)
    ).

%% NOTE: adds an event only within an active span (Otel Ctx must be set in the calling process dict)
-spec event(opentelemetry:event_name(), opentelemetry:attributes_map()) -> ok.
event(Name, Attributes) ->
    case otel_ctx:get_value(?IS_ENABLED, false) of
        true ->
            ?add_event(Name, Attributes),
            ok;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

packets_list(Packets) when is_list(Packets) ->
    Packets;
packets_list(Packet) ->
    [Packet].

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

%% TODO: move to emqx_topic module?
is_sys(<<"$SYS/", _/binary>> = _Topic) -> true;
is_sys(_Topic) -> false.

msg_attributes(Msg) ->
    #{
        'mqtt.topic' => emqx_message:topic(Msg),
        'mqtt.from.clientid' => emqx_message:from(Msg)
    }.

packet_attributes(#mqtt_packet{variable = Packet}) ->
    #{'mqtt.topic' => emqx_packet:info(topic_name, Packet)}.

channel_attributes(Channel) ->
    #{'mqtt.subscriber.clientid' => emqx_channel:info(clientid, Channel)}.

sub_channel_attributes(Channel) ->
    Attrs = channel_attributes(Channel),
    case emqx_channel:info({session, subscriptions}, Channel) of
        Subs when is_map(Subs) ->
            Attrs#{'mqtt.subscriber.subscriptions' => maps:keys(Subs)};
        _ ->
            Attrs
    end.

put_ctx_to_msg(OtelCtx, Msg = #message{extra = Extra}) when is_map(Extra) ->
    Msg#message{extra = Extra#{?EMQX_OTEL_CTX => OtelCtx}};
%% extra field has not being used previously and defaulted to an empty list, it's safe to overwrite it
put_ctx_to_msg(OtelCtx, Msg) when is_record(Msg, message) ->
    Msg#message{extra = #{?EMQX_OTEL_CTX => OtelCtx}}.

put_ctx_to_packet(
    OtelCtx, #mqtt_packet{variable = #mqtt_packet_publish{properties = Props} = PubPacket} = Packet
) ->
    Extra = maps:get(internal_extra, Props, #{}),
    Props1 = Props#{internal_extra => Extra#{?EMQX_OTEL_CTX => OtelCtx}},
    Packet#mqtt_packet{variable = PubPacket#mqtt_packet_publish{properties = Props1}}.

get_ctx_from_msg(#message{extra = Extra}) ->
    from_extra(Extra).

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
