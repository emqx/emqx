%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_lwm2m_session).

-include("emqx_lwm2m.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_gateway_coap/include/emqx_coap.hrl").

%% API
-export([
    new/0,
    init/4,
    update/3,
    parse_object_list/1,
    reregister/3,
    on_close/1,
    find_cmd_record/3
]).

%% Info & Stats
-export([
    info/1,
    info/2,
    stats/1,
    stats/2
]).

-export([
    handle_coap_in/3,
    handle_protocol_in/3,
    handle_deliver/3,
    timeout/3,
    send_cmd/3,
    set_reply/2
]).

%% froce update subscriptions
-export([set_subscriptions/2]).

-export_type([session/0]).

-type request_context() :: map().

-type timestamp() :: non_neg_integer().
-type queued_request() :: {timestamp(), request_context(), coap_message()}.

-type cmd_path() :: binary().
-type cmd_type() :: binary().
-type cmd_record_key() :: {cmd_path(), cmd_type()}.
-type cmd_code() :: binary().
-type cmd_code_msg() :: binary().
-type cmd_code_content() :: list(map()).
-type cmd_result() :: undefined | {cmd_code(), cmd_code_msg(), cmd_code_content()}.
-type cmd_record() :: #{
    cmd_record_key() => cmd_result(),
    queue := queue:queue()
}.

-record(session, {
    coap :: emqx_coap_tm:manager(),
    queue :: queue:queue(queued_request()),
    wait_ack :: request_context() | undefined,
    endpoint_name :: binary() | undefined,
    location_path :: list(binary()) | undefined,
    reg_info :: map() | undefined,
    lifetime :: non_neg_integer() | undefined,
    is_cache_mode :: boolean(),
    mountpoint :: binary(),
    last_active_at :: non_neg_integer(),
    created_at :: non_neg_integer(),
    cmd_record :: cmd_record(),
    subscriptions :: map()
}).

-type session() :: #session{}.

-define(PREFIX, <<"rd">>).
-define(NOW, erlang:system_time(second)).
-define(IGNORE_OBJECT, [
    <<"0">>,
    <<"1">>,
    <<"2">>,
    <<"4">>,
    <<"5">>,
    <<"6">>,
    <<"7">>,
    <<"9">>,
    <<"15">>
]).

-define(CMD_KEY(Path, Type), {Path, Type}).
-define(MAX_RECORD_SIZE, 100).
-define(RECORD_SIZE(R), (erlang:map_size(R) - 1)).

%% uplink and downlink topic configuration
-define(lwm2m_up_dm_topic, {<<"/v1/up/dm">>, 0}).

%% steal from emqx_session
-define(INFO_KEYS, [
    id,
    is_persistent,
    subscriptions,
    upgrade_qos,
    retry_interval,
    await_rel_timeout,
    created_at
]).

-define(STATS_KEYS, [
    subscriptions_cnt,
    subscriptions_max,
    inflight_cnt,
    inflight_max,
    mqueue_len,
    mqueue_max,
    mqueue_dropped,
    next_pkt_id,
    awaiting_rel_cnt,
    awaiting_rel_max,
    latency_stats
]).

-define(OUT_LIST_KEY, out_list).

-import(emqx_coap_medium, [iter/3, reply/2]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec new() -> session().
new() ->
    #session{
        coap = emqx_coap_tm:new(),
        queue = queue:new(),
        last_active_at = ?NOW,
        created_at = erlang:system_time(millisecond),
        is_cache_mode = false,
        mountpoint = <<>>,
        cmd_record = #{queue => queue:new()},
        lifetime = emqx:get_config([gateway, lwm2m, lifetime_max]),
        subscriptions = #{}
    }.

-spec init(coap_message(), binary(), function(), session()) -> map().
init(
    #coap_message{
        options = Opts,
        payload = Payload
    } = Msg,
    MountPoint,
    WithContext,
    Session
) ->
    Query = maps:get(uri_query, Opts),
    RegInfo = append_object_list(Query, Payload),
    LifeTime = get_lifetime(RegInfo),
    Epn = maps:get(<<"ep">>, Query),
    Location = [?PREFIX, Epn],

    NewSession = Session#session{
        endpoint_name = Epn,
        location_path = Location,
        reg_info = RegInfo,
        lifetime = LifeTime,
        mountpoint = MountPoint,
        is_cache_mode = is_psm(RegInfo) orelse is_qmode(RegInfo),
        queue = queue:new()
    },

    Result = return(register_init(WithContext, NewSession)),
    Reply = emqx_coap_message:piggyback({ok, created}, Msg),
    Reply2 = emqx_coap_message:set(location_path, Location, Reply),
    reply(Reply2, Result#{lifetime => true}).

reregister(Msg, WithContext, Session) ->
    update(Msg, WithContext, <<"register">>, Session).

update(Msg, WithContext, Session) ->
    update(Msg, WithContext, <<"update">>, Session).

-spec on_close(session()) -> binary().
on_close(Session) ->
    #{topic := Topic} = downlink_topic(),
    MountedTopic = mount(Topic, Session),
    emqx_broker:unsubscribe(MountedTopic),
    MountedTopic.

-spec find_cmd_record(cmd_path(), cmd_type(), session()) -> cmd_result().
find_cmd_record(Path, Type, #session{cmd_record = Record}) ->
    maps:get(?CMD_KEY(Path, Type), Record, undefined).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------
-spec info(session()) -> emqx_types:infos().
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(id, _) ->
    undefined;
info(is_persistent, _) ->
    false;
info(subscriptions, #session{subscriptions = Subs}) ->
    Subs;
info(upgrade_qos, _) ->
    false;
info(retry_interval, _) ->
    0;
info(await_rel_timeout, _) ->
    infinity;
info(created_at, #session{created_at = CreatedAt}) ->
    CreatedAt;
%% used for channel
info(location_path, #session{location_path = Path}) ->
    Path;
info(lifetime, #session{lifetime = LT}) ->
    LT;
info(reg_info, #session{reg_info = RI}) ->
    RI.

-spec stats(session()) -> emqx_types:stats().
stats(Session) -> stats(?STATS_KEYS, Session).

stats(Keys, Session) when is_list(Keys) ->
    [{Key, stats(Key, Session)} || Key <- Keys];
stats(subscriptions_cnt, #session{subscriptions = Subs}) ->
    maps:size(Subs);
stats(subscriptions_max, _) ->
    infinity;
stats(inflight_cnt, _) ->
    0;
stats(inflight_max, _) ->
    infinity;
stats(mqueue_len, _) ->
    0;
stats(mqueue_max, _) ->
    infinity;
stats(mqueue_dropped, _) ->
    0;
stats(next_pkt_id, _) ->
    0;
stats(awaiting_rel_cnt, _) ->
    0;
stats(awaiting_rel_max, _) ->
    infinity;
stats(latency_stats, _) ->
    #{}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
handle_coap_in(Msg, _WithContext, Session) ->
    call_coap(
        case emqx_coap_message:is_request(Msg) of
            true -> handle_request;
            _ -> handle_response
        end,
        Msg,
        Session#session{last_active_at = ?NOW}
    ).

handle_deliver(Delivers, WithContext, Session) ->
    return(deliver(Delivers, WithContext, Session)).

timeout({transport, Msg}, _, Session) ->
    call_coap(timeout, Msg, Session).

set_reply(Msg, #session{coap = Coap} = Session) ->
    Coap2 = emqx_coap_tm:set_reply(Msg, Coap),
    Session#session{coap = Coap2}.

send_cmd(Cmd, _, Session) ->
    return(send_cmd_impl(Cmd, Session)).

set_subscriptions(Subs, Session) ->
    Session#session{subscriptions = Subs}.

%%--------------------------------------------------------------------
%% Protocol Stack
%%--------------------------------------------------------------------
handle_protocol_in({response, CtxMsg}, WithContext, Session) ->
    return(handle_coap_response(CtxMsg, WithContext, Session));
handle_protocol_in({ack, CtxMsg}, WithContext, Session) ->
    return(handle_ack(CtxMsg, WithContext, Session));
handle_protocol_in({ack_failure, CtxMsg}, WithContext, Session) ->
    return(handle_ack_failure(CtxMsg, WithContext, Session));
handle_protocol_in({reset, CtxMsg}, WithContext, Session) ->
    return(handle_ack_reset(CtxMsg, WithContext, Session)).

%%--------------------------------------------------------------------
%% Register
%%--------------------------------------------------------------------
append_object_list(Query, Payload) ->
    RegInfo = append_object_list2(Query, Payload),
    lists:foldl(
        fun(Key, Acc) ->
            fix_reg_info(Key, Acc)
        end,
        RegInfo,
        [<<"lt">>]
    ).

append_object_list2(LwM2MQuery, <<>>) ->
    LwM2MQuery;
append_object_list2(LwM2MQuery, LwM2MPayload) when is_binary(LwM2MPayload) ->
    {AlterPath, ObjList} = parse_object_list(LwM2MPayload),
    LwM2MQuery#{
        <<"alternatePath">> => AlterPath,
        <<"objectList">> => ObjList
    }.

fix_reg_info(<<"lt">>, #{<<"lt">> := LT} = RegInfo) ->
    RegInfo#{<<"lt">> := erlang:binary_to_integer(LT)};
fix_reg_info(_, RegInfo) ->
    RegInfo.

parse_object_list(<<>>) ->
    {<<"/">>, <<>>};
parse_object_list(ObjLinks) when is_binary(ObjLinks) ->
    parse_object_list(binary:split(ObjLinks, <<",">>, [global]));
parse_object_list(FullObjLinkList) ->
    case drop_attr(FullObjLinkList) of
        {<<"/">>, _} = RootPrefixedLinks ->
            RootPrefixedLinks;
        {AlterPath, ObjLinkList} ->
            LenAlterPath = byte_size(AlterPath),
            WithOutPrefix =
                lists:map(
                    fun
                        (<<Prefix:LenAlterPath/binary, Link/binary>>) when Prefix =:= AlterPath ->
                            trim(Link);
                        (Link) ->
                            Link
                    end,
                    ObjLinkList
                ),
            {AlterPath, WithOutPrefix}
    end.

drop_attr(LinkList) ->
    lists:foldr(
        fun(Link, {AlternatePath, LinkAcc}) ->
            case parse_link(Link) of
                {false, MainLink} -> {AlternatePath, [MainLink | LinkAcc]};
                {true, MainLink} -> {MainLink, LinkAcc}
            end
        end,
        {<<"/">>, []},
        LinkList
    ).

parse_link(Link) ->
    [MainLink | Attrs] = binary:split(trim(Link), <<";">>, [global]),
    {is_alternate_path(Attrs), delink(trim(MainLink))}.

is_alternate_path(LinkAttrs) ->
    lists:any(
        fun(Attr) ->
            case binary:split(trim(Attr), <<"=">>) of
                [<<"rt">>, ?OMA_ALTER_PATH_RT] ->
                    true;
                [AttrKey, _] when AttrKey =/= <<>> ->
                    false;
                BadAttr ->
                    throw({bad_attr, BadAttr})
            end
        end,
        LinkAttrs
    ).

trim(Str) -> emqx_utils_binary:trim(Str, $\s).

delink(Str) ->
    Ltrim = emqx_utils_binary:ltrim(Str, $<),
    emqx_utils_binary:rtrim(Ltrim, $>).

get_lifetime(#{<<"lt">> := LT}) ->
    case LT of
        0 -> emqx:get_config([gateway, lwm2m, lifetime_max]);
        _ -> LT * 1000
    end;
get_lifetime(_) ->
    emqx:get_config([gateway, lwm2m, lifetime_max]).

get_lifetime(#{<<"lt">> := _} = NewRegInfo, _) ->
    get_lifetime(NewRegInfo);
get_lifetime(_, OldRegInfo) ->
    get_lifetime(OldRegInfo).

-spec update(coap_message(), function(), binary(), session()) -> map().
update(
    #coap_message{options = Opts, payload = Payload} = Msg,
    WithContext,
    CmdType,
    #session{reg_info = OldRegInfo} = Session
) ->
    Query = maps:get(uri_query, Opts, #{}),
    RegInfo = append_object_list(Query, Payload),
    UpdateRegInfo = maps:merge(OldRegInfo, RegInfo),
    LifeTime = get_lifetime(UpdateRegInfo, OldRegInfo),

    NewSession = Session#session{
        reg_info = UpdateRegInfo,
        is_cache_mode =
            is_psm(UpdateRegInfo) orelse is_qmode(UpdateRegInfo),
        lifetime = LifeTime
    },

    Session2 = proto_subscribe(WithContext, NewSession),
    Session3 = send_dl_msg(Session2),
    RegPayload = #{<<"data">> => UpdateRegInfo},
    Session4 = send_to_mqtt(#{}, CmdType, RegPayload, WithContext, Session3),

    Result = return(Session4),

    Reply = emqx_coap_message:piggyback({ok, changed}, Msg),
    reply(Reply, Result#{lifetime => true}).

register_init(WithContext, #session{reg_info = RegInfo} = Session) ->
    Session2 = send_auto_observe(RegInfo, Session),
    %% - subscribe to the downlink_topic and wait for commands
    #{topic := Topic, qos := Qos} = downlink_topic(),
    MountedTopic = mount(Topic, Session),
    SubOpts = maps:merge(
        emqx_gateway_utils:default_subopts(),
        #{qos => Qos}
    ),
    Session3 = do_subscribe(MountedTopic, SubOpts, WithContext, Session2),
    Session4 = send_dl_msg(Session3),

    %% - report the registration info
    RegPayload = #{<<"data">> => RegInfo},
    send_to_mqtt(#{}, <<"register">>, RegPayload, WithContext, Session4).

%%--------------------------------------------------------------------
%% Subscribe
%%--------------------------------------------------------------------

proto_subscribe(WithContext, #session{wait_ack = WaitAck} = Session) ->
    #{topic := Topic, qos := Qos} = downlink_topic(),
    MountedTopic = mount(Topic, Session),
    SubOpts = maps:merge(
        emqx_gateway_utils:default_subopts(),
        #{qos => Qos}
    ),
    NSession =
        case WaitAck of
            undefined ->
                Session;
            Ctx ->
                MqttPayload = emqx_lwm2m_cmd:coap_failure_to_mqtt(
                    Ctx, <<"coap_timeout">>
                ),
                send_to_mqtt(
                    Ctx,
                    <<"coap_timeout">>,
                    MqttPayload,
                    WithContext,
                    Session
                )
        end,
    do_subscribe(MountedTopic, SubOpts, WithContext, NSession).

do_subscribe(
    Topic,
    SubOpts,
    WithContext,
    Session = #session{subscriptions = Subs}
) ->
    case WithContext(subscribe, [Topic, SubOpts]) of
        {error, _} ->
            Session;
        ok ->
            NSubs = maps:put(Topic, SubOpts, Subs),
            Session#session{subscriptions = NSubs}
    end.

send_auto_observe(RegInfo, Session) ->
    %% - auto observe the objects
    case is_auto_observe() of
        true ->
            AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
            ObjectList = maps:get(<<"objectList">>, RegInfo, []),
            observe_object_list(AlternatePath, ObjectList, Session);
        _ ->
            ?SLOG(info, #{msg => "skip_auto_observe_due_to_disabled"}),
            Session
    end.

observe_object_list(_, [], Session) ->
    Session;
observe_object_list(AlternatePath, ObjectList, Session) ->
    Fun = fun(ObjectPath, Acc) ->
        {[ObjId | _], _} = emqx_lwm2m_cmd:path_list(ObjectPath),
        case lists:member(ObjId, ?IGNORE_OBJECT) of
            true ->
                Acc;
            false ->
                ObjId1 = binary_to_integer(ObjId),
                case emqx_lwm2m_xml_object_db:find_objectid(ObjId1) of
                    {error, no_xml_definition} ->
                        ?tp(
                            warning,
                            ignore_observer_resource,
                            #{
                                reason => no_xml_definition,
                                object_id => ObjId1
                            }
                        ),
                        Acc;
                    _ ->
                        observe_object(AlternatePath, ObjectPath, Acc)
                end
        end
    end,
    lists:foldl(Fun, Session, ObjectList).

observe_object(AlternatePath, ObjectPath, Session) ->
    Payload = #{
        <<"msgType">> => <<"observe">>,
        <<"data">> => #{<<"path">> => ObjectPath},
        <<"is_auto_observe">> => true
    },
    deliver_auto_observe_to_coap(AlternatePath, Payload, Session).

deliver_auto_observe_to_coap(AlternatePath, TermData, Session) ->
    ?SLOG(info, #{
        msg => "send_auto_observe",
        path => AlternatePath,
        data => TermData
    }),
    {Req0, Ctx} = emqx_lwm2m_cmd:mqtt_to_coap(AlternatePath, TermData),
    Req = alloc_token(Req0),
    maybe_do_deliver_to_coap(Ctx, Req, 0, false, Session).

is_auto_observe() ->
    emqx:get_config([gateway, lwm2m, auto_observe]).

alloc_token(Req = #coap_message{}) ->
    Req#coap_message{token = crypto:strong_rand_bytes(4)}.

%%--------------------------------------------------------------------
%% Response
%%--------------------------------------------------------------------

handle_coap_response(
    {Ctx = #{<<"msgType">> := EventType}, #coap_message{
        method = CoapMsgMethod,
        type = CoapMsgType,
        payload = CoapMsgPayload,
        options = CoapMsgOpts
    }},
    WithContext,
    Session
) ->
    MqttPayload = emqx_lwm2m_cmd:coap_to_mqtt(CoapMsgMethod, CoapMsgPayload, CoapMsgOpts, Ctx),
    {ReqPath, _} = emqx_lwm2m_cmd:path_list(emqx_lwm2m_cmd:extract_path(Ctx)),
    Session2 = record_response(EventType, MqttPayload, Session),
    Session3 =
        case {ReqPath, MqttPayload, EventType, CoapMsgType} of
            {[<<"5">> | _], _, <<"observe">>, CoapMsgType} when CoapMsgType =/= ack ->
                %% this is a notification for status update during NB firmware upgrade.
                %% need to reply to DM http callbacks
                send_to_mqtt(
                    Ctx, <<"notify">>, MqttPayload, ?lwm2m_up_dm_topic, WithContext, Session2
                );
            {_ReqPath, _, <<"observe">>, CoapMsgType} when CoapMsgType =/= ack ->
                %% this is actually a notification, correct the msgType
                send_to_mqtt(Ctx, <<"notify">>, MqttPayload, WithContext, Session2);
            _ ->
                send_to_mqtt(Ctx, EventType, MqttPayload, WithContext, Session2)
        end,
    send_dl_msg(Ctx, Session3).

%%--------------------------------------------------------------------
%% Ack
%%--------------------------------------------------------------------
handle_ack({Ctx, _}, WithContext, Session) ->
    Session2 = send_dl_msg(Ctx, Session),
    MqttPayload = emqx_lwm2m_cmd:empty_ack_to_mqtt(Ctx),
    send_to_mqtt(Ctx, <<"ack">>, MqttPayload, WithContext, Session2).

%%--------------------------------------------------------------------
%% Ack Failure(Timeout/Reset)
%%--------------------------------------------------------------------
handle_ack_failure({Ctx, _}, WithContext, Session) ->
    handle_ack_failure(Ctx, <<"coap_timeout">>, WithContext, Session).

handle_ack_reset({Ctx, _}, WithContext, Session) ->
    handle_ack_failure(Ctx, <<"coap_reset">>, WithContext, Session).

handle_ack_failure(Ctx, MsgType, WithContext, Session) ->
    Session2 = may_send_dl_msg(coap_timeout, Ctx, Session),
    MqttPayload = emqx_lwm2m_cmd:coap_failure_to_mqtt(Ctx, MsgType),
    send_to_mqtt(Ctx, MsgType, MqttPayload, WithContext, Session2).

%%--------------------------------------------------------------------
%% Send To CoAP
%%--------------------------------------------------------------------

may_send_dl_msg(coap_timeout, Ctx, #session{wait_ack = WaitAck} = Session) ->
    case is_cache_mode(Session) of
        false ->
            send_dl_msg(Ctx, Session);
        true ->
            case WaitAck of
                Ctx ->
                    Session#session{wait_ack = undefined};
                _ ->
                    Session
            end
    end.

is_cache_mode(#session{
    is_cache_mode = IsCacheMode,
    last_active_at = LastActiveAt
}) ->
    IsCacheMode andalso
        ((?NOW - LastActiveAt) >=
            emqx:get_config([gateway, lwm2m, qmode_time_window])).

is_psm(#{<<"apn">> := APN}) when
    APN =:= <<"Ctnb">>;
    APN =:= <<"psmA.eDRX0.ctnb">>;
    APN =:= <<"psmC.eDRX0.ctnb">>;
    APN =:= <<"psmF.eDRXC.ctnb">>
->
    true;
is_psm(_) ->
    false.

is_qmode(#{<<"b">> := Binding}) when
    Binding =:= <<"UQ">>;
    Binding =:= <<"SQ">>;
    Binding =:= <<"UQS">>
->
    true;
is_qmode(_) ->
    false.

send_dl_msg(Session) ->
    %% if has in waiting  donot send
    case Session#session.wait_ack of
        undefined ->
            send_to_coap(Session);
        _ ->
            Session
    end.

send_dl_msg(Ctx, Session) ->
    case Session#session.wait_ack of
        undefined ->
            send_to_coap(Session);
        Ctx ->
            send_to_coap(Session#session{wait_ack = undefined});
        _ ->
            Session
    end.

send_to_coap(#session{queue = Queue} = Session) ->
    case queue:out(Queue) of
        {{value, {Timestamp, Ctx, Req}}, Q2} ->
            Now = ?NOW,
            case Timestamp =:= 0 orelse Timestamp > Now of
                true ->
                    send_to_coap(Ctx, Req, Session#session{queue = Q2});
                false ->
                    send_to_coap(Session#session{queue = Q2})
            end;
        {empty, _} ->
            Session
    end.

send_to_coap(Ctx, Req, Session) ->
    ?SLOG(debug, #{
        msg => "deliver_to_coap",
        coap_request => Req
    }),
    out_to_coap(Ctx, Req, Session#session{wait_ack = Ctx}).

send_msg_not_waiting_ack(Ctx, Req, Session) ->
    ?SLOG(debug, #{
        msg => "deliver_to_coap_and_no_ack",
        coap_request => Req
    }),
    %%    cmd_sent(Ref, LwM2MOpts).
    out_to_coap(Ctx, Req, Session).

%%--------------------------------------------------------------------
%% Send To MQTT
%%--------------------------------------------------------------------
send_to_mqtt(Ref, EventType, Payload, WithContext, Session) ->
    #{topic := Topic, qos := Qos} = uplink_topic(EventType),
    Mheaders = maps:get(mheaders, Ref, #{}),
    proto_publish(Topic, Payload#{<<"msgType">> => EventType}, Qos, Mheaders, WithContext, Session).

send_to_mqtt(
    Ctx,
    EventType,
    Payload,
    {Topic, Qos},
    WithContext,
    Session
) ->
    Mheaders = maps:get(mheaders, Ctx, #{}),
    proto_publish(Topic, Payload#{<<"msgType">> => EventType}, Qos, Mheaders, WithContext, Session).

proto_publish(
    Topic,
    Payload,
    Qos,
    Headers,
    WithContext,
    #session{endpoint_name = Epn} = Session
) ->
    MountedTopic = mount(Topic, Session),
    %% TODO: Append message metadata into headers
    Msg = emqx_message:make(
        Epn,
        Qos,
        MountedTopic,
        emqx_utils_json:encode(Payload),
        #{},
        Headers
    ),
    _ = WithContext(publish, [MountedTopic, Msg]),
    Session.

mount(Topic, #session{mountpoint = MountPoint}) when is_binary(Topic) ->
    <<MountPoint/binary, Topic/binary>>.

%% XXX: get these confs from params instead of shared mem
downlink_topic() ->
    emqx:get_config([gateway, lwm2m, translators, command]).

uplink_topic(<<"notify">>) ->
    emqx:get_config([gateway, lwm2m, translators, notify]);
uplink_topic(<<"register">>) ->
    emqx:get_config([gateway, lwm2m, translators, register]);
uplink_topic(<<"update">>) ->
    emqx:get_config([gateway, lwm2m, translators, update]);
uplink_topic(_) ->
    emqx:get_config([gateway, lwm2m, translators, response]).

%%--------------------------------------------------------------------
%% Deliver
%%--------------------------------------------------------------------

deliver(Delivers, WithContext, #session{reg_info = RegInfo} = Session) ->
    IsCacheMode = is_cache_mode(Session),
    AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
    lists:foldl(
        fun({deliver, _, MQTT}, Acc) ->
            deliver_to_coap(
                AlternatePath,
                MQTT#message.payload,
                MQTT,
                IsCacheMode,
                WithContext,
                Acc
            )
        end,
        Session,
        Delivers
    ).

deliver_to_coap(AlternatePath, JsonData, MQTT, CacheMode, WithContext, Session) when
    is_binary(JsonData)
->
    try
        TermData = emqx_utils_json:decode(JsonData, [return_maps]),
        deliver_to_coap(AlternatePath, TermData, MQTT, CacheMode, WithContext, Session)
    catch
        ExClass:Error:ST ->
            ?SLOG(error, #{
                msg => "invaild_json_format_to_deliver",
                data => JsonData,
                reason => {ExClass, Error},
                stacktrace => ST
            }),
            WithContext(metrics, 'delivery.dropped'),
            Session
    end;
deliver_to_coap(AlternatePath, TermData, MQTT, CacheMode, WithContext, Session) when
    is_map(TermData)
->
    WithContext(metrics, 'messages.delivered'),
    {Req, Ctx} = emqx_lwm2m_cmd:mqtt_to_coap(AlternatePath, TermData),
    ExpiryTime = get_expiry_time(MQTT),
    Session2 = record_request(Ctx, Session),
    maybe_do_deliver_to_coap(Ctx, Req, ExpiryTime, CacheMode, Session2).

maybe_do_deliver_to_coap(
    Ctx,
    Req,
    ExpiryTime,
    CacheMode,
    #session{
        wait_ack = WaitAck,
        queue = Queue
    } = Session
) ->
    MHeaders = maps:get(mheaders, Ctx, #{}),
    TTL = maps:get(<<"ttl">>, MHeaders, 7200),
    case TTL of
        0 ->
            send_msg_not_waiting_ack(Ctx, Req, Session);
        _ ->
            case
                not CacheMode andalso
                    queue:is_empty(Queue) andalso WaitAck =:= undefined
            of
                true ->
                    send_to_coap(Ctx, Req, Session);
                false ->
                    Session#session{queue = queue:in({ExpiryTime, Ctx, Req}, Queue)}
            end
    end.

get_expiry_time(#message{
    headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
    timestamp = Ts
}) ->
    Ts + Interval * 1000;
get_expiry_time(_) ->
    0.

%%--------------------------------------------------------------------
%% Send CMD
%%--------------------------------------------------------------------
send_cmd_impl(Cmd, #session{reg_info = RegInfo} = Session) ->
    CacheMode = is_cache_mode(Session),
    AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
    {Req, Ctx} = emqx_lwm2m_cmd:mqtt_to_coap(AlternatePath, Cmd),
    Session2 = record_request(Ctx, Session),
    maybe_do_deliver_to_coap(Ctx, Req, 0, CacheMode, Session2).

%%--------------------------------------------------------------------
%% Call CoAP
%%--------------------------------------------------------------------
call_coap(Fun, Msg, #session{coap = Coap} = Session) ->
    iter(
        [tm, fun process_tm/4, fun process_session/3],
        emqx_coap_tm:Fun(Msg, Coap),
        Session
    ).

process_tm(TM, Result, Session, Cursor) ->
    iter(Cursor, Result, Session#session{coap = TM}).

process_session(_, Result, Session) ->
    Result#{session => Session}.

out_to_coap(Context, Msg, Session) ->
    out_to_coap({Context, Msg}, Session).

out_to_coap(Msg, Session) ->
    Outs = get_outs(),
    erlang:put(?OUT_LIST_KEY, [Msg | Outs]),
    Session.

get_outs() ->
    case erlang:get(?OUT_LIST_KEY) of
        undefined -> [];
        Any -> Any
    end.

return(#session{coap = CoAP} = Session) ->
    Outs = get_outs(),
    erlang:put(?OUT_LIST_KEY, []),
    {ok, Coap2, Msgs} = do_out(Outs, CoAP, []),
    #{return => {Msgs, Session#session{coap = Coap2}}}.

do_out([{Ctx, Out} | T], TM, Msgs) ->
    %% TODO maybe set a special token?
    #{
        out := [Msg],
        tm := TM2
    } = emqx_coap_tm:handle_out(Out, Ctx, TM),
    do_out(T, TM2, [Msg | Msgs]);
do_out(_, TM, Msgs) ->
    {ok, TM, Msgs}.

%%--------------------------------------------------------------------
%% CMD Record
%%--------------------------------------------------------------------
-spec record_request(request_context(), session()) -> session().
record_request(#{<<"msgType">> := Type} = Context, Session) ->
    Path = emqx_lwm2m_cmd:extract_path(Context),
    record_cmd(Path, Type, undefined, Session).

record_response(EventType, #{<<"data">> := Data}, Session) ->
    ReqPath = maps:get(<<"reqPath">>, Data, undefined),
    Code = maps:get(<<"code">>, Data, undefined),
    CodeMsg = maps:get(<<"codeMsg">>, Data, undefined),
    Content = maps:get(<<"content">>, Data, undefined),
    record_cmd(ReqPath, EventType, {Code, CodeMsg, Content}, Session).

record_cmd(Path, Type, Result, #session{cmd_record = #{queue := Queue} = Record} = Session) ->
    Key = ?CMD_KEY(Path, Type),
    Record2 = Record#{Key => Result},
    Queue2 = queue:in(Key, Queue),
    Record3 = check_record_size(Record2, Queue2),
    Session#session{cmd_record = Record3}.

check_record_size(Record, Queue) when ?RECORD_SIZE(Record) =< ?MAX_RECORD_SIZE ->
    Record#{queue := Queue};
check_record_size(Record, Queue) ->
    {{value, Key}, Queue2} = queue:out(Queue),
    Record2 = maps:remove(Key, Record),
    Record2#{queue := Queue2}.
