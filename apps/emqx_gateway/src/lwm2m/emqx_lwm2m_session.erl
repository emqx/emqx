%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").
-include_lib("emqx_gateway/src/lwm2m/include/emqx_lwm2m.hrl").

%% API
-export([new/0, init/3, update/3, reregister/3, on_close/1]).

-export([ info/1
        , info/2
        , stats/1
        ]).

-export([ handle_coap_in/3
        , handle_protocol_in/3
        , handle_deliver/3
        , timeout/3
        , set_reply/2]).

-export_type([session/0]).

-type request_context() :: map().

-type timestamp() :: non_neg_integer().
-type queued_request() :: {timestamp(), request_context(), emqx_coap_message()}.

-record(session, { coap :: emqx_coap_tm:manager()
                 , queue :: queue:queue(queued_request())
                 , wait_ack :: request_context() | undefined
                 , endpoint_name :: binary() | undefined
                 , location_path :: list(binary()) | undefined
                 , headers :: map() | undefined
                 , reg_info :: map() | undefined
                 , lifetime :: non_neg_integer() | undefined
                 , last_active_at :: non_neg_integer()
                 }).

-type session() :: #session{}.

-define(PREFIX, <<"rd">>).
-define(NOW, erlang:system_time(second)).
-define(IGNORE_OBJECT, [<<"0">>, <<"1">>, <<"2">>, <<"4">>, <<"5">>, <<"6">>,
                        <<"7">>, <<"9">>, <<"15">>]).

%% uplink and downlink topic configuration
-define(lwm2m_up_dm_topic,  {<<"v1/up/dm">>, 0}).

%% steal from emqx_session
-define(INFO_KEYS, [subscriptions,
                    upgrade_qos,
                    retry_interval,
                    await_rel_timeout,
                    created_at
                   ]).

-define(STATS_KEYS, [subscriptions_cnt,
                     subscriptions_max,
                     inflight_cnt,
                     inflight_max,
                     mqueue_len,
                     mqueue_max,
                     mqueue_dropped,
                     next_pkt_id,
                     awaiting_rel_cnt,
                     awaiting_rel_max
                    ]).

-define(OUT_LIST_KEY, out_list).

-import(emqx_coap_medium, [iter/3, reply/2]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec new () -> session().
new() ->
    #session{ coap = emqx_coap_tm:new()
            , queue = queue:new()
            , last_active_at = ?NOW
            , lifetime = emqx:get_config([gateway, lwm2m, lifetime_max])}.

-spec init(emqx_coap_message(), function(), session()) -> map().
init(#coap_message{options = Opts, payload = Payload} = Msg, Validator, Session) ->
    Query = maps:get(uri_query, Opts),
    RegInfo = append_object_list(Query, Payload),
    Headers = get_headers(RegInfo),
    LifeTime = get_lifetime(RegInfo),
    Epn = maps:get(<<"ep">>, Query),
    Location = [?PREFIX, Epn],

    Result = return(register_init(Validator,
                                  Session#session{headers = Headers,
                                                  endpoint_name = Epn,
                                                  location_path = Location,
                                                  reg_info = RegInfo,
                                                  lifetime = LifeTime,
                                                  queue = queue:new()})),

    Reply = emqx_coap_message:piggyback({ok, created}, Msg),
    Reply2 = emqx_coap_message:set(location_path, Location, Reply),
    reply(Reply2, Result#{lifetime => true}).

reregister(Msg, Validator, Session) ->
    update(Msg, Validator, <<"register">>, Session).

update(Msg, Validator, Session) ->
    update(Msg, Validator, <<"update">>, Session).

-spec on_close(session()) -> ok.
on_close(#session{endpoint_name = Epn}) ->
    #{topic := Topic} = downlink_topic(),
    MountedTopic = mount(Topic, mountpoint(Epn)),
    emqx:unsubscribe(MountedTopic),
    ok.

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------
-spec(info(session()) -> emqx_types:infos()).
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];

info(location_path, #session{location_path = Path}) ->
    Path;

info(lifetime, #session{lifetime = LT}) ->
    LT;

info(reg_info, #session{reg_info = RI}) ->
    RI;

info(subscriptions, _) ->
    [];
info(subscriptions_cnt, _) ->
    0;
info(subscriptions_max, _) ->
    infinity;
info(upgrade_qos, _) ->
    ?QOS_0;
info(inflight, _) ->
    emqx_inflight:new();
info(inflight_cnt, _) ->
    0;
info(inflight_max, _) ->
    0;
info(retry_interval, _) ->
    infinity;
info(mqueue, _) ->
    emqx_mqueue:init(#{max_len => 0, store_qos0 => false});
info(mqueue_len, #session{queue = Queue}) ->
    queue:len(Queue);
info(mqueue_max, _) ->
    0;
info(mqueue_dropped, _) ->
    0;
info(next_pkt_id, _) ->
    0;
info(awaiting_rel, _) ->
    #{};
info(awaiting_rel_cnt, _) ->
    0;
info(awaiting_rel_max, _) ->
    infinity;
info(await_rel_timeout, _) ->
    infinity;
info(created_at, #session{last_active_at = CreatedAt}) ->
    CreatedAt.

%% @doc Get stats of the session.
-spec(stats(session()) -> emqx_types:stats()).
stats(Session) -> info(?STATS_KEYS, Session).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
handle_coap_in(Msg, _Validator, Session) ->
    call_coap(case emqx_coap_message:is_request(Msg) of
                  true -> handle_request;
                  _ -> handle_response
              end,
              Msg, Session#session{last_active_at = ?NOW}).

handle_deliver(Delivers, _Validator, Session) ->
    return(deliver(Delivers, Session)).

timeout({transport, Msg}, _, Session) ->
    call_coap(timeout, Msg, Session).

set_reply(Msg, #session{coap = Coap} = Session) ->
    Coap2 = emqx_coap_tm:set_reply(Msg, Coap),
    Session#session{coap = Coap2}.

%%--------------------------------------------------------------------
%% Protocol Stack
%%--------------------------------------------------------------------
handle_protocol_in({response, CtxMsg}, Validator, Session) ->
    return(handle_coap_response(CtxMsg, Validator, Session));

handle_protocol_in({ack, CtxMsg}, Validator, Session) ->
    return(handle_ack(CtxMsg, Validator, Session));

handle_protocol_in({ack_failure, CtxMsg}, Validator, Session) ->
    return(handle_ack_failure(CtxMsg, Validator, Session));

handle_protocol_in({reset, CtxMsg}, Validator, Session) ->
    return(handle_ack_reset(CtxMsg, Validator, Session)).

%%--------------------------------------------------------------------
%% Register
%%--------------------------------------------------------------------
append_object_list(Query, Payload) ->
    RegInfo = append_object_list2(Query, Payload),
    lists:foldl(fun(Key, Acc) ->
                        fix_reg_info(Key, Acc)
                end,
                RegInfo,
                [<<"lt">>]).

append_object_list2(LwM2MQuery, <<>>) -> LwM2MQuery;
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

parse_object_list(<<>>) -> {<<"/">>, <<>>};
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
                      (Link) -> Link
                         end, ObjLinkList),
            {AlterPath, WithOutPrefix}
    end.

drop_attr(LinkList) ->
    lists:foldr(
      fun(Link, {AlternatePath, LinkAcc}) ->
              case parse_link(Link) of
                  {false, MainLink} -> {AlternatePath, [MainLink | LinkAcc]};
                  {true, MainLink}  -> {MainLink, LinkAcc}
              end
      end, {<<"/">>, []}, LinkList).

parse_link(Link) ->
    [MainLink | Attrs] = binary:split(trim(Link), <<";">>, [global]),
    {is_alternate_path(Attrs), delink(trim(MainLink))}.

is_alternate_path(LinkAttrs) ->
    lists:any(fun(Attr) ->
                      case binary:split(trim(Attr), <<"=">>) of
                          [<<"rt">>, ?OMA_ALTER_PATH_RT] ->
                              true;
                          [AttrKey, _] when AttrKey =/= <<>> ->
                              false;
                          _BadAttr -> throw({bad_attr, _BadAttr})
                      end
              end,
              LinkAttrs).

trim(Str)-> binary_util:trim(Str, $ ).

delink(Str) ->
    Ltrim = binary_util:ltrim(Str, $<),
    binary_util:rtrim(Ltrim, $>).

get_headers(RegInfo) ->
    lists:foldl(fun(K, Acc) ->
                        get_header(K, RegInfo, Acc)
                end,
                extract_module_params(RegInfo),
                [<<"apn">>, <<"im">>, <<"ct">>, <<"mv">>, <<"mt">>]).

get_header(Key, RegInfo, Headers) ->
    case maps:get(Key, RegInfo, undefined) of
        undefined ->
            Headers;
        Val ->
            AtomKey = erlang:binary_to_atom(Key),
            Headers#{AtomKey => Val}
    end.

extract_module_params(RegInfo) ->
    Keys = [<<"module">>, <<"sv">>, <<"chip">>, <<"imsi">>, <<"iccid">>],
    case lists:any(fun(K) -> maps:get(K, RegInfo, undefined) =:= undefined end, Keys) of
        true -> #{module_params => undefined};
        false ->
            Extras = [<<"rsrp">>, <<"sinr">>, <<"txpower">>, <<"cellid">>],
            case lists:any(fun(K) -> maps:get(K, RegInfo, undefined) =:= undefined end, Extras) of
                true ->
                    #{module_params =>
                          #{module => maps:get(<<"module">>, RegInfo),
                            softversion => maps:get(<<"sv">>, RegInfo),
                            chiptype => maps:get(<<"chip">>, RegInfo),
                            imsi => maps:get(<<"imsi">>, RegInfo),
                            iccid => maps:get(<<"iccid">>, RegInfo)}};
                false ->
                    #{module_params =>
                          #{module => maps:get(<<"module">>, RegInfo),
                            softversion => maps:get(<<"sv">>, RegInfo),
                            chiptype => maps:get(<<"chip">>, RegInfo),
                            imsi => maps:get(<<"imsi">>, RegInfo),
                            iccid => maps:get(<<"iccid">>, RegInfo),
                            rsrp => maps:get(<<"rsrp">>, RegInfo),
                            sinr => maps:get(<<"sinr">>, RegInfo),
                            txpower => maps:get(<<"txpower">>, RegInfo),
                            cellid => maps:get(<<"cellid">>, RegInfo)}}
            end
    end.

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

-spec update(emqx_coap_message(), function(), binary(), session()) -> map().
update(#coap_message{options = Opts, payload = Payload} = Msg,
       Validator,
       CmdType,
       #session{reg_info = OldRegInfo} = Session) ->
    Query = maps:get(uri_query, Opts),
    RegInfo = append_object_list(Query, Payload),
    UpdateRegInfo = maps:merge(OldRegInfo, RegInfo),
    LifeTime = get_lifetime(UpdateRegInfo, OldRegInfo),

    Session2 = proto_subscribe(Validator,
                               Session#session{reg_info = UpdateRegInfo,
                                               lifetime = LifeTime}),
    Session3 = send_dl_msg(Session2),
    RegPayload = #{<<"data">> => UpdateRegInfo},
    Session4 = send_to_mqtt(#{}, CmdType, RegPayload, Validator, Session3),

    Result = return(Session4),

    Reply = emqx_coap_message:piggyback({ok, changed}, Msg),
    reply(Reply, Result#{lifetime => true}).

register_init(Validator, #session{reg_info = RegInfo,
                                  endpoint_name = Epn} = Session) ->

    Session2 = send_auto_observe(RegInfo, Session),
    %% - subscribe to the downlink_topic and wait for commands
    #{topic := Topic, qos := Qos} = downlink_topic(),
    MountedTopic = mount(Topic, mountpoint(Epn)),
    Session3 = subscribe(MountedTopic, Qos, Validator, Session2),
    Session4 = send_dl_msg(Session3),

    %% - report the registration info
    RegPayload = #{<<"data">> => RegInfo},
    send_to_mqtt(#{}, <<"register">>, RegPayload, Validator, Session4).

%%--------------------------------------------------------------------
%% Subscribe
%%--------------------------------------------------------------------
proto_subscribe(Validator, #session{endpoint_name = Epn, wait_ack = WaitAck} = Session) ->
    #{topic := Topic, qos := Qos} = downlink_topic(),
    MountedTopic = mount(Topic, mountpoint(Epn)),
    Session2 = case WaitAck of
                   undefined ->
                       Session;
                   Ctx ->
                       MqttPayload = emqx_lwm2m_cmd:coap_failure_to_mqtt(Ctx, <<"coap_timeout">>),
                       send_to_mqtt(Ctx, <<"coap_timeout">>, MqttPayload, Validator, Session)
               end,
    subscribe(MountedTopic, Qos, Validator, Session2).

subscribe(Topic, Qos, Validator,
          #session{headers = Headers, endpoint_name = EndpointName} = Session) ->
    case Validator(subscribe, Topic) of
        allow ->
            ClientId = maps:get(device_id, Headers, undefined),
            Opts = get_sub_opts(Qos),
            ?LOG(debug, "Subscribe topic: ~0p, Opts: ~0p, EndpointName: ~0p", [Topic, Opts, EndpointName]),
            emqx:subscribe(Topic, ClientId, Opts);
        _ ->
            ?LOG(error, "Topic: ~0p not allow to subscribe", [Topic])
    end,
    Session.

send_auto_observe(RegInfo, Session) ->
    %% - auto observe the objects
    case is_auto_observe() of
        true ->
            AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
            ObjectList = maps:get(<<"objectList">>, RegInfo, []),
            observe_object_list(AlternatePath, ObjectList, Session);
        _ ->
            ?LOG(info, "Auto Observe Disabled", []),
            Session
    end.

observe_object_list(_, [], Session) ->
    Session;
observe_object_list(AlternatePath, ObjectList, Session) ->
    Fun = fun(ObjectPath, Acc) ->
                  {[ObjId| _], _} = emqx_lwm2m_cmd:path_list(ObjectPath),
                  case lists:member(ObjId, ?IGNORE_OBJECT) of
                      true -> Acc;
                      false ->
                          try
                              emqx_lwm2m_xml_object_db:find_objectid(binary_to_integer(ObjId)),
                              observe_object(AlternatePath, ObjectPath, Acc)
                          catch error:no_xml_definition ->
                                  Acc
                          end
                  end
          end,
    lists:foldl(Fun, Session, ObjectList).

observe_object(AlternatePath, ObjectPath, Session) ->
    Payload = #{<<"msgType">> => <<"observe">>,
                <<"data">> => #{<<"path">> => ObjectPath},
                <<"is_auto_observe">> => true
               },
    deliver_auto_observe_to_coap(AlternatePath, Payload, Session).

deliver_auto_observe_to_coap(AlternatePath, TermData, Session) ->
    ?LOG(info, "Auto Observe, SEND To CoAP, AlternatePath=~0p, Data=~0p ", [AlternatePath, TermData]),
    {Req, Ctx} = emqx_lwm2m_cmd:mqtt_to_coap(AlternatePath, TermData),
    maybe_do_deliver_to_coap(Ctx, Req, 0, false, Session).

get_sub_opts(Qos) ->
    #{
      qos => Qos,
      rap => 0,
      nl => 0,
      rh => 0,
      is_new => false
     }.

is_auto_observe() ->
    emqx:get_config([gateway, lwm2m, auto_observe]).

%%--------------------------------------------------------------------
%% Response
%%--------------------------------------------------------------------
handle_coap_response({Ctx = #{<<"msgType">> := EventType},
                      #coap_message{method = CoapMsgMethod,
                                    type = CoapMsgType,
                                    payload = CoapMsgPayload,
                                    options = CoapMsgOpts}},
                     Validator,
                     Session) ->
    MqttPayload = emqx_lwm2m_cmd:coap_to_mqtt(CoapMsgMethod, CoapMsgPayload, CoapMsgOpts, Ctx),
    {ReqPath, _} = emqx_lwm2m_cmd:path_list(emqx_lwm2m_cmd:extract_path(Ctx)),
    Session2 =
        case {ReqPath, MqttPayload, EventType, CoapMsgType} of
            {[<<"5">>| _], _, <<"observe">>, CoapMsgType} when CoapMsgType =/= ack ->
                %% this is a notification for status update during NB firmware upgrade.
                %% need to reply to DM http callbacks
                send_to_mqtt(Ctx, <<"notify">>, MqttPayload, ?lwm2m_up_dm_topic, Validator, Session);
            {_ReqPath, _, <<"observe">>, CoapMsgType} when CoapMsgType =/= ack ->
                %% this is actually a notification, correct the msgType
                send_to_mqtt(Ctx, <<"notify">>, MqttPayload, Validator, Session);
            _ ->
                send_to_mqtt(Ctx, EventType, MqttPayload, Validator, Session)
        end,
    send_dl_msg(Ctx, Session2).

%%--------------------------------------------------------------------
%% Ack
%%--------------------------------------------------------------------
handle_ack({Ctx, _}, Validator, Session) ->
    Session2 = send_dl_msg(Ctx, Session),
    MqttPayload = emqx_lwm2m_cmd:empty_ack_to_mqtt(Ctx),
    send_to_mqtt(Ctx, <<"ack">>, MqttPayload, Validator, Session2).

%%--------------------------------------------------------------------
%% Ack Failure(Timeout/Reset)
%%--------------------------------------------------------------------
handle_ack_failure({Ctx, _}, Validator, Session) ->
    handle_ack_failure(Ctx, <<"coap_timeout">>, Validator, Session).

handle_ack_reset({Ctx, _}, Validator, Session) ->
    handle_ack_failure(Ctx, <<"coap_reset">>, Validator, Session).

handle_ack_failure(Ctx, MsgType, Validator, Session) ->
    Session2 = may_send_dl_msg(coap_timeout, Ctx, Session),
    MqttPayload = emqx_lwm2m_cmd:coap_failure_to_mqtt(Ctx, MsgType),
    send_to_mqtt(Ctx, MsgType, MqttPayload, Validator, Session2).

%%--------------------------------------------------------------------
%% Send To CoAP
%%--------------------------------------------------------------------

may_send_dl_msg(coap_timeout, Ctx, #session{headers = Headers,
                                            reg_info = RegInfo,
                                            wait_ack = WaitAck} = Session) ->
    Lwm2mMode = maps:get(lwm2m_model, Headers, undefined),
    case is_cache_mode(Lwm2mMode, RegInfo, Session) of
        false -> send_dl_msg(Ctx, Session);
        true ->
            case WaitAck of
                Ctx ->
                    Session#session{wait_ack = undefined};
                _ ->
                    Session
            end
    end.

is_cache_mode(Lwm2mMode, RegInfo, #session{last_active_at = LastActiveAt}) ->
    case Lwm2mMode =:= psm orelse is_psm(RegInfo) orelse is_qmode(RegInfo) of
        true ->
            QModeTimeWind = emqx:get_config([gateway, lwm2m, qmode_time_window]),
            Now = ?NOW,
            (Now - LastActiveAt) >= QModeTimeWind;
        false -> false
    end.

is_psm(#{<<"apn">> := APN}) when APN =:= <<"Ctnb">>;
                                 APN =:= <<"psmA.eDRX0.ctnb">>;
                                 APN =:= <<"psmC.eDRX0.ctnb">>;
                                 APN =:= <<"psmF.eDRXC.ctnb">>
                                 -> true;
is_psm(_) -> false.

is_qmode(#{<<"b">> := Binding}) when Binding =:= <<"UQ">>;
                                     Binding =:= <<"SQ">>;
                                     Binding =:= <<"UQS">>
                                     -> true;
is_qmode(_) -> false.

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
            if Timestamp =:= 0 orelse Timestamp > Now ->
                    send_to_coap(Ctx, Req, Session#session{queue = Q2});
               true ->
                    send_to_coap(Session#session{queue = Q2})
            end;
        {empty, _} ->
            Session
    end.

send_to_coap(Ctx, Req, Session) ->
    ?LOG(debug, "Deliver To CoAP, CoapRequest: ~0p", [Req]),
    out_to_coap(Ctx, Req, Session#session{wait_ack = Ctx}).

send_msg_not_waiting_ack(Ctx, Req, Session) ->
    ?LOG(debug, "Deliver To CoAP not waiting ack, CoapRequest: ~0p", [Req]),
    %%    cmd_sent(Ref, LwM2MOpts).
    out_to_coap(Ctx, Req, Session).

%%--------------------------------------------------------------------
%% Send To MQTT
%%--------------------------------------------------------------------
send_to_mqtt(Ref, EventType, Payload, Validator, Session = #session{headers = Headers}) ->
    #{topic := Topic, qos := Qos} = uplink_topic(EventType),
    NHeaders = extract_ext_flags(Headers),
    Mheaders = maps:get(mheaders, Ref, #{}),
    NHeaders1 = maps:merge(NHeaders, Mheaders),
    proto_publish(Topic, Payload#{<<"msgType">> => EventType}, Qos, NHeaders1, Validator, Session).

send_to_mqtt(Ctx, EventType, Payload, {Topic, Qos},
             Validator, #session{headers = Headers} = Session) ->
    Mheaders = maps:get(mheaders, Ctx, #{}),
    NHeaders = extract_ext_flags(Headers),
    NHeaders1 = maps:merge(NHeaders, Mheaders),
    proto_publish(Topic, Payload#{<<"msgType">> => EventType}, Qos, NHeaders1, Validator, Session).

proto_publish(Topic, Payload, Qos, Headers, Validator,
              #session{endpoint_name = Epn} = Session) ->
    MountedTopic = mount(Topic, mountpoint(Epn)),
    _ = case Validator(publish, MountedTopic) of
            allow ->
                Msg = emqx_message:make(Epn, Qos, MountedTopic,
                                        emqx_json:encode(Payload), #{}, Headers),
                emqx:publish(Msg);
            _ ->
                ?LOG(error, "topic:~p not allow to publish ", [MountedTopic])
        end,
    Session.

mountpoint(Epn) ->
    Prefix = emqx:get_config([gateway, lwm2m, mountpoint]),
    <<Prefix/binary, "/", Epn/binary, "/">>.

mount(Topic, MountPoint) when is_binary(Topic), is_binary(MountPoint) ->
    <<MountPoint/binary, Topic/binary>>.

extract_ext_flags(Headers) ->
    Header0 = #{is_tr => maps:get(is_tr, Headers, true)},
    check(Header0, Headers, [sota_type, appId, nbgwFlag]).

check(Params, _Headers, []) -> Params;
check(Params, Headers, [Key | Rest]) ->
    case maps:get(Key, Headers, null) of
        V when V == undefined; V == null ->
            check(Params, Headers, Rest);
        Value ->
            Params1 = Params#{Key => Value},
            check(Params1, Headers, Rest)
    end.

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

deliver(Delivers, #session{headers = Headers, reg_info = RegInfo} = Session) ->
    Lwm2mMode = maps:get(lwm2m_model, Headers, undefined),
    IsCacheMode = is_cache_mode(Lwm2mMode, RegInfo, Session),
    AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
    lists:foldl(fun({deliver, _, MQTT}, Acc) ->
                        deliver_to_coap(AlternatePath,
                                        MQTT#message.payload, MQTT, IsCacheMode, Acc)
                end,
                Session,
                Delivers).

deliver_to_coap(AlternatePath, JsonData, MQTT, CacheMode, Session) when is_binary(JsonData)->
    try
        TermData = emqx_json:decode(JsonData, [return_maps]),
        deliver_to_coap(AlternatePath, TermData, MQTT, CacheMode, Session)
    catch
        ExClass:Error:ST ->
            ?LOG(error, "deliver_to_coap - Invalid JSON: ~0p, Exception: ~0p, stacktrace: ~0p",
                 [JsonData, {ExClass, Error}, ST]),
            Session
    end;

deliver_to_coap(AlternatePath, TermData, MQTT, CacheMode, Session) when is_map(TermData) ->
    {Req, Ctx} = emqx_lwm2m_cmd:mqtt_to_coap(AlternatePath, TermData),
    ExpiryTime = get_expiry_time(MQTT),
    maybe_do_deliver_to_coap(Ctx, Req, ExpiryTime, CacheMode, Session).

maybe_do_deliver_to_coap(Ctx, Req, ExpiryTime, CacheMode,
                         #session{wait_ack = WaitAck,
                                  queue = Queue} = Session) ->
    MHeaders = maps:get(mheaders, Ctx, #{}),
    TTL = maps:get(<<"ttl">>, MHeaders, 7200),
    case TTL of
        0 ->
            send_msg_not_waiting_ack(Ctx, Req, Session);
        _ ->
            case not CacheMode
                andalso queue:is_empty(Queue) andalso WaitAck =:= undefined of
                true ->
                    send_to_coap(Ctx, Req, Session);
                false ->
                    Session#session{queue = queue:in({ExpiryTime, Ctx, Req}, Queue)}
            end
    end.

get_expiry_time(#message{headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
                         timestamp = Ts}) ->
    Ts + Interval * 1000;
get_expiry_time(_) ->
    0.

%%--------------------------------------------------------------------
%% Call CoAP
%%--------------------------------------------------------------------
call_coap(Fun, Msg, #session{coap = Coap} = Session) ->
    iter([tm, fun process_tm/4, fun process_session/3],
         emqx_coap_tm:Fun(Msg, Coap),
         Session).

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
    #{out := [Msg],
      tm := TM2} = emqx_coap_tm:handle_out(Out, Ctx, TM),
    do_out(T, TM2, [Msg | Msgs]);

do_out(_, TM, Msgs) ->
    {ok, TM, Msgs}.
