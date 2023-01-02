%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_protocol).

-include("emqx_lwm2m.hrl").

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").

%% API.
-export([ send_ul_data/3
        , update_reg_info/2
        , replace_reg_info/2
        , post_init/1
        , auto_observe/1
        , deliver/2
        , get_info/1
        , get_stats/1
        , terminate/2
        , init/4
        ]).

%% For Mgmt
-export([ call/2
        , call/3
        ]).

-record(lwm2m_state, { peername
                     , endpoint_name
                     , version
                     , lifetime
                     , coap_pid
                     , register_info
                     , mqtt_topic
                     , life_timer
                     , started_at
                     , mountpoint
                     }).

-define(DEFAULT_KEEP_ALIVE_DURATION,  60*2).

-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

-define(SUBOPTS, #{rh => 0, rap => 0, nl => 0, qos => 0, is_new => true}).

-define(LOG(Level, Format, Args), logger:Level("LWM2M-PROTO: " ++ Format, Args)).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

call(Pid, Msg) ->
    call(Pid, Msg, 5000).

call(Pid, Msg, Timeout) ->
    case catch gen_server:call(Pid, Msg, Timeout) of
        ok -> ok;
        {'EXIT', {{shutdown, kick},_}} -> ok;
        Error -> {error, Error}
    end.

init(CoapPid, EndpointName, Peername = {_Peerhost, _Port},
     RegInfo = #{<<"lt">> := LifeTime, <<"lwm2m">> := Ver}) ->
    Mountpoint = proplists:get_value(mountpoint, lwm2m_coap_responder:options(), ""),
    Lwm2mState = #lwm2m_state{peername = Peername,
                              endpoint_name = EndpointName,
                              version = Ver,
                              lifetime = LifeTime,
                              coap_pid = CoapPid,
                              register_info = RegInfo,
                              mountpoint = Mountpoint},
    ClientInfo = clientinfo(Lwm2mState),
    _ = run_hooks('client.connect', [conninfo(Lwm2mState)], undefined),
    case emqx_access_control:authenticate(ClientInfo) of
        {ok, AuthResult} ->
            _ = run_hooks('client.connack', [conninfo(Lwm2mState), success], undefined),

            ClientInfo1 = maps:merge(ClientInfo, AuthResult),
            Sockport = proplists:get_value(port, lwm2m_coap_responder:options(), 5683),
            ClientInfo2 = maps:put(sockport, Sockport, ClientInfo1),
            Lwm2mState1 = Lwm2mState#lwm2m_state{started_at = time_now(),
                                                 mountpoint = maps:get(mountpoint, ClientInfo2)},
            run_hooks('client.connected', [ClientInfo2, conninfo(Lwm2mState1)]),

            erlang:send(CoapPid, post_init),
            erlang:send_after(2000, CoapPid, auto_observe),

            _ = emqx_cm_locker:trans(EndpointName, fun(_) ->
                emqx_cm:register_channel(EndpointName, CoapPid, conninfo(Lwm2mState1))
            end),
            emqx_cm:insert_channel_info(EndpointName, info(Lwm2mState1), stats(Lwm2mState1)),
            emqx_lwm2m_cm:register_channel(EndpointName, RegInfo, LifeTime, Ver, Peername),

            NTimer = emqx_lwm2m_timer:start_timer(LifeTime, {life_timer, expired}),
            {ok, Lwm2mState1#lwm2m_state{life_timer = NTimer}};
        {error, Error} ->
            _ = run_hooks('client.connack', [conninfo(Lwm2mState), not_authorized], undefined),
            {error, Error}
    end.

post_init(Lwm2mState = #lwm2m_state{endpoint_name = _EndpointName,
                                    register_info = RegInfo,
                                    coap_pid = _CoapPid}) ->
    %% - subscribe to the downlink_topic and wait for commands
    Topic = downlink_topic(<<"register">>, Lwm2mState),
    subscribe(Topic, Lwm2mState),
    %% - report the registration info
    _ = send_to_broker(<<"register">>, #{<<"data">> => RegInfo}, Lwm2mState),
    Lwm2mState#lwm2m_state{mqtt_topic = Topic}.

update_reg_info(NewRegInfo, Lwm2mState=#lwm2m_state{life_timer = LifeTimer, register_info = RegInfo,
                                                    coap_pid = CoapPid, endpoint_name = Epn}) ->
    UpdatedRegInfo = maps:merge(RegInfo, NewRegInfo),

    _ = case proplists:get_value(update_msg_publish_condition,
            lwm2m_coap_responder:options(), contains_object_list) of
        always ->
            send_to_broker(<<"update">>, #{<<"data">> => UpdatedRegInfo}, Lwm2mState);
        contains_object_list ->
            %% - report the registration info update, but only when objectList is updated.
            case NewRegInfo of
                #{<<"objectList">> := _} ->
                    emqx_lwm2m_cm:update_reg_info(Epn, NewRegInfo),
                    send_to_broker(<<"update">>, #{<<"data">> => UpdatedRegInfo}, Lwm2mState);
                _ -> ok
            end
    end,

    %% - flush cached donwlink commands
    _ = flush_cached_downlink_messages(CoapPid),

    %% - update the life timer
    UpdatedLifeTimer = emqx_lwm2m_timer:refresh_timer(
                            maps:get(<<"lt">>, UpdatedRegInfo), LifeTimer),

    ?LOG(debug, "Update RegInfo to: ~p", [UpdatedRegInfo]),
    Lwm2mState#lwm2m_state{life_timer = UpdatedLifeTimer,
                           register_info = UpdatedRegInfo}.

replace_reg_info(NewRegInfo, Lwm2mState=#lwm2m_state{life_timer = LifeTimer,
                                                     coap_pid = CoapPid,
                                                     endpoint_name = EndpointName}) ->
    _ = send_to_broker(<<"register">>, #{<<"data">> => NewRegInfo}, Lwm2mState),

    %% - flush cached donwlink commands
    _ = flush_cached_downlink_messages(CoapPid),

    %% - update the life timer
    UpdatedLifeTimer = emqx_lwm2m_timer:refresh_timer(
                            maps:get(<<"lt">>, NewRegInfo), LifeTimer),

    _ = send_auto_observe(CoapPid, NewRegInfo, EndpointName),

    ?LOG(debug, "Replace RegInfo to: ~p", [NewRegInfo]),
    Lwm2mState#lwm2m_state{life_timer = UpdatedLifeTimer,
                           register_info = NewRegInfo}.

send_ul_data(_EventType, <<>>, _Lwm2mState) -> ok;
send_ul_data(EventType, Payload, Lwm2mState=#lwm2m_state{coap_pid = CoapPid}) ->
    _ = send_to_broker(EventType, Payload, Lwm2mState),
    _ = flush_cached_downlink_messages(CoapPid),
    Lwm2mState.

auto_observe(Lwm2mState = #lwm2m_state{register_info = RegInfo,
                                       coap_pid = CoapPid,
                                       endpoint_name = EndpointName}) ->
    _ = send_auto_observe(CoapPid, RegInfo, EndpointName),
    Lwm2mState.

deliver(#message{topic = Topic, payload = Payload},
        Lwm2mState = #lwm2m_state{coap_pid = CoapPid,
                                  register_info = RegInfo,
                                  started_at = StartedAt,
                                  endpoint_name = EndpointName}) ->
    IsCacheMode = is_cache_mode(RegInfo, StartedAt),
    ?LOG(debug, "Get MQTT message from broker, IsCacheModeNow?: ~p, "
                "Topic: ~p, Payload: ~p", [IsCacheMode, Topic, Payload]),
    AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
    deliver_to_coap(AlternatePath, Payload, CoapPid, IsCacheMode, EndpointName),
    Lwm2mState.

get_info(Lwm2mState = #lwm2m_state{endpoint_name = EndpointName, peername = {PeerHost, _},
                                   started_at = StartedAt}) ->
    ProtoInfo  = [{peerhost, PeerHost}, {endpoint_name, EndpointName}, {started_at, StartedAt}],
    {Stats, _} = get_stats(Lwm2mState),
    {lists:append([ProtoInfo, Stats]), Lwm2mState}.

get_stats(Lwm2mState) ->
    Stats = emqx_misc:proc_stats(),
    {Stats, Lwm2mState}.

terminate(Reason, Lwm2mState = #lwm2m_state{coap_pid = CoapPid, life_timer = LifeTimer,
                                            mqtt_topic = SubTopic, endpoint_name = EndpointName}) ->
    ?LOG(debug, "process terminated: ~p", [Reason]),

    emqx_cm:unregister_channel(EndpointName),

    is_reference(LifeTimer) andalso emqx_lwm2m_timer:cancel_timer(LifeTimer),
    clean_subscribe(CoapPid, Reason, SubTopic, Lwm2mState);
terminate(Reason, Lwm2mState) ->
    ?LOG(error, "process terminated: ~p, lwm2m_state: ~p", [Reason, Lwm2mState]).

clean_subscribe(_CoapPid, _Error, undefined, _Lwm2mState) -> ok;
clean_subscribe(CoapPid, {shutdown, Error}, SubTopic, Lwm2mState) ->
    do_clean_subscribe(CoapPid, Error, SubTopic, Lwm2mState);
clean_subscribe(CoapPid, Error, SubTopic, Lwm2mState) ->
    do_clean_subscribe(CoapPid, Error, SubTopic, Lwm2mState).

do_clean_subscribe(_CoapPid, Error, SubTopic, Lwm2mState) ->
    ?LOG(debug, "unsubscribe ~p while exiting", [SubTopic]),
    unsubscribe(SubTopic, Lwm2mState),

    ConnInfo0 = conninfo(Lwm2mState),
    ConnInfo = ConnInfo0#{disconnected_at => erlang:system_time(millisecond)},
    run_hooks('client.disconnected', [clientinfo(Lwm2mState), Error, ConnInfo]).

subscribe(Topic, Lwm2mState = #lwm2m_state{endpoint_name = EndpointName}) ->
    emqx_broker:subscribe(Topic, EndpointName, ?SUBOPTS),
    emqx_hooks:run('session.subscribed', [clientinfo(Lwm2mState), Topic, ?SUBOPTS]).

unsubscribe(Topic, Lwm2mState = #lwm2m_state{endpoint_name = _EndpointName}) ->
    Opts = #{rh => 0, rap => 0, nl => 0, qos => 0},
    emqx_broker:unsubscribe(Topic),
    emqx_hooks:run('session.unsubscribed', [clientinfo(Lwm2mState), Topic, Opts]).

publish(Topic, Payload, Qos,
        #lwm2m_state{
           version = ProtoVer,
           peername = {PeerHost, _},
           endpoint_name = EndpointName}) ->
    Message = emqx_message:set_flag(
                retain, false,
                emqx_message:make(EndpointName, Qos, Topic, Payload)
               ),
    NMessage = emqx_message:set_headers(
                 #{proto_ver => ProtoVer,
                   protocol => lwm2m,
                   peerhost => PeerHost}, Message),
    emqx_broker:publish(NMessage).

time_now() -> erlang:system_time(millisecond).

%%--------------------------------------------------------------------
%% Deliver downlink message to coap
%%--------------------------------------------------------------------

deliver_to_coap(AlternatePath, JsonData,
                CoapPid, CacheMode, EndpointName) when is_binary(JsonData)->
    try
        TermData = emqx_json:decode(JsonData, [return_maps]),
        deliver_to_coap(AlternatePath, TermData, CoapPid, CacheMode, EndpointName)
    catch
        C:R:Stack ->
            ?LOG(error, "deliver_to_coap - Invalid JSON: ~p, Exception: ~p, stacktrace: ~p",
                [JsonData, {C, R}, Stack])
    end;

deliver_to_coap(AlternatePath, TermData, CoapPid, CacheMode, EndpointName) when is_map(TermData) ->
    ?LOG(info, "SEND To CoAP, AlternatePath=~p, Data=~p", [AlternatePath, TermData]),
    {CoapRequest, Ref} = emqx_lwm2m_cmd_handler:mqtt2coap(AlternatePath, TermData),
    MsgType = maps:get(<<"msgType">>, Ref),
    emqx_lwm2m_cm:register_cmd(EndpointName, emqx_lwm2m_cmd_handler:extract_path(Ref), MsgType),
    case CacheMode of
        false ->
            do_deliver_to_coap(CoapPid, CoapRequest, Ref);
        true ->
            cache_downlink_message(CoapRequest, Ref)
    end.

%%--------------------------------------------------------------------
%% Send uplink message to broker
%%--------------------------------------------------------------------

send_to_broker(EventType, Payload = #{}, Lwm2mState) ->
    do_send_to_broker(EventType, Payload, Lwm2mState).

do_send_to_broker(EventType, #{<<"data">> := Data} = Payload,
                  #lwm2m_state{endpoint_name = EndpointName} = Lwm2mState) ->
    ReqPath = maps:get(<<"reqPath">>, Data, undefined),
    Code = maps:get(<<"code">>, Data, undefined),
    CodeMsg = maps:get(<<"codeMsg">>, Data, undefined),
    Content = maps:get(<<"content">>, Data, undefined),
    emqx_lwm2m_cm:register_cmd(EndpointName, ReqPath, EventType, {Code, CodeMsg, Content}),
    NewPayload = maps:put(<<"msgType">>, EventType, Payload),
    Topic = uplink_topic(EventType, Lwm2mState),
    publish(Topic, emqx_json:encode(NewPayload), _Qos = 0, Lwm2mState).

%%--------------------------------------------------------------------
%% Auto Observe
%%--------------------------------------------------------------------

auto_observe_object_list(true = _Expected, Registered) ->
    Registered;
auto_observe_object_list(Expected, Registered) ->
    Expected1 = lists:map(fun(S) -> iolist_to_binary(S) end, Expected),
    lists:filter(fun(S) -> lists:member(S, Expected1) end, Registered).

send_auto_observe(CoapPid, RegInfo, EndpointName) ->
    %% - auto observe the objects
    case proplists:get_value(auto_observe, lwm2m_coap_responder:options(), false) of
        false ->
            ?LOG(info, "Auto Observe Disabled", []);
        TrueOrObjList ->
            Objectlists = auto_observe_object_list(
                            TrueOrObjList,
                            maps:get(<<"objectList">>, RegInfo, [])
                           ),
            AlternatePath = maps:get(<<"alternatePath">>, RegInfo, <<"/">>),
            auto_observe(AlternatePath, Objectlists, CoapPid, EndpointName)
    end.

auto_observe(AlternatePath, ObjectList, CoapPid, EndpointName) ->
    ?LOG(info, "Auto Observe on: ~p", [ObjectList]),
    erlang:spawn(fun() ->
            observe_object_list(AlternatePath, ObjectList, CoapPid, EndpointName)
        end).

observe_object_list(AlternatePath, ObjectList, CoapPid, EndpointName) ->
    lists:foreach(fun(ObjectPath) ->
        [ObjId | LastPath] = emqx_lwm2m_cmd_handler:path_list(ObjectPath),
        case ObjId of
            <<"19">> ->
                [ObjInsId | _LastPath1] = LastPath,
                case ObjInsId of
                    <<"0">> ->
                        observe_object_slowly(
                          AlternatePath, <<"/19/0/0">>,
                          CoapPid, 100, EndpointName
                         );
                    _ ->
                        observe_object_slowly(
                          AlternatePath, ObjectPath,
                          CoapPid, 100, EndpointName
                         )
                end;
            _ ->
                observe_object_slowly(
                  AlternatePath, ObjectPath,
                  CoapPid, 100, EndpointName
                 )
        end
    end, ObjectList).

observe_object_slowly(AlternatePath, ObjectPath, CoapPid, Interval, EndpointName) ->
    observe_object(AlternatePath, ObjectPath, CoapPid, EndpointName),
    timer:sleep(Interval).

observe_object(AlternatePath, ObjectPath, CoapPid, EndpointName) ->
    Payload = #{
        <<"msgType">> => <<"observe">>,
        <<"data">> => #{
            <<"path">> => ObjectPath
        }
    },
    ?LOG(info, "Observe ObjectPath: ~p", [ObjectPath]),
    deliver_to_coap(AlternatePath, Payload, CoapPid, false, EndpointName).

do_deliver_to_coap_slowly(CoapPid, CoapRequestList, Interval) ->
    erlang:spawn(fun() ->
        lists:foreach(fun({CoapRequest, Ref}) ->
                _ = do_deliver_to_coap(CoapPid, CoapRequest, Ref),
                timer:sleep(Interval)
            end, lists:reverse(CoapRequestList))
        end).

do_deliver_to_coap(CoapPid, CoapRequest, Ref) ->
    ?LOG(debug, "Deliver To CoAP(~p), CoapRequest: ~p", [CoapPid, CoapRequest]),
    CoapPid ! {deliver_to_coap, CoapRequest, Ref}.

%%--------------------------------------------------------------------
%% Queue Mode
%%--------------------------------------------------------------------

cache_downlink_message(CoapRequest, Ref) ->
    ?LOG(debug, "Cache downlink coap request: ~p, Ref: ~p", [CoapRequest, Ref]),
    put(dl_msg_cache, [{CoapRequest, Ref} | get_cached_downlink_messages()]).

flush_cached_downlink_messages(CoapPid) ->
    case erase(dl_msg_cache) of
        CachedMessageList when is_list(CachedMessageList)->
            do_deliver_to_coap_slowly(CoapPid, CachedMessageList, 100);
        undefined -> ok
    end.

get_cached_downlink_messages() ->
    case get(dl_msg_cache) of
        undefined -> [];
        CachedMessageList -> CachedMessageList
    end.

is_cache_mode(RegInfo, StartedAt) ->
    case is_psm(RegInfo) orelse is_qmode(RegInfo) of
        true ->
            QModeTimeWind = proplists:get_value(
                              qmode_time_window,
                              lwm2m_coap_responder:options(),
                              22
                             ),
            (time_now() - StartedAt) >= QModeTimeWind;
        false -> false
    end.

is_psm(_) -> false.

is_qmode(#{<<"b">> := Binding}) when Binding =:= <<"UQ">>;
                                     Binding =:= <<"SQ">>;
                                     Binding =:= <<"UQS">>
            -> true;
is_qmode(_) -> false.

%%--------------------------------------------------------------------
%% Construct downlink and uplink topics
%%--------------------------------------------------------------------

downlink_topic(EventType, Lwm2mState = #lwm2m_state{mountpoint = Mountpoint}) ->
    Topics = proplists:get_value(topics, lwm2m_coap_responder:options(), []),
    DnTopic = proplists:get_value(downlink_topic_key(EventType), Topics,
                                  default_downlink_topic(EventType)),
    take_place(mountpoint(iolist_to_binary(DnTopic), Mountpoint), Lwm2mState).

uplink_topic(EventType, Lwm2mState = #lwm2m_state{mountpoint = Mountpoint}) ->
    Topics = proplists:get_value(topics, lwm2m_coap_responder:options(), []),
    UpTopic = proplists:get_value(uplink_topic_key(EventType), Topics,
                                  default_uplink_topic(EventType)),
    take_place(mountpoint(iolist_to_binary(UpTopic), Mountpoint), Lwm2mState).

downlink_topic_key(EventType) when is_binary(EventType) ->
    command.

uplink_topic_key(<<"notify">>) -> notify;
uplink_topic_key(<<"register">>) -> register;
uplink_topic_key(<<"update">>) -> update;
uplink_topic_key(EventType) when is_binary(EventType) ->
    response.

default_downlink_topic(Type) when is_binary(Type)->
    <<"dn/#">>.

default_uplink_topic(<<"notify">>) ->
    <<"up/notify">>;
default_uplink_topic(Type) when is_binary(Type) ->
    <<"up/resp">>.

take_place(Text, Lwm2mState) ->
    {IPAddr, _} = Lwm2mState#lwm2m_state.peername,
    IPAddrBin = iolist_to_binary(inet:ntoa(IPAddr)),
    take_place(take_place(Text, <<"%a">>, IPAddrBin),
                    <<"%e">>, Lwm2mState#lwm2m_state.endpoint_name).

take_place(Text, Placeholder, Value) ->
    binary:replace(Text, Placeholder, Value, [global]).

clientinfo(#lwm2m_state{peername = {PeerHost, _},
                        endpoint_name = EndpointName,
                        mountpoint = Mountpoint}) ->
    #{zone => undefined,
      protocol => lwm2m,
      peerhost => PeerHost,
      sockport => 5683,         %% FIXME:
      clientid => EndpointName,
      username => undefined,
      password => undefined,
      peercert => nossl,
      is_bridge => false,
      is_superuser => false,
      mountpoint => Mountpoint,
      ws_cookie => undefined
     }.

mountpoint(Topic, <<>>) ->
    Topic;
mountpoint(Topic, Mountpoint) ->
    <<Mountpoint/binary, Topic/binary>>.

%%--------------------------------------------------------------------
%% Helper funcs

-compile({inline, [run_hooks/2, run_hooks/3]}).
run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run_fold(Name, Args, Acc).

%%--------------------------------------------------------------------
%% Info & Stats

info(State) ->
    ChannInfo = chann_info(State),
    ChannInfo#{sockinfo => sockinfo(State)}.

%% copies from emqx_connection:info/1
sockinfo(#lwm2m_state{peername = Peername}) ->
    #{socktype => udp,
      peername => Peername,
      sockname => {{127,0,0,1}, 5683},    %% FIXME: Sock?
      sockstate =>  running,
      active_n => 1
     }.

%% copies from emqx_channel:info/1
chann_info(State) ->
    #{conninfo => conninfo(State),
      conn_state => connected,
      clientinfo => clientinfo(State),
      session => maps:from_list(session_info(State)),
      will_msg => undefined
     }.

conninfo(#lwm2m_state{peername = Peername,
                      version = Ver,
                      started_at = StartedAt,
                      endpoint_name = Epn}) ->
    #{socktype => udp,
      sockname => {{127,0,0,1}, 5683},
      peername => Peername,
      peercert => nossl,        %% TODO: dtls
      conn_mod => ?MODULE,
      proto_name => <<"LwM2M">>,
      proto_ver => Ver,
      clean_start => true,
      clientid => Epn,
      username => undefined,
      conn_props => undefined,
      connected => true,
      connected_at => StartedAt,
      keepalive => 0,
      receive_maximum => 0,
      expiry_interval => 0
     }.

%% copies from emqx_session:info/1
session_info(#lwm2m_state{mqtt_topic = SubTopic, started_at = StartedAt}) ->
    [{subscriptions, #{SubTopic => ?SUBOPTS}},
     {upgrade_qos, false},
     {retry_interval, 0},
     {await_rel_timeout, 0},
     {created_at, StartedAt}
    ].

%% The stats keys copied from emqx_connection:stats/1
stats(_State) ->
    SockStats = [{recv_oct,0}, {recv_cnt,0}, {send_oct,0}, {send_cnt,0}, {send_pend,0}],
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = [{subscriptions_cnt, 1},
                 {subscriptions_max, 1},
                 {inflight_cnt, 0},
                 {inflight_max, 0},
                 {mqueue_len, 0},
                 {mqueue_max, 0},
                 {mqueue_dropped, 0},
                 {next_pkt_id, 0},
                 {awaiting_rel_cnt, 0},
                 {awaiting_rel_max, 0}
                ],
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

