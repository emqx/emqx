%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_ingress).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(MAX_SUBSCRIPTION_ID, 268435455).

%% management APIs
-export([
    status/1,
    info/1,
    subscribe_channel/2,
    unsubscribe_channel/5,
    config/4
]).

%% `ecpool` API helpers
-export([
    add_reconnect_callback/2,
    remove_reconnect_callback/2
]).
%% `ecpool` API
-export([
    on_reconnect/2,
    get_reconnect_callback_signature/1
]).

-export([handle_publish/4]).

subscribe_channel(PoolName, IngressConfig) ->
    Workers = ecpool:workers(PoolName),
    PoolSize = length(Workers),
    Results = [
        subscribe_channel(Pid, Name, IngressConfig, Idx, PoolSize)
     || {{Name, Idx}, Pid} <- Workers
    ],
    case proplists:get_value(error, Results, ok) of
        ok ->
            ok;
        Error ->
            Error
    end.

subscribe_channel(WorkerPid, Name, IngressConfig, WorkerIdx, PoolSize) ->
    case ecpool_worker:client(WorkerPid) of
        {ok, Client} ->
            subscribe_channel_helper(Client, Name, IngressConfig, WorkerIdx, PoolSize);
        {error, Reason} ->
            error({client_not_found, Reason})
    end.

subscribe_channel_helper(Client, Name, IngressConfig, WorkerIdx, PoolSize) ->
    IngressList = maps:get(ingress_list, IngressConfig, []),
    SubscribeResults = subscribe_remote_topics(
        Client, IngressList, WorkerIdx, PoolSize, Name
    ),
    %% Find error if any using proplists:get_value/2
    case proplists:get_value(error, SubscribeResults, ok) of
        ok ->
            ok;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "ingress_client_subscribe_failed",
                ingress => IngressConfig,
                name => Name,
                reason => Reason
            }),
            Error
    end.

subscribe_remote_topics(Pid, IngressList, WorkerIdx, PoolSize, Name) ->
    [subscribe_remote_topic(Pid, Ingress, WorkerIdx, PoolSize, Name) || Ingress <- IngressList].

subscribe_remote_topic(
    Pid,
    #{remote := #{topic := RemoteTopic, qos := QoS, no_local := NoLocal}} = Ingress,
    WorkerIdx,
    PoolSize,
    Name
) ->
    case should_subscribe(RemoteTopic, WorkerIdx, PoolSize, Name, true) of
        true ->
            maybe_retry_without_subscription_identifier(
                Ingress,
                emqtt:subscribe(
                    Pid,
                    subscribe_properties(Ingress),
                    RemoteTopic,
                    [{qos, QoS}, {nl, NoLocal}]
                ),
                Pid
            );
        false ->
            ok
    end.

subscribe_properties(#{subscription_id := SubscriptionId}) ->
    #{'Subscription-Identifier' => SubscriptionId};
subscribe_properties(_Ingress) ->
    #{}.

maybe_retry_without_subscription_identifier(
    #{subscription_id := _SubscriptionId, remote := #{topic := <<"$queue/", _/binary>>}},
    {ok, _Props, ReasonCodes} = Result,
    _Pid
) ->
    case lists:member(?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED, ReasonCodes) of
        true ->
            {error, subscription_identifier_required_for_queue_subscription};
        false ->
            Result
    end;
maybe_retry_without_subscription_identifier(
    #{
        subscription_id := _SubscriptionId,
        remote := #{topic := RemoteTopic, qos := QoS, no_local := NoLocal}
    },
    {ok, _Props, ReasonCodes} = Result,
    Pid
) ->
    case lists:member(?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED, ReasonCodes) of
        true ->
            emqtt:subscribe(Pid, #{}, RemoteTopic, [{qos, QoS}, {nl, NoLocal}]);
        false ->
            Result
    end;
maybe_retry_without_subscription_identifier(_Ingress, Result, _Pid) ->
    Result.

should_subscribe(RemoteTopic, WorkerIdx, PoolSize, Name, LogWarn) ->
    IsFirstWorker = WorkerIdx == 1,
    case emqx_topic:parse(RemoteTopic) of
        {#share{} = _Filter, _SubOpts} ->
            %% NOTE: this is shared subscription, many workers may subscribe
            true;
        {_Filter, #{}} ->
            case PoolSize > 1 orelse IsFirstWorker orelse LogWarn of
                true ->
                    ?SLOG(warning, #{
                        msg => "mqtt_pool_size_ignored",
                        connector => Name,
                        reason =>
                            "Remote topic filter is not a shared subscription, "
                            "only a single connection will be used from the connection pool",
                        config_pool_size => PoolSize,
                        pool_size => PoolSize
                    });
                false ->
                    ok
            end,
            %% NOTE: this is regular subscription, only one worker should subscribe
            IsFirstWorker
    end.

add_reconnect_callback(PoolName, ReconnectContext0) ->
    lists:foreach(
        fun({{WorkerName, WorkerIdx}, Pid}) ->
            ReconnectContext = ReconnectContext0#{
                worker_name => WorkerName, worker_idx => WorkerIdx
            },
            ecpool_worker:add_reconnect_callback(Pid, {?MODULE, on_reconnect, [ReconnectContext]})
        end,
        ecpool:workers(PoolName)
    ).

remove_reconnect_callback(PoolName, ChannelId) ->
    lists:foreach(
        fun({_, Pid}) ->
            ok = ecpool_worker:remove_reconnect_callback_by_signature(Pid, ChannelId)
        end,
        ecpool:workers(PoolName)
    ).

%% `ecpool` callback
on_reconnect(ClientPid, ReconnectContext) ->
    #{
        chan_res_id := _ChanResId,
        worker_name := WorkerName,
        worker_idx := WorkerIdx,
        ingress_config := IngressConfig,
        pool_size := PoolSize
    } = ReconnectContext,
    Res = subscribe_channel_helper(ClientPid, WorkerName, IngressConfig, WorkerIdx, PoolSize),
    ?tp(debug, "mqtt_source_reconnected", #{chan_res_id => _ChanResId}),
    Res.

%% `ecpool` callback
get_reconnect_callback_signature([#{chan_res_id := ChanResId}] = _ReconnectContext) ->
    ChanResId.

unsubscribe_channel(
    PoolName, IngressConfig, ChannelId, SubscriptionIdToHandlerIndex, TopicToHandlerIndex
) ->
    Workers = ecpool:workers(PoolName),
    PoolSize = length(Workers),
    _ = [
        unsubscribe_channel(
            Pid,
            Name,
            IngressConfig,
            Idx,
            PoolSize,
            ChannelId,
            SubscriptionIdToHandlerIndex,
            TopicToHandlerIndex
        )
     || {{Name, Idx}, Pid} <- Workers
    ],
    ok.

unsubscribe_channel(
    WorkerPid,
    Name,
    IngressConfig,
    WorkerIdx,
    PoolSize,
    ChannelId,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    case ecpool_worker:client(WorkerPid) of
        {ok, Client} ->
            unsubscribe_channel_helper(
                Client,
                Name,
                IngressConfig,
                WorkerIdx,
                PoolSize,
                ChannelId,
                SubscriptionIdToHandlerIndex,
                TopicToHandlerIndex
            );
        {error, Reason} ->
            error({client_not_found, Reason})
    end.

unsubscribe_channel_helper(
    Client,
    Name,
    IngressConfig,
    WorkerIdx,
    PoolSize,
    ChannelId,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    IngressList = maps:get(ingress_list, IngressConfig, []),
    [
        unsubscribe_remote_topic(
            Client,
            Ingress,
            WorkerIdx,
            PoolSize,
            Name,
            ChannelId,
            SubscriptionIdToHandlerIndex,
            TopicToHandlerIndex
        )
     || Ingress <- IngressList
    ].

unsubscribe_remote_topic(
    Pid,
    #{remote := #{topic := RemoteTopic}} = Ingress,
    WorkerIdx,
    PoolSize,
    Name,
    ChannelId,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    delete_from_handler_index(
        Ingress, RemoteTopic, ChannelId, SubscriptionIdToHandlerIndex, TopicToHandlerIndex
    ),
    case should_subscribe(RemoteTopic, WorkerIdx, PoolSize, Name, false) of
        true ->
            case emqtt:unsubscribe(Pid, RemoteTopic) of
                {ok, _Properties, _ReasonCodes} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "unsubscribe_mqtt_topic_failed",
                        channel_id => Name,
                        reason => Reason
                    }),
                    ok
            end;
        false ->
            ok
    end.

config(
    #{ingress_list := IngressList} = Conf,
    Name,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    NewIngressList = [
        fix_remote_config(
            Ingress,
            Name,
            SubscriptionIdToHandlerIndex,
            TopicToHandlerIndex
        )
     || Ingress <- IngressList
    ],
    Conf#{ingress_list := NewIngressList}.

fix_remote_config(
    #{remote := RC} = Conf,
    BridgeName,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    FixedConf0 = Conf#{
        remote => parse_remote(RC, BridgeName)
    },
    FixedConf1 = maybe_attach_subscription_identifier(FixedConf0, SubscriptionIdToHandlerIndex),
    FixedConf = emqx_utils_maps:update_if_present(
        local, fun emqx_bridge_mqtt_msg:parse/1, FixedConf1
    ),
    ok = insert_to_handler_index(
        FixedConf,
        SubscriptionIdToHandlerIndex,
        TopicToHandlerIndex,
        BridgeName
    ),
    FixedConf.

maybe_attach_subscription_identifier(Conf, undefined) ->
    Conf;
maybe_attach_subscription_identifier(Conf, SubscriptionIdToHandlerIndex) ->
    Conf#{subscription_id => allocate_subscription_id(SubscriptionIdToHandlerIndex)}.

allocate_subscription_id(SubscriptionIdToHandlerIndex) ->
    allocate_subscription_id(SubscriptionIdToHandlerIndex, 1).

allocate_subscription_id(SubscriptionIdToHandlerIndex, SubscriptionId) when
    SubscriptionId =< ?MAX_SUBSCRIPTION_ID
->
    case ets:member(SubscriptionIdToHandlerIndex, SubscriptionId) of
        true ->
            allocate_subscription_id(SubscriptionIdToHandlerIndex, SubscriptionId + 1);
        false ->
            SubscriptionId
    end;
allocate_subscription_id(_SubscriptionIdToHandlerIndex, SubscriptionId) ->
    error({no_available_subscription_id, SubscriptionId}).

insert_to_handler_index(
    #{subscription_id := SubscriptionId} = Conf,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex,
    BridgeName
) ->
    true = ets:insert(SubscriptionIdToHandlerIndex, {SubscriptionId, Conf}),
    maybe_insert_to_topic_to_handler_index(Conf, TopicToHandlerIndex, BridgeName);
insert_to_handler_index(
    #{remote := #{topic := Topic}} = Conf,
    undefined,
    TopicToHandlerIndex,
    BridgeName
) ->
    IndexTopic = to_index_topic(Topic),
    true = emqx_topic_index:insert(IndexTopic, BridgeName, Conf, TopicToHandlerIndex),
    ok.

maybe_insert_to_topic_to_handler_index(
    #{remote := #{topic := <<"$queue/", _/binary>>}},
    _TopicToHandlerIndex,
    _BridgeName
) ->
    ok;
maybe_insert_to_topic_to_handler_index(
    #{remote := #{topic := Topic}} = Conf,
    TopicToHandlerIndex,
    BridgeName
) ->
    IndexTopic = to_index_topic(Topic),
    true = emqx_topic_index:insert(IndexTopic, BridgeName, Conf, TopicToHandlerIndex),
    ok.

delete_from_handler_index(
    #{subscription_id := SubscriptionId},
    RemoteTopic,
    ChannelId,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    true = ets:delete(SubscriptionIdToHandlerIndex, SubscriptionId),
    maybe_delete_from_topic_to_handler_index(RemoteTopic, ChannelId, TopicToHandlerIndex);
delete_from_handler_index(
    _Ingress,
    RemoteTopic,
    ChannelId,
    undefined,
    TopicToHandlerIndex
) ->
    IndexTopic = to_index_topic(RemoteTopic),
    true = emqx_topic_index:delete(IndexTopic, ChannelId, TopicToHandlerIndex).

maybe_delete_from_topic_to_handler_index(<<"$queue/", _/binary>>, _ChannelId, _TopicToHandlerIndex) ->
    ok;
maybe_delete_from_topic_to_handler_index(RemoteTopic, ChannelId, TopicToHandlerIndex) ->
    IndexTopic = to_index_topic(RemoteTopic),
    true = emqx_topic_index:delete(IndexTopic, ChannelId, TopicToHandlerIndex).

to_index_topic(Topic) ->
    case emqx_topic:parse(Topic) of
        {#share{group = _Group, topic = TP}, _} ->
            TP;
        _ ->
            Topic
    end.

parse_remote(#{qos := QoSIn} = Remote, BridgeName) ->
    QoS = downgrade_ingress_qos(QoSIn),
    case QoS of
        QoSIn ->
            ok;
        _ ->
            ?SLOG(warning, #{
                msg => "downgraded_unsupported_ingress_qos",
                qos_configured => QoSIn,
                qos_used => QoS,
                name => BridgeName
            })
    end,
    Remote#{qos => QoS}.

downgrade_ingress_qos(2) ->
    1;
downgrade_ingress_qos(QoS) ->
    QoS.

%%

-spec info(pid()) ->
    [{atom(), term()}].
info(Pid) ->
    emqtt:info(Pid).

-spec status(pid()) ->
    emqx_resource:resource_status().
status(Pid) ->
    try
        case proplists:get_value(socket, info(Pid)) of
            Socket when Socket /= undefined ->
                ?status_connected;
            undefined ->
                ?status_connecting
        end
    catch
        exit:{noproc, _} ->
            ?status_disconnected
    end.

%%

handle_publish(
    #{properties := Props, topic := Topic} = MsgIn,
    Name,
    SubscriptionIdToHandlerIndex,
    TopicToHandlerIndex
) ->
    ?SLOG(debug, #{
        msg => "ingress_publish_local",
        message => MsgIn,
        name => Name
    }),
    ChannelConfigs = find_channel_configs(
        Topic, Props, SubscriptionIdToHandlerIndex, TopicToHandlerIndex
    ),
    lists:foreach(
        fun(ChannelConfig) -> handle_channel_config(ChannelConfig, MsgIn, Name, Props) end,
        ChannelConfigs
    ),
    ok.

find_channel_configs(Topic, Props, SubscriptionIdToHandlerIndex, TopicToHandlerIndex) ->
    case find_channel_configs_by_subscription_identifier(Props, SubscriptionIdToHandlerIndex) of
        [] ->
            find_channel_configs_by_topic(Topic, TopicToHandlerIndex);
        ChannelConfigs ->
            ChannelConfigs
    end.

find_channel_configs_by_topic(Topic, TopicToHandlerIndex) ->
    Matches = emqx_topic_index:matches(Topic, TopicToHandlerIndex, []),
    [
        ChannelConfig
     || Match <- Matches,
        [ChannelConfig] <- [emqx_topic_index:get_record(Match, TopicToHandlerIndex)]
    ].

find_channel_configs_by_subscription_identifier(_Props, undefined) ->
    [];
find_channel_configs_by_subscription_identifier(Props, SubscriptionIdToHandlerIndex) ->
    lists:flatmap(
        fun(SubscriptionId) ->
            case ets:lookup(SubscriptionIdToHandlerIndex, SubscriptionId) of
                [{SubscriptionId, ChannelConfig}] -> [ChannelConfig];
                [] -> []
            end
        end,
        subscription_ids(Props)
    ).

subscription_ids(Props) ->
    case maps:get('Subscription-Identifier', Props, []) of
        SubscriptionId when is_integer(SubscriptionId) ->
            [SubscriptionId];
        SubscriptionIds when is_list(SubscriptionIds) ->
            SubscriptionIds;
        _ ->
            []
    end.

handle_channel_config(ChannelConfig, MsgIn, _Name, Props) ->
    #{on_message_received := OnMessage} = ChannelConfig,
    Msg = import_msg(MsgIn, ChannelConfig),
    maybe_on_message_received(Msg, OnMessage),
    LocalPublish = maps:get(local, ChannelConfig, undefined),
    _ = maybe_publish_local(Msg, LocalPublish, Props),
    ok.

maybe_on_message_received(Msg, {Mod, Func, Args}) ->
    erlang:apply(Mod, Func, [Msg | Args]);
maybe_on_message_received(_Msg, undefined) ->
    ok.

maybe_publish_local(Msg, Local = #{topic := Topic}, Props) when Topic =/= undefined ->
    ?tp(mqtt_ingress_publish_local, #{msg => Msg, local => Local}),
    emqx_broker:publish(to_broker_msg(Msg, Local, Props));
maybe_publish_local(_Msg, _Local, _Props) ->
    ok.

%%

import_msg(
    #{
        dup := Dup,
        payload := Payload,
        properties := Props,
        qos := QoS,
        retain := Retain,
        topic := Topic
    },
    #{server := Server}
) ->
    #{
        id => emqx_guid:to_hexstr(emqx_guid:gen()),
        server => to_bin(Server),
        payload => Payload,
        topic => Topic,
        qos => QoS,
        dup => Dup,
        retain => Retain,
        pub_props => emqx_utils_maps:printable_props(Props),
        message_received_at => erlang:system_time(millisecond)
    }.

%% published from remote node over a MQTT connection
to_broker_msg(Msg, Vars, undefined) ->
    to_broker_msg(Msg, Vars, #{});
to_broker_msg(#{dup := Dup} = Msg, Local, Props) ->
    #{
        topic := Topic,
        payload := Payload,
        qos := QoS,
        retain := Retain
    } = emqx_bridge_mqtt_msg:render(Msg, Local),
    PubProps = maps:get(pub_props, Msg, #{}),
    emqx_message:set_headers(
        Props#{properties => emqx_utils:pub_props_to_packet(PubProps)},
        emqx_message:set_flags(
            #{dup => Dup, retain => Retain},
            emqx_message:make(bridge, QoS, Topic, Payload)
        )
    ).

to_bin(B) when is_binary(B) -> B;
to_bin(Str) when is_list(Str) -> iolist_to_binary(Str).
