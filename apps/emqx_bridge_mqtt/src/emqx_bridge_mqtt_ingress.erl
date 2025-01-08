%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_ingress).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% management APIs
-export([
    status/1,
    info/1,
    subscribe_channel/2,
    unsubscribe_channel/4,
    config/3
]).

-export([handle_publish/3]).

subscribe_channel(PoolName, ChannelConfig) ->
    Workers = ecpool:workers(PoolName),
    PoolSize = length(Workers),
    Results = [
        subscribe_channel(Pid, Name, ChannelConfig, Idx, PoolSize)
     || {{Name, Idx}, Pid} <- Workers
    ],
    case proplists:get_value(error, Results, ok) of
        ok ->
            ok;
        Error ->
            Error
    end.

subscribe_channel(WorkerPid, Name, Ingress, WorkerIdx, PoolSize) ->
    case ecpool_worker:client(WorkerPid) of
        {ok, Client} ->
            subscribe_channel_helper(Client, Name, Ingress, WorkerIdx, PoolSize);
        {error, Reason} ->
            error({client_not_found, Reason})
    end.

subscribe_channel_helper(Client, Name, Ingress, WorkerIdx, PoolSize) ->
    IngressList = maps:get(ingress_list, Ingress, []),
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
                ingress => Ingress,
                name => Name,
                reason => Reason
            }),
            Error
    end.

subscribe_remote_topics(Pid, IngressList, WorkerIdx, PoolSize, Name) ->
    [subscribe_remote_topic(Pid, Ingress, WorkerIdx, PoolSize, Name) || Ingress <- IngressList].

subscribe_remote_topic(
    Pid,
    #{remote := #{topic := RemoteTopic, qos := QoS, no_local := NoLocal}} = _Remote,
    WorkerIdx,
    PoolSize,
    Name
) ->
    case should_subscribe(RemoteTopic, WorkerIdx, PoolSize, Name, _LogWarn = true) of
        true ->
            emqtt:subscribe(Pid, RemoteTopic, [{qos, QoS}, {nl, NoLocal}]);
        false ->
            ok
    end.

should_subscribe(RemoteTopic, WorkerIdx, PoolSize, Name, LogWarn) ->
    IsFirstWorker = WorkerIdx == 1,
    case emqx_topic:parse(RemoteTopic) of
        {#share{} = _Filter, _SubOpts} ->
            % NOTE: this is shared subscription, many workers may subscribe
            true;
        {_Filter, #{}} when PoolSize > 1, IsFirstWorker, LogWarn ->
            % NOTE: this is regular subscription, only one worker should subscribe
            ?SLOG(warning, #{
                msg => "mqtt_pool_size_ignored",
                connector => Name,
                reason =>
                    "Remote topic filter is not a shared subscription, "
                    "only a single connection will be used from the connection pool",
                config_pool_size => PoolSize,
                pool_size => PoolSize
            }),
            IsFirstWorker;
        {_Filter, #{}} ->
            % NOTE: this is regular subscription, only one worker should subscribe
            IsFirstWorker
    end.

unsubscribe_channel(PoolName, ChannelConfig, ChannelId, TopicToHandlerIndex) ->
    Workers = ecpool:workers(PoolName),
    PoolSize = length(Workers),
    _ = [
        unsubscribe_channel(Pid, Name, ChannelConfig, Idx, PoolSize, ChannelId, TopicToHandlerIndex)
     || {{Name, Idx}, Pid} <- Workers
    ],
    ok.

unsubscribe_channel(WorkerPid, Name, Ingress, WorkerIdx, PoolSize, ChannelId, TopicToHandlerIndex) ->
    case ecpool_worker:client(WorkerPid) of
        {ok, Client} ->
            unsubscribe_channel_helper(
                Client, Name, Ingress, WorkerIdx, PoolSize, ChannelId, TopicToHandlerIndex
            );
        {error, Reason} ->
            error({client_not_found, Reason})
    end.

unsubscribe_channel_helper(
    Client, Name, Ingress, WorkerIdx, PoolSize, ChannelId, TopicToHandlerIndex
) ->
    IngressList = maps:get(ingress_list, Ingress, []),
    unsubscribe_remote_topics(
        Client, IngressList, WorkerIdx, PoolSize, Name, ChannelId, TopicToHandlerIndex
    ).

unsubscribe_remote_topics(
    Pid, IngressList, WorkerIdx, PoolSize, Name, ChannelId, TopicToHandlerIndex
) ->
    [
        unsubscribe_remote_topic(
            Pid, Ingress, WorkerIdx, PoolSize, Name, ChannelId, TopicToHandlerIndex
        )
     || Ingress <- IngressList
    ].

unsubscribe_remote_topic(
    Pid,
    #{remote := #{topic := RemoteTopic}} = _Remote,
    WorkerIdx,
    PoolSize,
    Name,
    ChannelId,
    TopicToHandlerIndex
) ->
    emqx_topic_index:delete(RemoteTopic, ChannelId, TopicToHandlerIndex),
    case should_subscribe(RemoteTopic, WorkerIdx, PoolSize, Name, _NoWarn = false) of
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

config(#{ingress_list := IngressList} = Conf, Name, TopicToHandlerIndex) ->
    NewIngressList = [
        fix_remote_config(Ingress, Name, TopicToHandlerIndex, Conf)
     || Ingress <- IngressList
    ],
    Conf#{ingress_list => NewIngressList}.

fix_remote_config(#{remote := RC}, BridgeName, TopicToHandlerIndex, Conf) ->
    FixedConf0 = Conf#{
        remote => parse_remote(RC, BridgeName)
    },
    FixedConf = emqx_utils_maps:update_if_present(
        local, fun emqx_bridge_mqtt_msg:parse/1, FixedConf0
    ),
    insert_to_topic_to_handler_index(FixedConf, TopicToHandlerIndex, BridgeName),
    FixedConf.

insert_to_topic_to_handler_index(
    #{remote := #{topic := Topic}} = Conf, TopicToHandlerIndex, BridgeName
) ->
    TopicPattern =
        case emqx_topic:parse(Topic) of
            {#share{group = _Group, topic = TP}, _} ->
                TP;
            _ ->
                Topic
        end,
    emqx_topic_index:insert(TopicPattern, BridgeName, Conf, TopicToHandlerIndex).

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
    TopicToHandlerIndex
) ->
    ?SLOG(debug, #{
        msg => "ingress_publish_local",
        message => MsgIn,
        name => Name
    }),
    Matches = emqx_topic_index:matches(Topic, TopicToHandlerIndex, []),
    lists:foreach(
        fun(Match) ->
            handle_match(TopicToHandlerIndex, Match, MsgIn, Name, Props)
        end,
        Matches
    ),
    ok.

handle_match(
    TopicToHandlerIndex,
    Match,
    MsgIn,
    _Name,
    Props
) ->
    [ChannelConfig] = emqx_topic_index:get_record(Match, TopicToHandlerIndex),
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
        pub_props => printable_maps(Props),
        message_received_at => erlang:system_time(millisecond)
    }.

printable_maps(undefined) ->
    #{};
printable_maps(Headers) ->
    maps:fold(
        fun
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [
                        #{
                            key => Key,
                            value => Value
                        }
                     || {Key, Value} <- V0
                    ]
                };
            (K, V0, AccIn) ->
                AccIn#{K => V0}
        end,
        #{},
        Headers
    ).

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
