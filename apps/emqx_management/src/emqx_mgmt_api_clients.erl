%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_clients).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/logger.hrl").

-include("emqx_mgmt.hrl").

%% API
-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-export([
    clients/2,
    client/2,
    subscriptions/2,
    authz_cache/2,
    subscribe/2,
    subscribe_batch/2,
    unsubscribe/2,
    unsubscribe_batch/2,
    set_keepalive/2
]).

-export([
    query/4,
    format_channel_info/1
]).

%% for batch operation
-export([do_subscribe/3]).

-define(CLIENT_QTAB, emqx_channel_info).

-define(CLIENT_QSCHEMA, [
    {<<"node">>, atom},
    {<<"username">>, binary},
    {<<"zone">>, atom},
    {<<"ip_address">>, ip},
    {<<"conn_state">>, atom},
    {<<"clean_start">>, atom},
    {<<"proto_name">>, binary},
    {<<"proto_ver">>, integer},
    {<<"like_clientid">>, binary},
    {<<"like_username">>, binary},
    {<<"gte_created_at">>, timestamp},
    {<<"lte_created_at">>, timestamp},
    {<<"gte_connected_at">>, timestamp},
    {<<"lte_connected_at">>, timestamp}
]).

-define(QUERY_FUN, {?MODULE, query}).
-define(FORMAT_FUN, {?MODULE, format_channel_info}).

-define(CLIENT_ID_NOT_FOUND,
    <<"{\"code\": \"RESOURCE_NOT_FOUND\", \"reason\": \"Client id not found\"}">>
).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/clients",
        "/clients/:clientid",
        "/clients/:clientid/authorization/cache",
        "/clients/:clientid/subscriptions",
        "/clients/:clientid/subscribe",
        "/clients/:clientid/subscribe/bulk",
        "/clients/:clientid/unsubscribe",
        "/clients/:clientid/unsubscribe/bulk",
        "/clients/:clientid/keepalive"
    ].

schema("/clients") ->
    #{
        'operationId' => clients,
        get => #{
            description => <<"List clients">>,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit),
                {node,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"Node name">>,
                        example => atom_to_list(node())
                    })},
                {username,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"User name">>
                    })},
                {zone,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false
                    })},
                {ip_address,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"Client's IP address">>,
                        example => <<"127.0.0.1">>
                    })},
                {conn_state,
                    hoconsc:mk(hoconsc:enum([connected, idle, disconnected]), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"The current connection status of the client, ",
                                "the possible values are connected,idle,disconnected">>
                    })},
                {clean_start,
                    hoconsc:mk(boolean(), #{
                        in => query,
                        required => false,
                        description => <<"Whether the client uses a new session">>
                    })},
                {proto_name,
                    hoconsc:mk(hoconsc:enum(['MQTT', 'CoAP', 'LwM2M', 'MQTT-SN']), #{
                        in => query,
                        required => false,
                        description =>
                            <<"Client protocol name, ",
                                "the possible values are MQTT,CoAP,LwM2M,MQTT-SN">>
                    })},
                {proto_ver,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"Client protocol version">>
                    })},
                {like_clientid,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"Fuzzy search `clientid` as substring">>
                    })},
                {like_username,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"Fuzzy search `username` as substring">>
                    })},
                {gte_created_at,
                    hoconsc:mk(emqx_datetime:epoch_millisecond(), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"Search client session creation time by greater",
                                " than or equal method, rfc3339 or timestamp(millisecond)">>
                    })},
                {lte_created_at,
                    hoconsc:mk(emqx_datetime:epoch_millisecond(), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"Search client session creation time by less",
                                " than or equal method, rfc3339 or timestamp(millisecond)">>
                    })},
                {gte_connected_at,
                    hoconsc:mk(emqx_datetime:epoch_millisecond(), #{
                        in => query,
                        required => false,
                        desc => <<
                            "Search client connection creation time by greater"
                            " than or equal method, rfc3339 or timestamp(epoch millisecond)"
                        >>
                    })},
                {lte_connected_at,
                    hoconsc:mk(emqx_datetime:epoch_millisecond(), #{
                        in => query,
                        required => false,
                        desc => <<
                            "Search client connection creation time by less"
                            " than or equal method, rfc3339 or timestamp(millisecond)"
                        >>
                    })}
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, client)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ],
                400 =>
                    emqx_dashboard_swagger:error_codes(
                        ['INVALID_PARAMETER'], <<"Invalid parameters">>
                    )
            }
        }
    };
schema("/clients/:clientid") ->
    #{
        'operationId' => client,
        get => #{
            description => <<"Get clients info by client ID">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(?MODULE, client), #{}),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        },
        delete => #{
            description => <<"Kick out client by client ID">>,
            parameters => [
                {clientid, hoconsc:mk(binary(), #{in => path})}
            ],
            responses => #{
                204 => <<"Kick out client successfully">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/authorization/cache") ->
    #{
        'operationId' => authz_cache,
        get => #{
            description => <<"Get client authz cache in the cluster.">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(?MODULE, authz_cache), #{}),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        },
        delete => #{
            description => <<"Clean client authz cache in the cluster.">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                204 => <<"Kick out client successfully">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/subscriptions") ->
    #{
        'operationId' => subscriptions,
        get => #{
            description => <<"Get client subscriptions">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                200 => hoconsc:mk(
                    hoconsc:array(hoconsc:ref(emqx_mgmt_api_subscriptions, subscription)), #{}
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/subscribe") ->
    #{
        'operationId' => subscribe,
        post => #{
            description => <<"Subscribe">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, subscribe)),
            responses => #{
                200 => hoconsc:ref(emqx_mgmt_api_subscriptions, subscription),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/subscribe/bulk") ->
    #{
        'operationId' => subscribe_batch,
        post => #{
            description => <<"Subscribe">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, subscribe))),
            responses => #{
                200 => hoconsc:array(hoconsc:ref(emqx_mgmt_api_subscriptions, subscription)),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/unsubscribe") ->
    #{
        'operationId' => unsubscribe,
        post => #{
            description => <<"Unsubscribe">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, unsubscribe)),
            responses => #{
                204 => <<"Unsubscribe OK">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/unsubscribe/bulk") ->
    #{
        'operationId' => unsubscribe_batch,
        post => #{
            description => <<"Unsubscribe">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, unsubscribe))),
            responses => #{
                204 => <<"Unsubscribe OK">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    };
schema("/clients/:clientid/keepalive") ->
    #{
        'operationId' => set_keepalive,
        put => #{
            description => <<"Set the online client keepalive by seconds">>,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, keepalive)),
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(?MODULE, client), #{}),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client id not found">>
                )
            }
        }
    }.

fields(client) ->
    [
        {awaiting_rel_cnt,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [awaiting_rel] Number of awaiting PUBREC packet">>
            })},
        {awaiting_rel_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<
                        "v4 api name [max_awaiting_rel]. "
                        "Maximum allowed number of awaiting PUBREC packet"
                    >>
            })},
        {clean_start,
            hoconsc:mk(boolean(), #{
                desc =>
                    <<"Indicate whether the client is using a brand new session">>
            })},
        {clientid, hoconsc:mk(binary(), #{desc => <<"Client identifier">>})},
        {connected, hoconsc:mk(boolean(), #{desc => <<"Whether the client is connected">>})},
        {connected_at,
            hoconsc:mk(
                emqx_datetime:epoch_millisecond(),
                #{desc => <<"Client connection time, rfc3339 or timestamp(millisecond)">>}
            )},
        {created_at,
            hoconsc:mk(
                emqx_datetime:epoch_millisecond(),
                #{desc => <<"Session creation time, rfc3339 or timestamp(millisecond)">>}
            )},
        {disconnected_at,
            hoconsc:mk(emqx_datetime:epoch_millisecond(), #{
                desc =>
                    <<
                        "Client offline time."
                        " It's Only valid and returned when connected is false, rfc3339 or timestamp(millisecond)"
                    >>
            })},
        {expiry_interval,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Session expiration interval, with the unit of second">>
            })},
        {heap_size,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Process heap size with the unit of byte">>
            })},
        {inflight_cnt, hoconsc:mk(integer(), #{desc => <<"Current length of inflight">>})},
        {inflight_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [max_inflight]. Maximum length of inflight">>
            })},
        {ip_address, hoconsc:mk(binary(), #{desc => <<"Client's IP address">>})},
        {is_bridge,
            hoconsc:mk(boolean(), #{
                desc =>
                    <<"Indicates whether the client is connectedvia bridge">>
            })},
        {keepalive,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"keepalive time, with the unit of second">>
            })},
        {mailbox_len, hoconsc:mk(integer(), #{desc => <<"Process mailbox size">>})},
        {mqueue_dropped,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of messages dropped by the message queue due to exceeding the length">>
            })},
        {mqueue_len, hoconsc:mk(integer(), #{desc => <<"Current length of message queue">>})},
        {mqueue_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [max_mqueue]. Maximum length of message queue">>
            })},
        {node,
            hoconsc:mk(binary(), #{
                desc =>
                    <<"Name of the node to which the client is connected">>
            })},
        {port, hoconsc:mk(integer(), #{desc => <<"Client's port">>})},
        {proto_name, hoconsc:mk(binary(), #{desc => <<"Client protocol name">>})},
        {proto_ver, hoconsc:mk(integer(), #{desc => <<"Protocol version used by the client">>})},
        {recv_cnt, hoconsc:mk(integer(), #{desc => <<"Number of TCP packets received">>})},
        {recv_msg, hoconsc:mk(integer(), #{desc => <<"Number of PUBLISH packets received">>})},
        {'recv_msg.dropped',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets">>
            })},
        {'recv_msg.dropped.await_pubrel_timeout',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to expired">>
            })},
        {'recv_msg.qos0',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS0 packets received">>
            })},
        {'recv_msg.qos1',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS1 packets received">>
            })},
        {'recv_msg.qos2',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS2 packets received">>
            })},
        {recv_oct, hoconsc:mk(integer(), #{desc => <<"Number of bytes received">>})},
        {recv_pkt, hoconsc:mk(integer(), #{desc => <<"Number of MQTT packets received">>})},
        {reductions, hoconsc:mk(integer(), #{desc => <<"Erlang reduction">>})},
        {send_cnt, hoconsc:mk(integer(), #{desc => <<"Number of TCP packets sent">>})},
        {send_msg, hoconsc:mk(integer(), #{desc => <<"Number of PUBLISH packets sent">>})},
        {'send_msg.dropped',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets">>
            })},
        {'send_msg.dropped.expired',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to expired">>
            })},
        {'send_msg.dropped.queue_full',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to queue full">>
            })},
        {'send_msg.dropped.too_large',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to packet length too large">>
            })},
        {'send_msg.qos0',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS0 packets sent">>
            })},
        {'send_msg.qos1',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS1 packets sent">>
            })},
        {'send_msg.qos2',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS2 packets sent">>
            })},
        {send_oct, hoconsc:mk(integer(), #{desc => <<"Number of bytes sent">>})},
        {send_pkt, hoconsc:mk(integer(), #{desc => <<"Number of MQTT packets sent">>})},
        {subscriptions_cnt,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of subscriptions established by this client.">>
            })},
        {subscriptions_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [max_subscriptions]",
                        " Maximum number of subscriptions allowed by this client">>
            })},
        {username, hoconsc:mk(binary(), #{desc => <<"User name of client when connecting">>})},
        {mountpoint, hoconsc:mk(binary(), #{desc => <<"Topic mountpoint">>})},
        {will_msg, hoconsc:mk(binary(), #{desc => <<"Client will message">>})},
        {zone,
            hoconsc:mk(binary(), #{
                desc =>
                    <<"Indicate the configuration group used by the client">>
            })}
    ];
fields(authz_cache) ->
    [
        {access, hoconsc:mk(binary(), #{desc => <<"Access type">>})},
        {result, hoconsc:mk(binary(), #{desc => <<"Allow or deny">>})},
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic name">>})},
        {updated_time, hoconsc:mk(integer(), #{desc => <<"Update time">>})}
    ];
fields(keepalive) ->
    [
        {interval, hoconsc:mk(integer(), #{desc => <<"Keepalive time, with the unit of second">>})}
    ];
fields(subscribe) ->
    [
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic">>})},
        {qos, hoconsc:mk(emqx_schema:qos(), #{desc => <<"QoS">>})},
        {nl, hoconsc:mk(integer(), #{default => 0, desc => <<"No Local">>})},
        {rap, hoconsc:mk(integer(), #{default => 0, desc => <<"Retain as Published">>})},
        {rh, hoconsc:mk(integer(), #{default => 0, desc => <<"Retain Handling">>})}
    ];
fields(unsubscribe) ->
    [
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic">>})}
    ].

%%%==============================================================================================
%% parameters trans
clients(get, #{query_string := QString}) ->
    list_clients(QString).

client(get, #{bindings := Bindings}) ->
    lookup(Bindings);
client(delete, #{bindings := Bindings}) ->
    kickout(Bindings).

authz_cache(get, #{bindings := Bindings}) ->
    get_authz_cache(Bindings);
authz_cache(delete, #{bindings := Bindings}) ->
    clean_authz_cache(Bindings).

subscribe(post, #{bindings := #{clientid := ClientID}, body := TopicInfo}) ->
    Opts = emqx_map_lib:unsafe_atom_key_map(TopicInfo),
    subscribe(Opts#{clientid => ClientID}).

subscribe_batch(post, #{bindings := #{clientid := ClientID}, body := TopicInfos}) ->
    Topics =
        [
            emqx_map_lib:unsafe_atom_key_map(TopicInfo)
         || TopicInfo <- TopicInfos
        ],
    subscribe_batch(#{clientid => ClientID, topics => Topics}).

unsubscribe(post, #{bindings := #{clientid := ClientID}, body := TopicInfo}) ->
    Topic = maps:get(<<"topic">>, TopicInfo),
    unsubscribe(#{clientid => ClientID, topic => Topic}).

unsubscribe_batch(post, #{bindings := #{clientid := ClientID}, body := TopicInfos}) ->
    Topics = [Topic || #{<<"topic">> := Topic} <- TopicInfos],
    unsubscribe_batch(#{clientid => ClientID, topics => Topics}).

subscriptions(get, #{bindings := #{clientid := ClientID}}) ->
    case emqx_mgmt:list_client_subscriptions(ClientID) of
        [] ->
            {200, []};
        {Node, Subs} ->
            Formatter =
                fun({Topic, SubOpts}) ->
                    maps:merge(
                        #{
                            node => Node,
                            clientid => ClientID,
                            topic => Topic
                        },
                        maps:with([qos, nl, rap, rh], SubOpts)
                    )
                end,
            {200, lists:map(Formatter, Subs)}
    end.

set_keepalive(put, #{bindings := #{clientid := ClientID}, body := Body}) ->
    case maps:find(<<"interval">>, Body) of
        error ->
            {400, 'BAD_REQUEST', "Interval Not Found"};
        {ok, Interval} ->
            case emqx_mgmt:set_keepalive(emqx_mgmt_util:urldecode(ClientID), Interval) of
                ok -> lookup(#{clientid => ClientID});
                {error, not_found} -> {404, ?CLIENT_ID_NOT_FOUND};
                {error, Reason} -> {400, #{code => 'PARAMS_ERROR', message => Reason}}
            end
    end.

%%%==============================================================================================
%% api apply

list_clients(QString) ->
    Result =
        case maps:get(<<"node">>, QString, undefined) of
            undefined ->
                emqx_mgmt_api:cluster_query(
                    QString,
                    ?CLIENT_QTAB,
                    ?CLIENT_QSCHEMA,
                    ?QUERY_FUN
                );
            Node0 ->
                Node1 = binary_to_atom(Node0, utf8),
                QStringWithoutNode = maps:without([<<"node">>], QString),
                emqx_mgmt_api:node_query(
                    Node1,
                    QStringWithoutNode,
                    ?CLIENT_QTAB,
                    ?CLIENT_QSCHEMA,
                    ?QUERY_FUN
                )
        end,
    case Result of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Response ->
            {200, Response}
    end.

lookup(#{clientid := ClientID}) ->
    case emqx_mgmt:lookup_client({clientid, ClientID}, ?FORMAT_FUN) of
        [] ->
            {404, ?CLIENT_ID_NOT_FOUND};
        ClientInfo ->
            {200, hd(ClientInfo)}
    end.

kickout(#{clientid := ClientID}) ->
    case emqx_mgmt:kickout_client({ClientID, ?FORMAT_FUN}) of
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        _ ->
            {204}
    end.

get_authz_cache(#{clientid := ClientID}) ->
    case emqx_mgmt:list_authz_cache(ClientID) of
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOW_ERROR">>, message => Message}};
        Caches ->
            Response = [format_authz_cache(Cache) || Cache <- Caches],
            {200, Response}
    end.

clean_authz_cache(#{clientid := ClientID}) ->
    case emqx_mgmt:clean_authz_cache(ClientID) of
        ok ->
            {204};
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOW_ERROR">>, message => Message}}
    end.

subscribe(#{clientid := ClientID, topic := Topic} = Sub) ->
    Opts = maps:with([qos, nl, rap, rh], Sub),
    case do_subscribe(ClientID, Topic, Opts) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOW_ERROR">>, message => Message}};
        {ok, SubInfo} ->
            {200, SubInfo}
    end.

subscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    case lookup(#{clientid => ClientID}) of
        {200, _} ->
            ArgList = [
                [ClientID, Topic, maps:with([qos, nl, rap, rh], Sub)]
             || #{topic := Topic} = Sub <- Topics
            ],
            {200, emqx_mgmt_util:batch_operation(?MODULE, do_subscribe, ArgList)};
        {404, ?CLIENT_ID_NOT_FOUND} ->
            {404, ?CLIENT_ID_NOT_FOUND}
    end.

unsubscribe(#{clientid := ClientID, topic := Topic}) ->
    case do_unsubscribe(ClientID, Topic) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {unsubscribe, [{Topic, #{}}]} ->
            {204}
    end.

unsubscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    case lookup(#{clientid => ClientID}) of
        {200, _} ->
            _ = emqx_mgmt:unsubscribe_batch(ClientID, Topics),
            {204};
        {404, ?CLIENT_ID_NOT_FOUND} ->
            {404, ?CLIENT_ID_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% internal function

do_subscribe(ClientID, Topic0, Options) ->
    {Topic, Opts} = emqx_topic:parse(Topic0, Options),
    TopicTable = [{Topic, Opts}],
    case emqx_mgmt:subscribe(ClientID, TopicTable) of
        {error, Reason} ->
            {error, Reason};
        {subscribe, Subscriptions, Node} ->
            case proplists:is_defined(Topic, Subscriptions) of
                true ->
                    {ok, Options#{node => Node, clientid => ClientID, topic => Topic}};
                false ->
                    {error, unknow_error}
            end
    end.

-spec do_unsubscribe(emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, channel_not_found}.
do_unsubscribe(ClientID, Topic) ->
    case emqx_mgmt:unsubscribe(ClientID, Topic) of
        {error, Reason} ->
            {error, Reason};
        Res ->
            Res
    end.

%%--------------------------------------------------------------------
%% Query Functions

query(Tab, {QString, []}, Continuation, Limit) ->
    Ms = qs2ms(QString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format_channel_info/1
    );
query(Tab, {QString, FuzzyQString}, Continuation, Limit) ->
    Ms = qs2ms(QString),
    FuzzyFilterFun = fuzzy_filter_fun(FuzzyQString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format_channel_info/1
    ).

%%--------------------------------------------------------------------
%% QueryString to Match Spec

-spec qs2ms(list()) -> ets:match_spec().
qs2ms(Qs) ->
    {MtchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{'$1', MtchHead, '_'}, Conds, ['$_']}].

qs2ms([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};
qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) ->
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Value)),
    qs2ms(Rest, N, {NMtchHead, Conds});
qs2ms([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = binary_to_atom(iolist_to_binary(["$", integer_to_list(N)]), utf8),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    qs2ms(Rest, N + 1, {NMtchHead, NConds}).

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds];
put_conds({_, Op1, V1, Op2, V2}, Holder, Conds) ->
    [
        {Op2, Holder, V2},
        {Op1, Holder, V1}
        | Conds
    ].

ms(clientid, X) ->
    #{clientinfo => #{clientid => X}};
ms(username, X) ->
    #{clientinfo => #{username => X}};
ms(zone, X) ->
    #{clientinfo => #{zone => X}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(ip_address, X) ->
    #{conninfo => #{peername => {X, '_'}}};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_name, X) ->
    #{conninfo => #{proto_name => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}}.

%%--------------------------------------------------------------------
%% Match funcs

fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, like, SubStr} | Fuzzy]) ->
    Val =
        case maps:get(Key, ClientInfo, <<>>) of
            undefined -> <<>>;
            V -> V
        end,
    binary:match(Val, SubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%--------------------------------------------------------------------
%% format funcs

format_channel_info({_, ClientInfo0, ClientStats}) ->
    Node =
        case ClientInfo0 of
            #{node := N} -> N;
            _ -> node()
        end,
    ClientInfo1 = emqx_map_lib:deep_remove([conninfo, clientid], ClientInfo0),
    ClientInfo2 = emqx_map_lib:deep_remove([conninfo, username], ClientInfo1),
    StatsMap = maps:without(
        [memory, next_pkt_id, total_heap_size],
        maps:from_list(ClientStats)
    ),
    ClientInfoMap0 = maps:fold(fun take_maps_from_inner/3, #{}, ClientInfo2),
    {IpAddress, Port} = peername_dispart(maps:get(peername, ClientInfoMap0)),
    Connected = maps:get(conn_state, ClientInfoMap0) =:= connected,
    ClientInfoMap1 = maps:merge(StatsMap, ClientInfoMap0),
    ClientInfoMap2 = maps:put(node, Node, ClientInfoMap1),
    ClientInfoMap3 = maps:put(ip_address, IpAddress, ClientInfoMap2),
    ClientInfoMap4 = maps:put(port, Port, ClientInfoMap3),
    ClientInfoMap = maps:put(connected, Connected, ClientInfoMap4),

    RemoveList =
        [
            auth_result,
            peername,
            sockname,
            peerhost,
            conn_state,
            send_pend,
            conn_props,
            peercert,
            sockstate,
            subscriptions,
            receive_maximum,
            protocol,
            is_superuser,
            sockport,
            anonymous,
            socktype,
            active_n,
            await_rel_timeout,
            conn_mod,
            sockname,
            retry_interval,
            upgrade_qos,
            %% sessionID, defined in emqx_session.erl
            id
        ],
    TimesKeys = [created_at, connected_at, disconnected_at],
    %% format timestamp to rfc3339
    result_format_undefined_to_null(
        lists:foldl(
            fun result_format_time_fun/2,
            maps:without(RemoveList, ClientInfoMap),
            TimesKeys
        )
    ).

%% format func helpers
take_maps_from_inner(_Key, Value, Current) when is_map(Value) ->
    maps:merge(Current, Value);
take_maps_from_inner(Key, Value, Current) ->
    maps:put(Key, Value, Current).

result_format_time_fun(Key, NClientInfoMap) ->
    case NClientInfoMap of
        #{Key := TimeStamp} ->
            NClientInfoMap#{
                Key => emqx_datetime:epoch_to_rfc3339(TimeStamp)
            };
        #{} ->
            NClientInfoMap
    end.

result_format_undefined_to_null(Map) ->
    maps:map(
        fun
            (_, undefined) -> null;
            (_, V) -> V
        end,
        Map
    ).

-spec peername_dispart(emqx_types:peername()) -> {binary(), inet:port_number()}.
peername_dispart({Addr, Port}) ->
    AddrBinary = list_to_binary(inet:ntoa(Addr)),
    %% PortBinary = integer_to_binary(Port),
    {AddrBinary, Port}.

format_authz_cache({{PubSub, Topic}, {AuthzResult, Timestamp}}) ->
    #{
        access => PubSub,
        topic => Topic,
        result => AuthzResult,
        updated_time => Timestamp
    }.
