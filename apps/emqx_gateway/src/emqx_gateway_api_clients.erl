%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_api_clients).

-include("emqx_gateway_http.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(minirest_api).

-import(hoconsc, [mk/2, ref/1, ref/2]).

-import(
    emqx_gateway_http,
    [
        return_http_error/2,
        with_gateway/2
    ]
).

%% minirest/dashbaord_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    roots/0,
    fields/1
]).

%% http handlers
-export([
    clients/2,
    clients_insta/2,
    subscriptions/2
]).

%% internal exports (for client query)
-export([
    query/4,
    format_channel_info/1
]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/gateway/:name/clients",
        "/gateway/:name/clients/:clientid",
        "/gateway/:name/clients/:clientid/subscriptions",
        "/gateway/:name/clients/:clientid/subscriptions/:topic"
    ].

-define(CLIENT_QSCHEMA, [
    {<<"node">>, atom},
    {<<"clientid">>, binary},
    {<<"username">>, binary},
    {<<"ip_address">>, ip},
    {<<"conn_state">>, atom},
    {<<"clean_start">>, atom},
    {<<"proto_ver">>, binary},
    {<<"like_clientid">>, binary},
    {<<"like_username">>, binary},
    {<<"gte_created_at">>, timestamp},
    {<<"lte_created_at">>, timestamp},
    {<<"gte_connected_at">>, timestamp},
    {<<"lte_connected_at">>, timestamp},
    %% special keys for lwm2m protocol
    {<<"endpoint_name">>, binary},
    {<<"like_endpoint_name">>, binary},
    {<<"gte_lifetime">>, integer},
    {<<"lte_lifetime">>, integer}
]).

-define(QUERY_FUN, {?MODULE, query}).

clients(get, #{
    bindings := #{name := Name0},
    query_string := QString
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        TabName = emqx_gateway_cm:tabname(info, GwName),
        case maps:get(<<"node">>, QString, undefined) of
            undefined ->
                Response = emqx_mgmt_api:cluster_query(
                    QString,
                    TabName,
                    ?CLIENT_QSCHEMA,
                    ?QUERY_FUN
                ),
                emqx_mgmt_util:generate_response(Response);
            Node1 ->
                Node = binary_to_atom(Node1, utf8),
                QStringWithoutNode = maps:without([<<"node">>], QString),
                Response = emqx_mgmt_api:node_query(
                    Node,
                    QStringWithoutNode,
                    TabName,
                    ?CLIENT_QSCHEMA,
                    ?QUERY_FUN
                ),
                emqx_mgmt_util:generate_response(Response)
        end
    end).

clients_insta(get, #{
    bindings := #{
        name := Name0,
        clientid := ClientId0
    }
}) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        case
            emqx_gateway_http:lookup_client(
                GwName,
                ClientId,
                {?MODULE, format_channel_info}
            )
        of
            [ClientInfo] ->
                {200, ClientInfo};
            [ClientInfo | _More] ->
                ?SLOG(warning, #{
                    msg => "more_than_one_channel_found",
                    clientid => ClientId
                }),
                {200, ClientInfo};
            [] ->
                return_http_error(404, "Client not found")
        end
    end);
clients_insta(delete, #{
    bindings := #{
        name := Name0,
        clientid := ClientId0
    }
}) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        _ = emqx_gateway_http:kickout_client(GwName, ClientId),
        {204}
    end).

%% FIXME:
%% List the subscription without mountpoint, but has SubOpts,
%% for example, share group ...
subscriptions(get, #{
    bindings := #{
        name := Name0,
        clientid := ClientId0
    }
}) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        case emqx_gateway_http:list_client_subscriptions(GwName, ClientId) of
            {error, Reason} ->
                return_http_error(500, Reason);
            {ok, Subs} ->
                {200, Subs}
        end
    end);
%% Create the subscription without mountpoint
subscriptions(post, #{
    bindings := #{
        name := Name0,
        clientid := ClientId0
    },
    body := Body
}) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        case {maps:get(<<"topic">>, Body, undefined), subopts(Body)} of
            {undefined, _} ->
                return_http_error(400, "Miss topic property");
            {Topic, SubOpts} ->
                case
                    emqx_gateway_http:client_subscribe(
                        GwName, ClientId, Topic, SubOpts
                    )
                of
                    {error, Reason} ->
                        return_http_error(404, Reason);
                    {ok, {NTopic, NSubOpts}} ->
                        {201, maps:merge(NSubOpts, #{topic => NTopic})}
                end
        end
    end);
%% Remove the subscription without mountpoint
subscriptions(delete, #{
    bindings := #{
        name := Name0,
        clientid := ClientId0,
        topic := Topic0
    }
}) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    Topic = emqx_mgmt_util:urldecode(Topic0),
    with_gateway(Name0, fun(GwName, _) ->
        _ = emqx_gateway_http:client_unsubscribe(GwName, ClientId, Topic),
        {204}
    end).

%%--------------------------------------------------------------------
%% Utils

subopts(Req) ->
    SubOpts = #{
        qos => maps:get(<<"qos">>, Req, 0),
        rap => maps:get(<<"rap">>, Req, 0),
        nl => maps:get(<<"nl">>, Req, 0),
        rh => maps:get(<<"rh">>, Req, 1)
    },
    SubProps = extra_sub_props(maps:get(<<"sub_props">>, Req, #{})),
    case maps:size(SubProps) of
        0 -> SubOpts;
        _ -> maps:put(sub_props, SubProps, SubOpts)
    end.

extra_sub_props(Props) ->
    maps:filter(
        fun(_, V) -> V =/= undefined end,
        #{subid => maps:get(<<"subid">>, Props, undefined)}
    ).

%%--------------------------------------------------------------------
%% query funcs

query(Tab, {Qs, []}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format_channel_info/1
    );
query(Tab, {Qs, Fuzzy}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    FuzzyFilterFun = fuzzy_filter_fun(Fuzzy),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format_channel_info/1
    ).

qs2ms(Qs) ->
    {MtchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{'$1', MtchHead, '_'}, Conds, ['$_']}].

qs2ms([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};
qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) ->
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Value)),
    qs2ms(Rest, N, {NMtchHead, Conds});
qs2ms([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = binary_to_atom(
        iolist_to_binary(["$", integer_to_list(N)]), utf8
    ),
    NMtchHead = emqx_mgmt_util:merge_maps(
        MtchHead, ms(element(1, Qs), Holder)
    ),
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
ms(ip_address, X) ->
    #{clientinfo => #{peerhost => X}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}};
%% lwm2m fields
ms(endpoint_name, X) ->
    #{clientinfo => #{endpoint_name => X}};
ms(lifetime, X) ->
    #{clientinfo => #{lifetime => X}}.

%%--------------------------------------------------------------------
%% Fuzzy filter funcs

fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = {_, #{clientinfo := ClientInfo}, _},
    [{Key, like, SubStr} | Fuzzy]
) ->
    Val =
        case maps:get(Key, ClientInfo, <<>>) of
            undefined -> <<>>;
            V -> V
        end,
    binary:match(Val, SubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%--------------------------------------------------------------------
%% format funcs

format_channel_info({_, Infos, Stats} = R) ->
    Node = maps:get(node, Infos, node()),
    ClientInfo = maps:get(clientinfo, Infos, #{}),
    ConnInfo = maps:get(conninfo, Infos, #{}),
    SessInfo = maps:get(session, Infos, #{}),
    FetchX = [
        {node, ClientInfo, Node},
        {clientid, ClientInfo},
        {username, ClientInfo},
        {proto_name, ConnInfo},
        {proto_ver, ConnInfo},
        {ip_address, {peername, ConnInfo, fun peer_to_binary_addr/1}},
        {port, {peername, ConnInfo, fun peer_to_port/1}},
        {is_bridge, ClientInfo, false},
        {connected_at, {connected_at, ConnInfo, fun emqx_gateway_utils:unix_ts_to_rfc3339/1}},
        {disconnected_at, {disconnected_at, ConnInfo, fun emqx_gateway_utils:unix_ts_to_rfc3339/1}},
        {connected, {conn_state, Infos, fun conn_state_to_connected/1}},
        {keepalive, ClientInfo, 0},
        {clean_start, ConnInfo, true},
        {expiry_interval, ConnInfo, 0},
        {created_at, {created_at, SessInfo, fun emqx_gateway_utils:unix_ts_to_rfc3339/1}},
        {subscriptions_cnt, Stats, 0},
        {subscriptions_max, Stats, infinity},
        {inflight_cnt, Stats, 0},
        {inflight_max, Stats, infinity},
        {mqueue_len, Stats, 0},
        {mqueue_max, Stats, infinity},
        {mqueue_dropped, Stats, 0},
        {awaiting_rel_cnt, Stats, 0},
        {awaiting_rel_max, Stats, infinity},
        {recv_oct, Stats, 0},
        {recv_cnt, Stats, 0},
        {recv_pkt, Stats, 0},
        {recv_msg, Stats, 0},
        {send_oct, Stats, 0},
        {send_cnt, Stats, 0},
        {send_pkt, Stats, 0},
        {send_msg, Stats, 0},
        {mailbox_len, Stats, 0},
        {heap_size, Stats, 0},
        {reductions, Stats, 0}
    ],
    eval(FetchX ++ extra_feilds(R)).

extra_feilds({_, Infos, _Stats} = R) ->
    extra_feilds(
        maps:get(protocol, maps:get(clientinfo, Infos)),
        R
    ).

extra_feilds(lwm2m, {_, Infos, _Stats}) ->
    ClientInfo = maps:get(clientinfo, Infos, #{}),
    [
        {endpoint_name, ClientInfo},
        {lifetime, ClientInfo}
    ];
extra_feilds(_, _) ->
    [].

eval(Ls) ->
    eval(Ls, #{}).
eval([], AccMap) ->
    AccMap;
eval([{K, Vx} | More], AccMap) ->
    case valuex_get(K, Vx) of
        undefined -> eval(More, AccMap#{K => null});
        Value -> eval(More, AccMap#{K => Value})
    end;
eval([{K, Vx, Default} | More], AccMap) ->
    case valuex_get(K, Vx) of
        undefined -> eval(More, AccMap#{K => Default});
        Value -> eval(More, AccMap#{K => Value})
    end.

valuex_get(K, Vx) when is_map(Vx); is_list(Vx) ->
    key_get(K, Vx);
valuex_get(_K, {InKey, Obj}) when is_map(Obj); is_list(Obj) ->
    key_get(InKey, Obj);
valuex_get(_K, {InKey, Obj, MappingFun}) when is_map(Obj); is_list(Obj) ->
    case key_get(InKey, Obj) of
        undefined -> undefined;
        Val -> MappingFun(Val)
    end.

key_get(K, M) when is_map(M) ->
    maps:get(K, M, undefined);
key_get(K, L) when is_list(L) ->
    proplists:get_value(K, L).

-spec peer_to_binary_addr(emqx_types:peername()) -> binary().
peer_to_binary_addr({Addr, _}) ->
    list_to_binary(inet:ntoa(Addr)).

-spec peer_to_port(emqx_types:peername()) -> inet:port_number().
peer_to_port({_, Port}) ->
    Port.

conn_state_to_connected(connected) -> true;
conn_state_to_connected(_) -> false.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateway/:name/clients") ->
    #{
        'operationId' => clients,
        get =>
            #{
                desc => <<"Get the gateway client list">>,
                parameters => params_client_query(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_client_list()})
            }
    };
schema("/gateway/:name/clients/:clientid") ->
    #{
        'operationId' => clients_insta,
        get =>
            #{
                desc => <<"Get the gateway client information">>,
                parameters => params_client_insta(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_client()})
            },
        delete =>
            #{
                desc => <<"Kick out the gateway client">>,
                parameters => params_client_insta(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Kicked">>})
            }
    };
schema("/gateway/:name/clients/:clientid/subscriptions") ->
    #{
        'operationId' => subscriptions,
        get =>
            #{
                desc => <<"Get the gateway client subscriptions">>,
                parameters => params_client_insta(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_examples(
                                hoconsc:array(ref(subscription)),
                                examples_subsctiption_list()
                            )
                        }
                    )
            },
        post =>
            #{
                desc => <<"Create a subscription membership">>,
                parameters => params_client_insta(),
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    ref(subscription),
                    examples_subsctiption()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            201 => emqx_dashboard_swagger:schema_with_examples(
                                ref(subscription),
                                examples_subsctiption()
                            )
                        }
                    )
            }
    };
schema("/gateway/:name/clients/:clientid/subscriptions/:topic") ->
    #{
        'operationId' => subscriptions,
        delete =>
            #{
                desc => <<"Delete a subscriptions membership">>,
                parameters => params_topic_name_in_path() ++ params_client_insta(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Unsubscribed">>})
            }
    }.

params_client_query() ->
    params_gateway_name_in_path() ++
        params_client_searching_in_qs() ++
        params_paging().

params_client_insta() ->
    params_clientid_in_path() ++
        params_gateway_name_in_path().

params_client_searching_in_qs() ->
    M = #{in => query, required => false, example => <<"">>},
    [
        {node,
            mk(
                binary(),
                M#{desc => <<"Match the client's node name">>}
            )},
        {clientid,
            mk(
                binary(),
                M#{desc => <<"Match the client's ID">>}
            )},
        {username,
            mk(
                binary(),
                M#{desc => <<"Match the client's Username">>}
            )},
        {ip_address,
            mk(
                binary(),
                M#{desc => <<"Match the client's ip address">>}
            )},
        {conn_state,
            mk(
                binary(),
                M#{desc => <<"Match the client's connection state">>}
            )},
        {proto_ver,
            mk(
                binary(),
                M#{desc => <<"Match the client's protocol version">>}
            )},
        {clean_start,
            mk(
                boolean(),
                M#{desc => <<"Match the client's clean start flag">>}
            )},
        {like_clientid,
            mk(
                binary(),
                M#{desc => <<"Use sub-string to match client's ID">>}
            )},
        {like_username,
            mk(
                binary(),
                M#{desc => <<"Use sub-string to match client's username">>}
            )},
        {gte_created_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                M#{
                    desc => <<
                        "Match the session created datetime greater than "
                        "a certain value"
                    >>
                }
            )},
        {lte_created_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                M#{
                    desc => <<
                        "Match the session created datetime less than "
                        "a certain value"
                    >>
                }
            )},
        {gte_connected_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                M#{
                    desc => <<
                        "Match the client socket connected datetime greater "
                        "than a certain value"
                    >>
                }
            )},
        {lte_connected_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                M#{
                    desc => <<
                        "Match the client socket connected datatime less than "
                        "a certain value"
                    >>
                }
            )},
        {endpoint_name,
            mk(
                binary(),
                M#{desc => <<"Match the lwm2m client's endpoint name">>}
            )},
        {like_endpoint_name,
            mk(
                binary(),
                M#{desc => <<"Use sub-string to match lwm2m client's endpoint name">>}
            )},
        {gte_lifetime,
            mk(
                binary(),
                M#{
                    desc => <<
                        "Match the lwm2m client registered lifetime greater "
                        "than a certain value"
                    >>
                }
            )},
        {lte_lifetime,
            mk(
                binary(),
                M#{
                    desc => <<
                        "Match the lwm2m client registered lifetime less than "
                        "a certain value"
                    >>
                }
            )}
    ].

params_paging() ->
    [
        {page,
            mk(
                integer(),
                #{
                    in => query,
                    required => false,
                    desc => <<"Page Index">>,
                    example => 1
                }
            )},
        {limit,
            mk(
                integer(),
                #{
                    in => query,
                    desc => <<"Page Limit">>,
                    required => false,
                    example => 100
                }
            )}
    ].

params_gateway_name_in_path() ->
    [
        {name,
            mk(
                binary(),
                #{
                    in => path,
                    desc => <<"Gateway Name">>
                }
            )}
    ].

params_clientid_in_path() ->
    [
        {clientid,
            mk(
                binary(),
                #{
                    in => path,
                    desc => <<"Client ID">>
                }
            )}
    ].

params_topic_name_in_path() ->
    [
        {topic,
            mk(
                binary(),
                #{
                    in => path,
                    desc => <<"Topic Filter/Name">>
                }
            )}
    ].

%%--------------------------------------------------------------------
%% schemas

schema_client_list() ->
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:union([
            hoconsc:array(ref(?MODULE, stomp_client)),
            hoconsc:array(ref(?MODULE, mqttsn_client)),
            hoconsc:array(ref(?MODULE, coap_client)),
            hoconsc:array(ref(?MODULE, lwm2m_client)),
            hoconsc:array(ref(?MODULE, exproto_client))
        ]),
        examples_client_list()
    ).

schema_client() ->
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:union([
            ref(?MODULE, stomp_client),
            ref(?MODULE, mqttsn_client),
            ref(?MODULE, coap_client),
            ref(?MODULE, lwm2m_client),
            ref(?MODULE, exproto_client)
        ]),
        examples_client()
    ).

roots() ->
    [
        stomp_client,
        mqttsn_client,
        coap_client,
        lwm2m_client,
        exproto_client,
        subscription
    ].

fields(stomp_client) ->
    common_client_props();
fields(mqttsn_client) ->
    common_client_props();
fields(coap_client) ->
    common_client_props();
fields(lwm2m_client) ->
    [
        {endpoint_name,
            mk(
                binary(),
                #{desc => <<"The LwM2M client endpoint name">>}
            )},
        {lifetime,
            mk(
                integer(),
                #{desc => <<"Life time">>}
            )}
    ] ++ common_client_props();
fields(exproto_client) ->
    common_client_props();
fields(subscription) ->
    [
        {topic,
            mk(
                binary(),
                #{desc => <<"Topic Fillter">>}
            )},
        {qos,
            mk(
                integer(),
                #{desc => <<"QoS level, enum: 0, 1, 2">>}
            )},
        {nl,
            %% FIXME: why not boolean?
            mk(
                integer(),
                #{desc => <<"No Local option, enum: 0, 1">>}
            )},
        {rap,
            mk(
                integer(),
                #{desc => <<"Retain as Published option, enum: 0, 1">>}
            )},
        {rh,
            mk(
                integer(),
                #{desc => <<"Retain Handling option, enum: 0, 1, 2">>}
            )},
        {sub_props,
            mk(
                ref(extra_sub_props),
                #{desc => <<"Subscription properties">>}
            )}
    ];
fields(extra_sub_props) ->
    [
        {subid,
            mk(
                binary(),
                #{
                    desc => <<
                        "Only stomp protocol, a unique identity for "
                        "the subscription. range: 1-65535."
                    >>
                }
            )}
    ].

common_client_props() ->
    [
        {node,
            mk(
                binary(),
                #{
                    desc => <<
                        "Name of the node to which the client is "
                        "connected"
                    >>
                }
            )},
        {clientid,
            mk(
                binary(),
                #{desc => <<"Client identifier">>}
            )},
        {username,
            mk(
                binary(),
                #{desc => <<"Username of client when connecting">>}
            )},
        {proto_name,
            mk(
                binary(),
                #{desc => <<"Client protocol name">>}
            )},
        {proto_ver,
            mk(
                binary(),
                #{desc => <<"Protocol version used by the client">>}
            )},
        {ip_address,
            mk(
                binary(),
                #{desc => <<"Client's IP address">>}
            )},
        {port,
            mk(
                integer(),
                #{desc => <<"Client's port">>}
            )},
        {is_bridge,
            mk(
                boolean(),
                #{
                    desc => <<
                        "Indicates whether the client is connected via "
                        "bridge"
                    >>
                }
            )},
        {connected_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                #{desc => <<"Client connection time">>}
            )},
        {disconnected_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                #{
                    desc => <<
                        "Client offline time, This field is only valid and "
                        "returned when connected is false"
                    >>
                }
            )},
        {connected,
            mk(
                boolean(),
                #{desc => <<"Whether the client is connected">>}
            )},
        %% FIXME: the will_msg attribute is not a general attribute
        %% for every protocol. But it should be returned to frontend if someone
        %% want it
        %%
        %, {will_msg,
        %   mk(binary(),
        %      #{ desc => <<"Client will message">>})}
        {keepalive,
            mk(
                integer(),
                #{desc => <<"keepalive time, with the unit of second">>}
            )},
        {clean_start,
            mk(
                boolean(),
                #{
                    desc => <<
                        "Indicate whether the client is using a brand "
                        "new session"
                    >>
                }
            )},
        {expiry_interval,
            mk(
                integer(),
                #{
                    desc => <<
                        "Session expiration interval, with the unit of "
                        "second"
                    >>
                }
            )},
        {created_at,
            mk(
                emqx_datetime:epoch_millisecond(),
                #{desc => <<"Session creation time">>}
            )},
        {subscriptions_cnt,
            mk(
                integer(),
                #{
                    desc => <<
                        "Number of subscriptions established by this "
                        "client"
                    >>
                }
            )},
        {subscriptions_max,
            mk(
                integer(),
                #{
                    desc => <<
                        "Maximum number of subscriptions allowed by this "
                        "client"
                    >>
                }
            )},
        {inflight_cnt,
            mk(
                integer(),
                #{desc => <<"Current length of inflight">>}
            )},
        {inflight_max,
            mk(
                integer(),
                #{desc => <<"Maximum length of inflight">>}
            )},
        {mqueue_len,
            mk(
                integer(),
                #{desc => <<"Current length of message queue">>}
            )},
        {mqueue_max,
            mk(
                integer(),
                #{desc => <<"Maximum length of message queue">>}
            )},
        {mqueue_dropped,
            mk(
                integer(),
                #{
                    desc => <<
                        "Number of messages dropped by the message queue "
                        "due to exceeding the length"
                    >>
                }
            )},
        {awaiting_rel_cnt,
            mk(
                integer(),
                %% FIXME: PUBREC ??
                #{desc => <<"Number of awaiting acknowledge packet">>}
            )},
        {awaiting_rel_max,
            mk(
                integer(),
                #{
                    desc => <<
                        "Maximum allowed number of awaiting PUBREC "
                        "packet"
                    >>
                }
            )},
        {recv_oct,
            mk(
                integer(),
                #{desc => <<"Number of bytes received">>}
            )},
        {recv_cnt,
            mk(
                integer(),
                #{desc => <<"Number of socket packets received">>}
            )},
        {recv_pkt,
            mk(
                integer(),
                #{desc => <<"Number of protocol packets received">>}
            )},
        {recv_msg,
            mk(
                integer(),
                #{desc => <<"Number of message packets received">>}
            )},
        {send_oct,
            mk(
                integer(),
                #{desc => <<"Number of bytes sent">>}
            )},
        {send_cnt,
            mk(
                integer(),
                #{desc => <<"Number of socket packets sent">>}
            )},
        {send_pkt,
            mk(
                integer(),
                #{desc => <<"Number of protocol packets sent">>}
            )},
        {send_msg,
            mk(
                integer(),
                #{desc => <<"Number of message packets sent">>}
            )},
        {mailbox_len,
            mk(
                integer(),
                #{desc => <<"Process mailbox size">>}
            )},
        {heap_size,
            mk(
                integer(),
                #{desc => <<"Process heap size with the unit of byte">>}
            )},
        {reductions,
            mk(
                integer(),
                #{desc => <<"Erlang reduction">>}
            )}
    ].

%%--------------------------------------------------------------------
%% examples

examples_client_list() ->
    #{
        general_client_list =>
            #{
                summary => <<"General Client List">>,
                value => [example_general_client()]
            },
        lwm2m_client_list =>
            #{
                summary => <<"LwM2M Client List">>,
                value => [example_lwm2m_client()]
            }
    }.

examples_client() ->
    #{
        general_client =>
            #{
                summary => <<"General Client Info">>,
                value => example_general_client()
            },
        lwm2m_client =>
            #{
                summary => <<"LwM2M Client Info">>,
                value => example_lwm2m_client()
            }
    }.

examples_subsctiption_list() ->
    #{
        general_subscription_list =>
            #{
                summary => <<"A General Subscription List">>,
                value => [example_general_subscription()]
            },
        stomp_subscription_list =>
            #{
                summary => <<"The Stomp Subscription List">>,
                value => [example_stomp_subscription]
            }
    }.

examples_subsctiption() ->
    #{
        general_subscription =>
            #{
                summary => <<"A General Subscription">>,
                value => example_general_subscription()
            },
        stomp_subscription =>
            #{
                summary => <<"A Stomp Subscription">>,
                value => example_stomp_subscription()
            }
    }.

example_lwm2m_client() ->
    maps:merge(
        example_general_client(),
        #{
            proto_name => <<"LwM2M">>,
            proto_ver => <<"1.0">>,
            endpoint_name => <<"urn:imei:154928475237123">>,
            lifetime => 86400
        }
    ).

example_general_client() ->
    #{
        clientid => <<"MzAyMzEzNTUwNzk1NDA1MzYyMzIwNzUxNjQwMTY1NzQ0NjE">>,
        username => <<"guest">>,
        node => <<"emqx@127.0.0.1">>,
        proto_name => "STOMP",
        proto_ver => <<"1.0">>,
        ip_address => <<"127.0.0.1">>,
        port => 50675,
        clean_start => true,
        connected => true,
        is_bridge => false,
        keepalive => 0,
        expiry_interval => 0,
        subscriptions_cnt => 0,
        subscriptions_max => <<"infinity">>,
        awaiting_rel_cnt => 0,
        awaiting_rel_max => <<"infinity">>,
        mqueue_len => 0,
        mqueue_max => <<"infinity">>,
        mqueue_dropped => 0,
        inflight_cnt => 0,
        inflight_max => <<"infinity">>,
        heap_size => 4185,
        recv_oct => 56,
        recv_cnt => 1,
        recv_pkt => 1,
        recv_msg => 0,
        send_oct => 61,
        send_cnt => 1,
        send_pkt => 1,
        send_msg => 0,
        reductions => 72022,
        mailbox_len => 0,
        created_at => <<"2021-12-07T10:44:02.721+08:00">>,
        connected_at => <<"2021-12-07T10:44:02.721+08:00">>,
        disconnected_at => null
    }.

example_stomp_subscription() ->
    maps:merge(
        example_general_subscription(),
        #{
            topic => <<"stomp/topic">>,
            sub_props => #{subid => <<"10">>}
        }
    ).

example_general_subscription() ->
    #{
        topic => <<"test/topic">>,
        qos => 1,
        nl => 0,
        rap => 0,
        rh => 0
    }.
