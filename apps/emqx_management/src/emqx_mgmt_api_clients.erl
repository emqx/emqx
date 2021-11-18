%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/logger.hrl").

-include("emqx_mgmt.hrl").

%% API
-export([api_spec/0]).

-export([ clients/2
        , client/2
        , subscriptions/2
        , authz_cache/2
        , subscribe/2
        , unsubscribe/2
        , subscribe_batch/2
        , set_keepalive/2
        ]).

-export([ query/4
        , format_channel_info/1
        ]).

%% for batch operation
-export([do_subscribe/3]).

%% for test suite
-export([ unix_ts_to_rfc3339_bin/1
        , unix_ts_to_rfc3339_bin/2
        , time_string_to_unix_ts_int/1
        , time_string_to_unix_ts_int/2
        ]).


-define(CLIENT_QS_SCHEMA, {emqx_channel_info,
    [ {<<"node">>, atom}
    , {<<"username">>, binary}
    , {<<"zone">>, atom}
    , {<<"ip_address">>, ip}
    , {<<"conn_state">>, atom}
    , {<<"clean_start">>, atom}
    , {<<"proto_name">>, binary}
    , {<<"proto_ver">>, integer}
    , {<<"like_clientid">>, binary}
    , {<<"like_username">>, binary}
    , {<<"gte_created_at">>, timestamp}
    , {<<"lte_created_at">>, timestamp}
    , {<<"gte_connected_at">>, timestamp}
    , {<<"lte_connected_at">>, timestamp}]}).

-define(QUERY_FUN, {?MODULE, query}).
-define(FORMAT_FUN, {?MODULE, format_channel_info}).

-define(CLIENT_ID_NOT_FOUND,
    <<"{\"code\": \"RESOURCE_NOT_FOUND\", \"reason\": \"Client id not found\"}">>).

api_spec() ->
    {apis(), schemas()}.

apis() ->
    [ clients_api()
    , client_api()
    , clients_authz_cache_api()
    , clients_subscriptions_api()
    , subscribe_api()
    , unsubscribe_api()
    , keepalive_api()
    ].

schemas() ->
    Client = #{
        client => #{
            type => object,
            properties => emqx_mgmt_util:properties(properties(client))
        }
    },
    AuthzCache = #{
        authz_cache => #{
            type => object,
            properties => emqx_mgmt_util:properties(properties(authz_cache))
        }
    },
    [Client, AuthzCache].

properties(client) ->
    [
    {awaiting_rel_cnt,  integer,
     <<"v4 api name [awaiting_rel] Number of awaiting PUBREC packet">>},
    {awaiting_rel_max,  integer,
     <<"v4 api name [max_awaiting_rel]. Maximum allowed number of awaiting PUBREC packet">>},
    {clean_start,       boolean,
     <<"Indicate whether the client is using a brand new session">>},
    {clientid,          string ,
     <<"Client identifier">>},
    {connected,         boolean,
     <<"Whether the client is connected">>},
    {connected_at,      string ,
     <<"Client connection time, rfc3339">>},
    {created_at,        string ,
     <<"Session creation time, rfc3339">>},
    {disconnected_at,   string ,
     <<"Client offline time. It's Only valid and returned when connected is false, rfc3339">>},
    {expiry_interval,   integer,
     <<"Session expiration interval, with the unit of second">>},
    {heap_size,         integer,
     <<"Process heap size with the unit of byte">>},
    {inflight_cnt,      integer,
     <<"Current length of inflight">>},
    {inflight_max,      integer,
     <<"v4 api name [max_inflight]. Maximum length of inflight">>},
    {ip_address,        string ,
     <<"Client's IP address">>},
    {port,              integer,
     <<"Client's port">>},
    {is_bridge,         boolean,
     <<"Indicates whether the client is connectedvia bridge">>},
    {keepalive,         integer,
     <<"keepalive time, with the unit of second">>},
    {mailbox_len,       integer,
     <<"Process mailbox size">>},
    {mqueue_dropped,    integer,
     <<"Number of messages dropped by the message queue due to exceeding the length">>},
    {mqueue_len,        integer,
     <<"Current length of message queue">>},
    {mqueue_max,        integer,
     <<"v4 api name [max_mqueue]. Maximum length of message queue">>},
    {node,              string ,
     <<"Name of the node to which the client is connected">>},
    {proto_name,        string ,
     <<"Client protocol name">>},
    {proto_ver,         integer,
     <<"Protocol version used by the client">>},
    {recv_cnt,          integer,
     <<"Number of TCP packets received">>},
    {recv_msg,          integer,
     <<"Number of PUBLISH packets received">>},
    {recv_oct,          integer,
     <<"Number of bytes received by EMQ X Broker (the same below)">>},
    {recv_pkt,          integer,
     <<"Number of MQTT packets received">>},
    {reductions,        integer,
     <<"Erlang reduction">>},
    {send_cnt,          integer,
     <<"Number of TCP packets sent">>},
    {send_msg,          integer,
     <<"Number of PUBLISH packets sent">>},
    {send_oct,          integer,
     <<"Number of bytes sent">>},
    {send_pkt,          integer,
     <<"Number of MQTT packets sent">>},
    {subscriptions_cnt, integer,
     <<"Number of subscriptions established by this client.">>},
    {subscriptions_max, integer,
     <<"v4 api name [max_subscriptions] Maximum number of subscriptions allowed by this client">>},
    {username,          string ,
     <<"User name of client when connecting">>},
    {will_msg,          string ,
     <<"Client will message">>},
    {zone,              string ,
     <<"Indicate the configuration group used by the client">>}
    ];
properties(authz_cache) ->
    [
        {access,            string,  <<"Access type">>},
        {result,            string,  <<"Allow or deny">>},
        {topic,             string,  <<"Topic name">>},
        {updated_time,      integer, <<"Update time">>}
    ].

clients_api() ->
    Metadata = #{
        get => #{
            description => <<"List clients">>,
            parameters => [
                #{
                    name => page,
                    in => query,
                    required => false,
                    description => <<"Page">>,
                    schema => #{type => integer}
                },
                #{
                    name => limit,
                    in => query,
                    required => false,
                    description => <<"Page limit">>,
                    schema => #{type => integer}
                },
                #{
                    name => node,
                    in => query,
                    required => false,
                    description => <<"Node name">>,
                    schema => #{type => string}
                },
                #{
                    name => username,
                    in => query,
                    required => false,
                    description => <<"User name">>,
                    schema => #{type => string}
                },
                #{
                    name => zone,
                    in => query,
                    required => false,
                    schema => #{type => string}
                },
                #{
                    name => ip_address,
                    in => query,
                    required => false,
                    description => <<"Client's IP address">>,
                    schema => #{type => string}
                },
                #{
                    name => conn_state,
                    in => query,
                    required => false,
                    description =>
                      <<"The current connection status of the client, ",
                        "the possible values are connected,idle,disconnected">>,
                    schema => #{type => string, enum => [connected, idle, disconnected]}
                },
                #{
                    name => clean_start,
                    in => query,
                    required => false,
                    description => <<"Whether the client uses a new session">>,
                    schema => #{type => boolean}
                },
                #{
                    name => proto_name,
                    in => query,
                    required => false,
                    description =>
                      <<"Client protocol name, ",
                        "the possible values are MQTT,CoAP,LwM2M,MQTT-SN">>,
                    schema => #{type => string, enum => ['MQTT', 'CoAP', 'LwM2M', 'MQTT-SN']}
                },
                #{
                    name => proto_ver,
                    in => query,
                    required => false,
                    description => <<"Client protocol version">>,
                    schema => #{type => string}
                },
                #{
                    name => like_clientid,
                    in => query,
                    required => false,
                    description => <<"Fuzzy search of client identifier by substring method">>,
                    schema => #{type => string}
                },
                #{
                    name => like_username,
                    in => query,
                    required => false,
                    description => <<"Client user name, fuzzy search by substring">>,
                    schema => #{type => string}
                },
                #{
                    name => gte_created_at,
                    in => query,
                    required => false,
                    description =>
                      <<"Search client session creation time by greater than or equal method, "
                        "rfc3339 or timestamp(millisecond)">>,
                    schema => #{type => string}
                },
                #{
                    name => lte_created_at,
                    in => query,
                    required => false,
                    description =>
                      <<"Search client session creation time by less than or equal method, ",
                        "rfc3339 or timestamp(millisecond)">>,
                    schema => #{type => string}
                },
                #{
                    name => gte_connected_at,
                    in => query,
                    required => false,
                    description =>
                      <<"Search client connection creation time by greater than or equal method, ",
                        "rfc3339 or timestamp(millisecond)">>,
                    schema => #{type => string}
                },
                #{
                    name => lte_connected_at,
                    in => query,
                    required => false,
                    description =>
                      <<"Search client connection creation time by less than or equal method, ",
                        "rfc3339 or timestamp(millisecond) ">>,
                    schema => #{type => string}
                }
            ],
            responses => #{
                <<"200">> => emqx_mgmt_util:array_schema(client, <<"List clients 200 OK">>),
                <<"400">> => emqx_mgmt_util:error_schema( <<"Invalid parameters">>
                                                        , ['INVALID_PARAMETER'])}}},
    {"/clients", Metadata, clients}.

client_api() ->
    Metadata = #{
        get => #{
            description => <<"Get clients info by client ID">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:schema(client, <<"List clients 200 OK">>)}},
        delete => #{
            description => <<"Kick out client by client ID">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"204">> => emqx_mgmt_util:schema(<<"Kick out client successfully">>)}}},
    {"/clients/:clientid", Metadata, client}.

clients_authz_cache_api() ->
    Metadata = #{
        get => #{
            description => <<"Get client authz cache">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:schema(authz_cache, <<"Get client authz cache">>)}},
        delete => #{
            description => <<"Clean client authz cache">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"204">> => emqx_mgmt_util:schema(<<"Clean client authz cache successfully">>)}}},
    {"/clients/:clientid/authz_cache", Metadata, authz_cache}.

clients_subscriptions_api() ->
    Metadata = #{
        get => #{
            description => <<"Get client subscriptions">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:array_schema(subscription, <<"Get client subscriptions">>)}}
    },
    {"/clients/:clientid/subscriptions", Metadata, subscriptions}.

unsubscribe_api() ->
    Metadata = #{
        post => #{
            description => <<"Unsubscribe">>,
            parameters => [
                #{
                    name => clientid,
                    in => path,
                    schema => #{type => string},
                    required => true
                }
            ],
            'requestBody' => emqx_mgmt_util:schema(#{
                type => object,
                properties => #{
                    topic => #{
                        type => string,
                        description => <<"Topic">>}}}),
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:schema(<<"Unsubscribe ok">>)}}},
    {"/clients/:clientid/unsubscribe", Metadata, unsubscribe}.
subscribe_api() ->
    Metadata = #{
        post => #{
            description => <<"Subscribe">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            'requestBody' => emqx_mgmt_util:schema(#{
                type => object,
                properties => #{
                    topic => #{
                        type => string,
                        description => <<"Topic">>},
                    qos => #{
                        type => integer,
                        enum => [0, 1, 2],
                        example => 0,
                        description => <<"QoS">>}}}),
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:schema(<<"Subscribe ok">>)}}},
    {"/clients/:clientid/subscribe", Metadata, subscribe}.

keepalive_api() ->
    Metadata = #{
        put => #{
            description => <<"set the online client keepalive by second ">>,
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true
            },
                #{
                    name => interval,
                    in => query,
                    schema => #{type => integer},
                    required => true
                }
                ],
            responses => #{
                <<"404">> => emqx_mgmt_util:error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:schema(<<"ok">>)}}},
    {"/clients/:clientid/keepalive", Metadata, set_keepalive}.
%%%==============================================================================================
%% parameters trans
clients(get, #{query_string := Qs}) ->
    list(generate_qs(Qs)).

client(get, #{bindings := Bindings}) ->
    lookup(Bindings);

client(delete, #{bindings := Bindings}) ->
    kickout(Bindings).

authz_cache(get, #{bindings := Bindings}) ->
    get_authz_cache(Bindings);

authz_cache(delete, #{bindings := Bindings}) ->
    clean_authz_cache(Bindings).

subscribe(post, #{bindings := #{clientid := ClientID}, body := TopicInfo}) ->
    Topic = maps:get(<<"topic">>, TopicInfo),
    Qos = maps:get(<<"qos">>, TopicInfo, 0),
    subscribe(#{clientid => ClientID, topic => Topic, qos => Qos}).

unsubscribe(post, #{bindings := #{clientid := ClientID}, body := TopicInfo}) ->
    Topic = maps:get(<<"topic">>, TopicInfo),
    unsubscribe(#{clientid => ClientID, topic => Topic}).

%% TODO: batch
subscribe_batch(post, #{bindings := #{clientid := ClientID}, body := TopicInfos}) ->
    Topics =
        [begin
             Topic = maps:get(<<"topic">>, TopicInfo),
             Qos = maps:get(<<"qos">>, TopicInfo, 0),
             #{topic => Topic, qos => Qos}
         end || TopicInfo <- TopicInfos],
    subscribe_batch(#{clientid => ClientID, topics => Topics}).

subscriptions(get, #{bindings := #{clientid := ClientID}}) ->
    {Node, Subs0} = emqx_mgmt:list_client_subscriptions(ClientID),
    Subs = lists:map(fun({Topic, SubOpts}) ->
        #{node => Node, clientid => ClientID, topic => Topic, qos => maps:get(qos, SubOpts)}
    end, Subs0),
    {200, Subs}.

set_keepalive(put, #{bindings := #{clientid := ClientID}, query_string := Query}) ->
    case maps:find(<<"interval">>, Query) of
        error -> {404, "Interval Not Found"};
        {ok, Interval0} ->
            Interval = binary_to_integer(Interval0),
            case emqx_mgmt:set_keepalive(emqx_mgmt_util:urldecode(ClientID), Interval) of
                ok -> {200};
                {error, not_found} ->{404, ?CLIENT_ID_NOT_FOUND}
            end
    end.

%%%==============================================================================================
%% api apply

list(Params) ->
    {Tab, QuerySchema} = ?CLIENT_QS_SCHEMA,
    case maps:get(<<"node">>, Params, undefined) of
        undefined ->
            Response = emqx_mgmt_api:cluster_query(Params, Tab,
                                                   QuerySchema, ?QUERY_FUN),
            emqx_mgmt_util:generate_response(Response);
        Node1 ->
            Node = binary_to_atom(Node1, utf8),
            ParamsWithoutNode = maps:without([<<"node">>], Params),
            Response = emqx_mgmt_api:node_query(Node, ParamsWithoutNode,
                                                Tab, QuerySchema, ?QUERY_FUN),
            emqx_mgmt_util:generate_response(Response)
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

get_authz_cache(#{clientid := ClientID})->
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
            {200};
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOW_ERROR">>, message => Message}}
    end.

subscribe(#{clientid := ClientID, topic := Topic, qos := Qos}) ->
    case do_subscribe(ClientID, Topic, Qos) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOW_ERROR">>, message => Message}};
        ok ->
            {200}
    end.

unsubscribe(#{clientid := ClientID, topic := Topic}) ->
    case do_unsubscribe(ClientID, Topic) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOW_ERROR">>, message => Message}};
        {unsubscribe, [{Topic, #{}}]} ->
            {200}
    end.

subscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    ArgList = [[ClientID, Topic, Qos] || #{topic := Topic, qos := Qos} <- Topics],
    emqx_mgmt_util:batch_operation(?MODULE, do_subscribe, ArgList).

%%--------------------------------------------------------------------
%% internal function

do_subscribe(ClientID, Topic0, Qos) ->
    {Topic, Opts} = emqx_topic:parse(Topic0),
    TopicTable = [{Topic, Opts#{qos => Qos}}],
    case emqx_mgmt:subscribe(ClientID, TopicTable) of
        {error, Reason} ->
            {error, Reason};
        {subscribe, Subscriptions} ->
            case proplists:is_defined(Topic, Subscriptions) of
                true ->
                    ok;
                false ->
                    {error, unknow_error}
            end
    end.

do_unsubscribe(ClientID, Topic) ->
    case emqx_mgmt:unsubscribe(ClientID, Topic) of
        {error, Reason} ->
            {error, Reason};
        Res ->
            Res
    end.

%%--------------------------------------------------------------------
%% QueryString Generation (try rfc3339 to timestamp or keep timestamp)

time_keys() ->
    [ <<"gte_created_at">>
    , <<"lte_created_at">>
    , <<"gte_connected_at">>
    , <<"lte_connected_at">>].

generate_qs(Qs) ->
    Fun =
        fun (Key, NQs) ->
                case NQs of
                    %% TimeString likes "2021-01-01T00:00:00.000+08:00" (in rfc3339)
                    %% or "1609430400000" (in millisecond)
                    #{Key := TimeString} -> NQs#{Key => time_string_to_unix_ts_int(TimeString)};
                    #{}                  -> NQs
                end
        end,
    lists:foldl(Fun, Qs, time_keys()).

%%--------------------------------------------------------------------
%% Query Functions

query(Tab, {Qs, []}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table_with_count(Tab, Ms, Continuation, Limit,
                                          fun format_channel_info/1);

query(Tab, {Qs, Fuzzy}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    FuzzyFilterFun = fuzzy_filter_fun(Fuzzy),
    emqx_mgmt_api:select_table_with_count(Tab, {Ms, FuzzyFilterFun}, Continuation, Limit,
                                          fun format_channel_info/1).

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
    qs2ms(Rest, N+1, {NMtchHead, NConds}).

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds];
put_conds({_, Op1, V1, Op2, V2}, Holder, Conds) ->
    [{Op2, Holder, V2},
        {Op1, Holder, V1} | Conds].

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
    REFuzzy = lists:map(fun({K, like, S}) ->
                 {ok, RE} = re:compile(escape(S)),
                 {K, like, RE}
              end, Fuzzy),
    fun(MsRaws) when is_list(MsRaws) ->
            lists:filter( fun(E) -> run_fuzzy_filter(E, REFuzzy) end
                        , MsRaws)
    end.

escape(B) when is_binary(B) ->
    re:replace(B, <<"\\\\">>, <<"\\\\\\\\">>, [{return, binary}, global]).

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, _, RE} | Fuzzy]) ->
    Val = case maps:get(Key, ClientInfo, "") of
              undefined -> "";
              V -> V
          end,
    re:run(Val, RE, [{capture, none}]) == match andalso run_fuzzy_filter(E, Fuzzy).

%%--------------------------------------------------------------------
%% format funcs

format_channel_info({_, ClientInfo, ClientStats}) ->
    StatsMap = maps:without([memory, next_pkt_id, total_heap_size],
        maps:from_list(ClientStats)),
    ClientInfoMap0 = maps:fold(fun take_maps_from_inner/3, #{}, ClientInfo),
    {IpAddress, Port} = peername_dispart(maps:get(peername, ClientInfoMap0)),
    Connected      = maps:get(conn_state, ClientInfoMap0) =:= connected,
    ClientInfoMap1 = maps:merge(StatsMap, ClientInfoMap0),
    ClientInfoMap2 = maps:put(node, node(), ClientInfoMap1),
    ClientInfoMap3 = maps:put(ip_address, IpAddress, ClientInfoMap2),
    ClientInfoMap4 = maps:put(port, Port, ClientInfoMap3),
    ClientInfoMap  = maps:put(connected, Connected, ClientInfoMap4),
    RemoveList =
        [ auth_result
        , peername
        , sockname
        , peerhost
        , conn_state
        , send_pend
        , conn_props
        , peercert
        , sockstate
        , subscriptions
        , receive_maximum
        , protocol
        , is_superuser
        , sockport
        , anonymous
        , mountpoint
        , socktype
        , active_n
        , await_rel_timeout
        , conn_mod
        , sockname
        , retry_interval
        , upgrade_qos
        , id %% sessionID, defined in emqx_session.erl
    ],
    TimesKeys = [created_at, connected_at, disconnected_at],
    %% format timestamp to rfc3339
    lists:foldl(fun result_format_time_fun/2
               , maps:without(RemoveList, ClientInfoMap)
               , TimesKeys).

%% format func helpers
take_maps_from_inner(_Key, Value, Current) when is_map(Value) ->
    maps:merge(Current, Value);
take_maps_from_inner(Key, Value, Current) ->
    maps:put(Key, Value, Current).

result_format_time_fun(Key, NClientInfoMap) ->
    case NClientInfoMap of
        #{Key := TimeStamp} -> NClientInfoMap#{Key => unix_ts_to_rfc3339_bin(TimeStamp)};
        #{}                 -> NClientInfoMap
    end.

-spec(peername_dispart(emqx_types:peername()) -> {binary(), inet:port_number()}).
peername_dispart({Addr, Port}) ->
    AddrBinary = list_to_binary(inet:ntoa(Addr)),
    %% PortBinary = integer_to_binary(Port),
    {AddrBinary, Port}.

format_authz_cache({{PubSub, Topic}, {AuthzResult, Timestamp}}) ->
    #{ access => PubSub,
       topic => Topic,
       result => AuthzResult,
       updated_time => Timestamp
     }.

%%--------------------------------------------------------------------
%% time format funcs

unix_ts_to_rfc3339_bin(TimeStamp) ->
    unix_ts_to_rfc3339_bin(TimeStamp, millisecond).

unix_ts_to_rfc3339_bin(TimeStamp, Unit) when is_integer(TimeStamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(TimeStamp, [{unit, Unit}])).

time_string_to_unix_ts_int(DateTime) ->
    time_string_to_unix_ts_int(DateTime, millisecond).

time_string_to_unix_ts_int(DateTime, Unit) when is_binary(DateTime) ->
    try binary_to_integer(DateTime) of
        TimeStamp when is_integer(TimeStamp) -> TimeStamp
    catch
        error:badarg ->
            calendar:rfc3339_to_system_time(binary_to_list(DateTime), [{unit, Unit}])
    end.
